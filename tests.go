package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/minio-go/v7"
)

func putOp(concurrency bool) Op {
	return Op{
		Type:       http.MethodPut,
		Concurrent: concurrency,
	}
}
func getOp(concurrency bool, verifier ObjVerifier) Op {
	return Op{
		Type:       http.MethodGet,
		Concurrent: concurrency,
		Verifier:   verifier,
	}
}
func statOp(concurrency bool, verifier ObjVerifier) Op {
	op := getOp(concurrency, verifier)
	op.Type = http.MethodHead
	return op
}

func deleteOp(concurrency bool, key, versionID string, nIdx int) Op {
	return Op{
		Type:       http.MethodDelete,
		Concurrent: concurrency,
		Key:        key,
		VersionID:  versionID,
		NodeIdx:    nIdx,
	}
}
func (n *nodeState) prepareTest(ctx context.Context, outFn func(res testResult)) {
	resCh := n.runConcurrent(ctx, putOp(true))
	for res := range resCh {
		r := res.(PutOpResult)
		outFn(testResult{
			Method:  string(r.op),
			Path:    fmt.Sprintf("%s/%s", r.bucket, r.data.Key),
			Node:    n.nodes[r.nodeIdx].endpointURL.Host,
			Err:     r.err,
			Latency: r.latency,
		})
		n.buf.lock.Lock()
		if len(n.buf.objects) < bufSize {
			n.buf.objects = append(n.buf.objects, Object{Key: r.data.Key, VersionID: r.data.VersionID})
		} else {
			idx := rand.Intn(len(n.buf.objects))
			o := n.buf.objects[idx]
			go n.deleteTest(ctx, o.Key, o.VersionID, outFn)
			n.buf.objects[idx] = Object{Key: r.data.Key, VersionID: r.data.VersionID}
		}
		n.buf.lock.Unlock()
	}
}
func (n *nodeState) deleteTest(ctx context.Context, key, versionID string, outFn func(res testResult)) {
	var nodeIdx int
	for i, node := range n.nodes {
		if n.hc.isOffline(node.endpointURL) {
			continue
		}
		nodeIdx = i
		break
	}
	delCh := n.runConcurrent(ctx, deleteOp(false, key, versionID, nodeIdx))
	for dres := range delCh {
		r := dres.(DelOpResult)
		outFn(testResult{
			Method:  string(r.op),
			Path:    fmt.Sprintf("%s/%s", r.bucket, key),
			Node:    n.nodes[r.nodeIdx].endpointURL.Host,
			Err:     r.err,
			Latency: r.latency,
		})
	}
}
func (n *nodeState) finishTest(ctx context.Context, outFn func(res testResult)) {
	bucket := n.cliCtx.String("bucket")
	var clnt *minio.Client
	var nodeIdx int
	for i, node := range n.nodes {
		if n.hc.isOffline(node.endpointURL) {
			continue
		}
		clnt = node.client
		nodeIdx = i
		break
	}
	if clnt == nil {
		outFn(testResult{
			Method:  http.MethodDelete,
			Err:     fmt.Errorf("all nodes offline"),
			Cleanup: true,
		})
		return
	}
	doneCh := make(chan struct{})
	defer close(doneCh)
	start := time.Now()
	for objCh := range clnt.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
		if objCh.Err != nil {
			outFn(testResult{
				Method:  "LIST",
				Path:    fmt.Sprintf("%s/%s", bucket, objCh.Key),
				Node:    n.nodes[nodeIdx].endpointURL.Host,
				Err:     objCh.Err,
				Latency: time.Since(start),
				Cleanup: true,
			})
			return
		}
		if objCh.Key != "" {
			start = time.Now()
			err := clnt.RemoveObject(ctx, bucket, objCh.Key, minio.RemoveObjectOptions{})
			if err != nil {
				outFn(testResult{
					Method:  http.MethodDelete,
					Path:    fmt.Sprintf("%s/%s", bucket, objCh.Key),
					Node:    n.nodes[nodeIdx].endpointURL.Host,
					Err:     objCh.Err,
					Latency: time.Since(start),
					Cleanup: true,
				})
				continue
			}
		}
	}

	// // objects are already deleted, clear the buckets now
	// start = time.Now()
	// err := clnt.RemoveBucket(ctx, bucket)
	// if err != nil {
	// 	outFn(testResult{
	// 		Method:  http.MethodDelete,
	// 		Path:    bucket,
	// 		Node:    n.nodes[nodeIdx].endpointURL.Host,
	// 		Err:     err,
	// 		Latency: time.Since(start),
	// 	})
	// }
	n.buf.lock.Lock()
	n.buf.objects = n.buf.objects[:0]
	n.buf.lock.Unlock()

}

func (n *nodeState) getRandomObj() Object {
	n.buf.lock.RLock()
	defer n.buf.lock.RUnlock()
	idx := rand.Intn(len(n.buf.objects))
	return n.buf.objects[idx]
}

func (n *nodeState) runTests(ctx context.Context, outFn func(res testResult)) (err error) {
	// defer func() {
	// 	n.finishTest(globalContext, outFn)
	// 	outFn(testResult{
	// 		Final: true,
	// 	})
	// }()

	var ctx2 = ctx
	if n.cliCtx.IsSet("duration") {
		var cancel context.CancelFunc
		ctx2, cancel = context.WithDeadline(context.Background(), time.Now().Add(n.cliCtx.Duration("duration")))
		defer cancel()
	}
	for {
		select {
		case <-ctx2.Done():
			return
		default:
			// upload bufSize objects
			n.prepareTest(ctx2, outFn)
			o := n.getRandomObj()
			getCh := n.runConcurrent(ctx2, getOp(true, ObjVerifier{ObjectInfo: minio.ObjectInfo{
				ETag: o.ETag,
				Key:  o.Key,
			}}))
			for gr := range getCh {
				res := gr.(GetOpResult)
				outFn(testResult{
					Method:  string(res.op),
					Path:    fmt.Sprintf("%s/%s", res.bucket, res.data.Key),
					Node:    n.nodes[res.nodeIdx].endpointURL.Host,
					Err:     res.err,
					Latency: res.latency,
				})
				statCh := n.runConcurrent(ctx2, statOp(true, ObjVerifier{ObjectInfo: res.data}))
				for sres := range statCh {
					r := sres.(StatOpResult)
					outFn(testResult{
						Method:  string(r.op),
						Path:    fmt.Sprintf("%s/%s", r.bucket, r.data.Key),
						Node:    n.nodes[r.nodeIdx].endpointURL.Host,
						Err:     r.err,
						Latency: r.latency,
					})
				}
			}

		}
	}
}

// OpType - operation type
type OpType string

type Op struct {
	Type       OpType
	Verifier   ObjVerifier
	Concurrent bool // run op on all nodes concurrently
	Key        string
	VersionID  string
	NodeIdx    int // node at which to run operation
}
type ObjVerifier struct {
	minio.ObjectInfo
}

type OpSeq struct {
	Ops []Op
}

func (n *nodeState) runConcurrent(ctx context.Context, op Op) chan OpResult {
	var wg sync.WaitGroup
	bucket := n.cliCtx.String("bucket")
	resCh := make(chan OpResult, len(n.nodes))
	go func() {
		defer close(resCh)
		for idx, node := range n.nodes {
			if n.hc.isOffline(node.endpointURL) {
				continue
			}
			if !op.Concurrent && op.NodeIdx != idx {
				continue
			}
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				switch op.Type {
				case http.MethodPut:
					select {
					case resCh <- n.put(ctx, putOpts{
						Bucket:  bucket,
						Object:  fmt.Sprintf("%s/%d", shortuuid.New(), idx),
						NodeIdx: idx,
					}):
					case <-ctx.Done():
						return
					}
				case http.MethodGet:
					select {
					case resCh <- n.get(ctx, getOpts{
						Bucket:   bucket,
						Object:   op.Verifier.Key,
						NodeIdx:  idx,
						Verifier: op.Verifier.ObjectInfo,
					}):
					case <-ctx.Done():
						return
					}
				case http.MethodHead:
					select {
					case resCh <- n.stat(ctx, statOpts{
						Bucket:   bucket,
						Object:   op.Verifier.Key,
						NodeIdx:  idx,
						Verifier: op.Verifier.ObjectInfo,
					}):
					case <-ctx.Done():
						return
					}
				case http.MethodDelete:
					select {
					case resCh <- n.delete(ctx, delOpts{
						Bucket:              bucket,
						Object:              op.Key,
						RemoveObjectOptions: minio.RemoveObjectOptions{VersionID: op.VersionID},
						NodeIdx:             idx,
					}):
					case <-ctx.Done():
						return
					}
				default:
				}
			}(idx)
		}
		wg.Wait()
	}()
	return resCh
}

type putOpts struct {
	minio.PutObjectOptions
	Bucket       string
	Object       string
	Size         int64
	UserMetadata map[string]string
	NodeIdx      int
}

type getOpts struct {
	minio.GetObjectOptions
	Bucket   string
	Object   string
	Size     int64
	NodeIdx  int
	Verifier minio.ObjectInfo
}

type statOpts getOpts

type delOpts struct {
	minio.RemoveObjectOptions
	Bucket  string
	Object  string
	NodeIdx int
}
type listOpts struct {
	Bucket  string
	Object  string
	NodeIdx int
}

type OpResult interface{}
type opResult struct {
	op      OpType
	err     error
	offline bool
	nodeIdx int
	bucket  string
	data    minio.ObjectInfo
	latency time.Duration
}
type PutOpResult struct {
	opResult
	opts putOpts
}

type GetOpResult struct {
	opResult
	opts getOpts
}

type StatOpResult GetOpResult

type DelOpResult struct {
	opResult
	opts delOpts
}

func (n *nodeState) put(ctx context.Context, o putOpts) (res PutOpResult) {
	start := time.Now()
	reader := getDataReader(o.Size)
	defer reader.Close()
	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.offline = true
		return
	}
	oi, err := node.client.PutObject(ctx, o.Bucket, o.Object, reader, int64(o.Size), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	return PutOpResult{
		opResult: opResult{
			op:      http.MethodPut,
			err:     err,
			nodeIdx: o.NodeIdx,
			bucket:  o.Bucket,
			data:    toObjectInfo(oi),
			latency: time.Since(start),
		},
		opts: o,
	}
}

func (n *nodeState) get(ctx context.Context, o getOpts) (res GetOpResult) {
	start := time.Now()

	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.offline = true
		return
	}
	opts := minio.GetObjectOptions{}
	opts.SetMatchETag(o.Verifier.ETag)
	obj, err := node.client.GetObject(ctx, o.Bucket, o.Object, opts)
	var oi minio.ObjectInfo
	if err == nil {
		_, err = io.Copy(ioutil.Discard, obj)
		if err == nil {
			oi, err = obj.Stat()
		}
	}

	return GetOpResult{
		opResult: opResult{
			op:      http.MethodGet,
			err:     err,
			nodeIdx: o.NodeIdx,
			data:    oi,
			latency: time.Since(start),
		},
		opts: o,
	}
}

func (n *nodeState) stat(ctx context.Context, o statOpts) (res StatOpResult) {
	start := time.Now()

	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.offline = true
		return
	}
	opts := minio.StatObjectOptions{}
	opts.SetMatchETag(o.Verifier.ETag)
	oi, err := node.client.StatObject(ctx, o.Bucket, o.Object, opts)
	if err == nil {
		if oi.ETag != o.Verifier.ETag ||
			oi.VersionID != o.Verifier.VersionID ||
			oi.Size != o.Verifier.Size {
			err = fmt.Errorf("metadata mismatch %s, %s, %s,%s, %d, %d", oi.ETag, o.Verifier.ETag, oi.VersionID, o.VersionID, oi.Size, o.Size)
		}
	}

	return StatOpResult{
		opResult: opResult{
			op:      http.MethodHead,
			err:     err,
			nodeIdx: o.NodeIdx,
			data:    oi,
			latency: time.Since(start),
		},
		opts: getOpts(o),
	}
}

func (n *nodeState) delete(ctx context.Context, o delOpts) (res DelOpResult) {
	start := time.Now()

	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.offline = true
		return
	}
	opts := o.RemoveObjectOptions
	err := node.client.RemoveObject(ctx, o.Bucket, o.Object, opts)

	return DelOpResult{
		opResult: opResult{
			op:      http.MethodDelete,
			err:     err,
			nodeIdx: o.NodeIdx,
			latency: time.Since(start),
		},
		opts: delOpts(o),
	}
}
func toObjectInfo(o minio.UploadInfo) minio.ObjectInfo {
	return minio.ObjectInfo{
		Key:            o.Key,
		ETag:           o.ETag,
		Size:           o.Size,
		LastModified:   o.LastModified,
		VersionID:      o.VersionID,
		ChecksumCRC32:  o.ChecksumCRC32,
		ChecksumCRC32C: o.ChecksumCRC32C,
		ChecksumSHA1:   o.ChecksumSHA1,
		ChecksumSHA256: o.ChecksumSHA256,
	}
}
