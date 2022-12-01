package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
)

// OpType - operation type
type OpType string

type Op struct {
	Type       OpType
	Verifier   ObjVerifier
	Concurrent bool // run op on all nodes concurrently
	Key        string
	VersionID  string
	Prefix     string
	NodeIdx    int // node at which to run operation
}
type ObjVerifier struct {
	minio.ObjectInfo
}

type OpSeq struct {
	Ops []Op
}

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

func deleteOp(concurrency bool, key, versionID string) Op {
	return Op{
		Type:       http.MethodDelete,
		Concurrent: concurrency,
		Key:        key,
		VersionID:  versionID,
	}
}
func listOp(pfx string) Op {
	return Op{
		Type:   "LIST",
		Prefix: pfx,
	}
}

// create upto bufSize worth of objects. Once past bufSize, delete previously uploaded and
// replace with a new upload
func (n *nodeState) prepareTest(ctx context.Context, nodeIdx int) {
	r := n.runTest(ctx, nodeIdx, putOp(true))
	// if r, ok := res.(PutOpResult); ok {

	// 	n.outFn(testResult{
	// 		Method:  string(r.op),
	// 		Path:    fmt.Sprintf("%s/%s", r.bucket, r.data.Key),
	// 		Node:    n.nodes[nodeIdx].endpointURL.Host,
	// 		Err:     r.err,
	// 		Latency: r.latency,
	// 	})
	n.buf.lock.Lock()
	if len(n.buf.objects) < bufSize {
		n.buf.objects = append(n.buf.objects, Object{Key: r.data.Key, VersionID: r.data.VersionID})
	} else {
		idx := rand.Intn(len(n.buf.objects))
		o := n.buf.objects[idx]
		go n.deleteTest(ctx, nodeIdx, o.Key, o.VersionID)
		n.buf.objects[idx] = Object{Key: r.data.Key, VersionID: r.data.VersionID}
	}
	n.buf.lock.Unlock()

}

// deleteTest deletes an object
func (n *nodeState) deleteTest(ctx context.Context, nodeIdx int, key, versionID string) {
	node := n.nodes[nodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		return
	}
	res := n.runTest(ctx, nodeIdx, deleteOp(false, key, versionID))
	n.logCh <- res
	// if r, ok := dres.(DelOpResult); ok {
	// 	n.outFn(testResult{
	// 		Method:  string(r.op),
	// 		Path:    fmt.Sprintf("%s/%s", r.bucket, key),
	// 		Node:    n.nodes[r.nodeIdx].endpointURL.Host,
	// 		Err:     r.err,
	// 		Latency: r.latency,
	// 	})
	// }
}
func (n *nodeState) finishTest(ctx context.Context) {
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
		n.logCh <- testResult{
			Method:  http.MethodDelete,
			Err:     fmt.Errorf("all nodes offline"),
			Cleanup: true,
		}
		return
	}

	start := time.Now()
	for _, pfx := range n.buf.prefixes {
		err := clnt.RemoveObject(ctx, bucket, pfx, minio.RemoveObjectOptions{ForceDelete: true})
		if err != nil {
			n.logCh <- testResult{
				Method:  http.MethodDelete,
				Path:    fmt.Sprintf("%s/%s", bucket, pfx),
				Node:    n.nodes[nodeIdx].endpointURL.Host,
				Err:     err,
				Latency: time.Since(start),
				Cleanup: true,
			}
			continue
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
func (n *nodeState) getRandomNode() int {
	return rand.Intn(len(n.nodes))
}
func (n *nodeState) getRandomPfx() string {
	idx := rand.Intn(len(n.buf.prefixes))
	return n.buf.prefixes[idx]
}

// addWorker creates a new worker to process tasks
func (n *nodeState) addWorker(ctx context.Context) {
	n.wg.Add(1)
	// Add a new worker.
	go func() {
		defer n.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case op, ok := <-n.testCh:
				if !ok {
					return
				}
				nodeIdx := n.getRandomNode()
				// upload bufSize objects
				n.prepareTest(ctx, nodeIdx)
				res := n.runTest(ctx, nodeIdx, op)
				n.logCh <- res
			}
		}
	}()
}
func (n *nodeState) finish(ctx context.Context) {
	close(n.testCh)
	n.wg.Wait() // wait on workers to finish
	// close(m.failedCh)
	close(n.logCh)
}
func (n *nodeState) init(ctx context.Context) {
	if n == nil {
		return
	}
	for i := 0; i < concurrency; i++ {
		n.addWorker(ctx)
	}
	go func() {
		logFile := fmt.Sprintf("%s%s", "confess_log", time.Now().Format(".01-02-2006-15-04-05"))
		if n.cliCtx.IsSet("output") {
			logFile = fmt.Sprintf("%s/%s", n.cliCtx.String("output"), logFile)
		}
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			console.Fatalln("could not create + migration_log.txt", err)
			return
		}
		f.WriteString(getHeader(n.cliCtx))
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		for {
			select {
			case <-globalContext.Done():
				return
			case res, ok := <-n.logCh:
				if !ok {
					return
				}
				if _, err := f.WriteString(res.String() + "\n"); err != nil {
					console.Errorln(fmt.Sprintf("Error writing to migration_log.txt for "+res.String(), err))
					os.Exit(1)
				}

			}
		}
	}()
}

func (n *nodeState) runTests(ctx context.Context) (err error) {
	// defer func() {
	// 	n.finishTest(globalContext, outFn)
	// 	outFn(testResult{
	// 		Final: true,
	// 	})
	// }()
	// bucket := n.cliCtx.String("bucket")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			nodeIdx := n.getRandomNode()
			// upload bufSize objects
			n.prepareTest(ctx, nodeIdx)
			n.queueTest(listOp(n.getRandomPfx()))
			// lr := n.runTest(ctx, nodeIdx, listOp(n.getRandomPfx()))
			// if res, ok := lr.(ListOpResult); ok {
			// 	n.outFn(testResult{
			// 		Method:  string(res.op),
			// 		Path:    fmt.Sprintf("%s/%s", bucket, res.opts.Prefix),
			// 		Node:    n.nodes[res.nodeIdx].endpointURL.Host,
			// 		Err:     res.err,
			// 		Latency: res.latency,
			// 	})
			// }
			o := n.getRandomObj() // get random object from buffer
			n.queueTest(getOp(true, ObjVerifier{ObjectInfo: minio.ObjectInfo{
				ETag: o.ETag,
				Key:  o.Key,
			}}))
			n.queueTest(statOp(true, ObjVerifier{ObjectInfo: minio.ObjectInfo{
				ETag: o.ETag,
				Key:  o.Key,
			}}))
			// n.runTest(ctx, nodeIdx, getOp(true, ObjVerifier{ObjectInfo: minio.ObjectInfo{
			// 	ETag: o.ETag,
			// 	Key:  o.Key,
			// }}))
			// res, ok := gr.(GetOpResult)
			// if ok {
			// 	n.outFn(testResult{
			// 		Method:  string(res.op),
			// 		Path:    fmt.Sprintf("%s/%s", bucket, o.Key),
			// 		Node:    n.nodes[res.nodeIdx].endpointURL.Host,
			// 		Err:     res.err,
			// 		Latency: res.latency,
			// 	})
			// sres := n.runTest(ctx, nodeIdx, statOp(true, ObjVerifier{ObjectInfo: res.data}))
			// if r, ok := sres.(StatOpResult); ok {
			// 	n.outFn(testResult{
			// 		Method:  string(r.op),
			// 		Path:    fmt.Sprintf("%s/%s", bucket, o.Key),
			// 		Node:    n.nodes[r.nodeIdx].endpointURL.Host,
			// 		Err:     r.err,
			// 		Latency: r.latency,
			// 	})
			// }

		}
	}
}

func (n *nodeState) runTest(ctx context.Context, idx int, op Op) (res testResult) {
	bucket := n.cliCtx.String("bucket")
	node := n.nodes[idx]
	if n.hc.isOffline(node.endpointURL) {
		return
	}

	switch op.Type {
	case http.MethodPut:
		select {
		case <-ctx.Done():
			return
		default:
			pfx := n.buf.prefixes[rand.Intn(len(n.buf.prefixes))]
			res = n.put(ctx, putOpts{
				Bucket:  bucket,
				Object:  fmt.Sprintf("%s/%s", pfx, shortuuid.New()),
				NodeIdx: idx,
			})
			return
		}
	case http.MethodGet:
		select {
		default:
			res = n.get(ctx, getOpts{
				Bucket:   bucket,
				Object:   op.Verifier.Key,
				NodeIdx:  idx,
				Verifier: op.Verifier.ObjectInfo,
			})
			return
		case <-ctx.Done():
			return
		}
	case http.MethodHead:
		select {
		default:
			res = n.stat(ctx, statOpts{
				Bucket:   bucket,
				Object:   op.Verifier.Key,
				NodeIdx:  idx,
				Verifier: op.Verifier.ObjectInfo,
			})
			return
		case <-ctx.Done():
			return
		}
	case http.MethodDelete:
		select {
		default:
			res = n.delete(ctx, delOpts{
				Bucket:              bucket,
				Object:              op.Key,
				RemoveObjectOptions: minio.RemoveObjectOptions{VersionID: op.VersionID},
				NodeIdx:             idx,
			})
			return
		case <-ctx.Done():
			return
		}
	case "LIST":
		select {
		default:
			pfx := n.buf.prefixes[rand.Intn(len(n.buf.prefixes))]
			res = n.list(ctx, listOpts{
				Bucket:  bucket,
				Prefix:  pfx,
				NodeIdx: idx,
			})
			return
		case <-ctx.Done():
			return
		}
	default:
	}

	return res
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
	minio.ListObjectsOptions
	Bucket  string
	Prefix  string
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

type ListOpResult struct {
	opResult
	opts listOpts
}

func (n *nodeState) put(ctx context.Context, o putOpts) (res testResult) {
	start := time.Now()
	reader := getDataReader(o.Size)
	defer reader.Close()
	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
		return
	}
	oi, err := node.client.PutObject(ctx, o.Bucket, o.Object, reader, int64(o.Size), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	return testResult{
		Method:  http.MethodPut,
		Path:    fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Err:     err,
		Node:    n.nodes[o.NodeIdx].endpointURL.Host,
		Latency: time.Since(start),
		data:    toObjectInfo(oi),
	}
}

func (n *nodeState) get(ctx context.Context, o getOpts) (res testResult) {
	start := time.Now()

	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
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
	return testResult{
		Method:  http.MethodGet,
		Path:    fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Err:     err,
		Node:    n.nodes[o.NodeIdx].endpointURL.Host,
		Latency: time.Since(start),
		data:    oi,
	}
}

func (n *nodeState) stat(ctx context.Context, o statOpts) (res testResult) {
	start := time.Now()

	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
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
	return testResult{
		Method:  http.MethodHead,
		Path:    fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Err:     err,
		Node:    n.nodes[o.NodeIdx].endpointURL.Host,
		Latency: time.Since(start),
		data:    oi,
	}
}

func (n *nodeState) delete(ctx context.Context, o delOpts) (res testResult) {
	start := time.Now()

	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
		return
	}
	opts := o.RemoveObjectOptions
	err := node.client.RemoveObject(ctx, o.Bucket, o.Object, opts)
	return testResult{
		Method:  http.MethodDelete,
		Path:    fmt.Sprintf("%s/%s", o.Bucket, o.Object),
		Err:     err,
		Node:    n.nodes[o.NodeIdx].endpointURL.Host,
		Latency: time.Since(start),
	}
}
func (n *nodeState) list(ctx context.Context, o listOpts) (res testResult) {

	start := time.Now()

	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.Offline = true
		return
	}
	doneCh := make(chan struct{})
	defer close(doneCh)
	for objCh := range node.client.ListObjects(ctx, o.Bucket, minio.ListObjectsOptions{
		Prefix:       o.Prefix,
		Recursive:    true,
		WithVersions: true,
	}) {
		if objCh.Err != nil {
			return testResult{
				Method:  "LIST",
				Path:    fmt.Sprintf("%s/%s", o.Bucket, o.Prefix),
				Node:    n.nodes[o.NodeIdx].endpointURL.Host,
				Err:     objCh.Err,
				Latency: time.Since(start),
			}
		}
	}
	return testResult{
		Method:  "LIST",
		Path:    fmt.Sprintf("%s/%s", o.Bucket, o.Prefix),
		Err:     nil,
		Node:    n.nodes[o.NodeIdx].endpointURL.Host,
		Latency: time.Since(start),
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
