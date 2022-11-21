package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
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
func (n *nodeState) runTests(ctx context.Context, outFn func(res testResult)) (err error) {
	ctx2, cancel := context.WithDeadline(context.Background(), time.Now().Add(n.cliCtx.Duration("duration")))
	defer cancel()
	for {
		select {
		case <-ctx2.Done():
			return
		default:
			resCh := n.runConcurrent(ctx2, putOp(true))
			for r := range resCh {
				res := r.(PutOpResult)
				outFn(testResult{
					Method: string(res.r.op),
					Path:   fmt.Sprintf("%s/%s", res.data.Bucket, res.data.Key),
					Node:   n.nodes[res.r.nodeIdx].endpointURL.Host,
					Err:    res.r.err,
				})
				getCh := n.runConcurrent(ctx2, getOp(true, ObjVerifier{ObjectInfo: minio.ObjectInfo{
					ETag: res.data.ETag,
					Key:  res.data.Key,
				}}))
				for gr := range getCh {
					res := gr.(GetOpResult)
					outFn(testResult{
						Method: string(res.r.op),
						Path:   fmt.Sprintf("%s/%s", res.r.bucket, res.data.Key),
						Node:   n.nodes[res.r.nodeIdx].endpointURL.Host,
						Err:    res.r.err,
					})
					statCh := n.runConcurrent(ctx2, statOp(true, ObjVerifier{ObjectInfo: res.data}))
					for sres := range statCh {
						res := sres.(GetOpResult)
						outFn(testResult{
							Method: string(res.r.op),
							Path:   fmt.Sprintf("%s/%s", res.r.bucket, res.data.Key),
							Node:   n.nodes[res.r.nodeIdx].endpointURL.Host,
							Err:    res.r.err,
						})
					}
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
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				switch op.Type {
				case http.MethodPut:
					resCh <- n.put(ctx, putOpts{
						Bucket:  bucket,
						Object:  fmt.Sprintf("%s/%d", shortuuid.New(), idx),
						NodeIdx: idx,
					})
				case http.MethodGet:
					resCh <- n.get(ctx, getOpts{
						Bucket:  bucket,
						Object:  op.Verifier.Key,
						NodeIdx: idx,
					})
				// case http.MethodHead:
				// 	resCh <- n.stat(ctx, statOpts{
				// 		Bucket:  bucket,
				// 		Object:  op.Verifier.Key,
				// 		NodeIdx: idx,
				// 	})
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

type statOpts struct {
	getOpts
}

type listOpts struct {
	Bucket  string
	Object  string
	NodeIdx int
}

type OpResult interface {
	// PutOpResult | GetOpResult
}
type opResult struct {
	op      OpType
	err     error
	offline bool
	nodeIdx int
	bucket  string
}
type PutOpResult struct {
	r    opResult
	data minio.UploadInfo
	opts putOpts
}

type GetOpResult struct {
	r    opResult
	data minio.ObjectInfo
	opts getOpts
}

type StatOpResult struct {
	GetOpResult
}

func (n *nodeState) put(ctx context.Context, o putOpts) (res PutOpResult) {
	reader := getDataReader(o.Size)
	defer reader.Close()
	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.r.offline = true
		return
	}
	oi, err := node.client.PutObject(ctx, o.Bucket, o.Object, reader, int64(o.Size), minio.PutObjectOptions{ContentType: "binary/octet-stream"})
	return PutOpResult{
		r: opResult{
			op:      http.MethodPut,
			err:     err,
			nodeIdx: o.NodeIdx,
			bucket:  o.Bucket,
		},
		data: oi,
		opts: o,
	}
}

func (n *nodeState) get(ctx context.Context, o getOpts) (res GetOpResult) {
	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.r.offline = true
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
		r: opResult{
			op:      http.MethodGet,
			err:     err,
			nodeIdx: o.NodeIdx,
		},
		data: oi,
		opts: o,
	}
}

/*
func (n *nodeState) stat(ctx context.Context, o statOpts) (res StatOpResult) {
	node := n.nodes[o.NodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		res.r.offline = true
		return
	}
	opts := minio.StatObjectOptions{}
	opts.SetMatchETag(o.Verifier.ETag)
	oi, err := node.client.StatObject(ctx, o.Bucket, o.Object, opts)
	if err == nil {
		if oi.ETag != o.Verifier.ETag ||
			oi.VersionID != o.Verifier.VersionID ||
			oi.Size != o.Verifier.Size {
			err = fmt.Errorf("metadata mismatch")
		}
	}

	return StatOpResult{
		r: opResult{
			op:      http.MethodGet,
			err:     err,
			nodeIdx: o.NodeIdx,
		},
		data: oi,
		opts: o,
	}
}
*/
