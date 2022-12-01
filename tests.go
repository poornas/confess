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

	tea "github.com/charmbracelet/bubbletea"
	"github.com/lithammer/shortuuid/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/console"
)

// OpType - operation type
type OpType string

type Op struct {
	Type      OpType
	Verifier  ObjVerifier
	Key       string
	VersionID string
	Prefix    string
	NodeIdx   int // node at which to run operation
}
type ObjVerifier struct {
	minio.ObjectInfo
}

type OpSeq struct {
	Ops []Op
}

func putOp(pfx string) Op {
	return Op{
		Type: http.MethodPut,
		Key:  fmt.Sprintf("%s/%s", pfx, shortuuid.New()),
	}
}
func getOp(verifier ObjVerifier) Op {
	return Op{
		Type:     http.MethodGet,
		Verifier: verifier,
		Key:      verifier.Key,
	}
}
func statOp(verifier ObjVerifier) Op {
	op := getOp(verifier)
	op.Type = http.MethodHead
	return op
}

func deleteOp(key, versionID string) Op {
	return Op{
		Type:      http.MethodDelete,
		Key:       key,
		VersionID: versionID,
	}
}
func listOp(pfx string) Op {
	return Op{
		Type:   "LIST",
		Prefix: pfx,
	}
}

// deleteTest deletes an object
func (n *nodeState) deleteTest(ctx context.Context, nodeIdx int, key, versionID string) {
	node := n.nodes[nodeIdx]
	if n.hc.isOffline(node.endpointURL) {
		return
	}
	res := n.runTest(ctx, nodeIdx, deleteOp(key, versionID))
	n.logCh <- res
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
	if len(n.buf.objects) == 0 {
		return Object{}
	}
	idx := rand.Intn(len(n.buf.objects))
	return n.buf.objects[idx]
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
				// upload bufSize objects
				res := n.runTest(ctx, op.NodeIdx, op)
				n.logCh <- res
			}
		}
	}()
}

// func (n *nodeState) finish(ctx context.Context) {
// 	close(n.testCh)
// 	n.wg.Wait() // wait on workers to finish
// 	// close(m.failedCh)
// 	close(n.logCh)
// }
func (n *nodeState) init(ctx context.Context, sendFn func(tea.Msg)) {
	if n == nil {
		return
	}
	for i := 0; i < concurrency; i++ {
		n.addWorker(ctx)
	}
	go func() {
		for i := 0; i < bufSize; i++ {
			n.queueTest(putOp(n.getRandomPfx()))
		}
	}()

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
				sendFn(res)
				//if res.Err != nil { . //temporarily for testing...
				if _, err := f.WriteString(res.String() + "\n"); err != nil {
					console.Errorln(fmt.Sprintf("Error writing to migration_log.txt for "+res.String(), err))
					os.Exit(1)
				}
				//}
			}
		}
	}()
}

func (n *nodeState) runTests(ctx context.Context) (err error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			opIdx := rand.Intn(4)
			var op Op
			switch opIdx {
			case 0:
				op = putOp(n.getRandomPfx())
			case 1:
				op = listOp(n.getRandomPfx())
			case 2:
				o := n.getRandomObj()

				op = getOp(ObjVerifier{ObjectInfo: minio.ObjectInfo{
					ETag: o.ETag,
					Key:  o.Key,
				}})
			case 3:
				o := n.getRandomObj()
				op = statOp(ObjVerifier{ObjectInfo: minio.ObjectInfo{
					ETag: o.ETag,
					Key:  o.Key,
				}})
			}
			if op.Key == "" && op.Prefix == "" {
				continue
			}
			for i := 0; i < len(n.nodes); i++ {
				op.NodeIdx = i
				n.queueTest(op)
			}
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
	if err == nil {
		n.buf.lock.Lock()
		if len(n.buf.objects) < bufSize {
			n.buf.objects = append(n.buf.objects, Object{Key: o.Object, VersionID: oi.VersionID, ETag: oi.ETag})
		} else {
			// replace an object randomly in the buffer
			idx := rand.Intn(len(n.buf.objects))
			n.buf.objects[idx] = Object{Key: oi.Key, VersionID: oi.VersionID, ETag: oi.ETag}
		}
		n.buf.lock.Unlock()
	}
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
	fmt.Println("o.Verifier...", err, o.Verifier, "|", oi)

	if err == nil {
		if oi.ETag != o.Verifier.ETag ||
			oi.VersionID != o.Verifier.VersionID ||
			oi.Size != o.Verifier.Size {
			err = fmt.Errorf("metadata mismatch %s, %s, %s,%s, %d, %d", oi.ETag, o.Verifier.ETag, oi.VersionID, o.VersionID, oi.Size, o.Size)
		}
	}
	// testing......
	err = fmt.Errorf("request resource is unreadable %s", o.Object)
	// testing......

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
