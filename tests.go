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
	"sync"
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
	ObjInfo   minio.ObjectInfo
	Key       string
	VersionID string
	Prefix    string
	NodeIdx   int // node at which to run operation
}

type OpSeq struct {
	Ops []Op
}

var errInvalidOpSeq = fmt.Errorf("invalid op sequence")

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
			Method: http.MethodDelete,
			Err:    fmt.Errorf("all nodes offline"),
		}
		return
	}

	start := time.Now()
	for _, pfx := range n.Prefixes {
		err := clnt.RemoveObject(ctx, bucket, pfx, minio.RemoveObjectOptions{ForceDelete: true})
		if err != nil {
			n.logCh <- testResult{
				Method:  http.MethodDelete,
				Path:    fmt.Sprintf("%s/%s", bucket, pfx),
				Node:    n.nodes[nodeIdx].endpointURL.Host,
				Err:     err,
				Latency: time.Since(start),
			}
			continue
		}
	}
}

func (n *nodeState) getRandomPfx() string {
	idx := rand.Intn(len(n.Prefixes))
	return n.Prefixes[idx]
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
			case ops, ok := <-n.testCh:
				if !ok {
					return
				}
				n.runOpSeq(ctx, ops)
			}
		}
	}()
}

func (n *nodeState) finish(ctx context.Context) {
	close(n.testCh)
	n.wg.Wait() // wait on workers to finish
	close(n.logCh)
}
func (n *nodeState) init(ctx context.Context, sendFn func(tea.Msg)) {
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
			console.Fatalln("could not create confess_log", err)
			return
		}
		f.WriteString(getHeader(n.cliCtx))
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		for {
			select {
			case <-globalContext.Done():
				n.finish(context.Background())
				return
			case res, ok := <-n.logCh:
				if !ok {
					return
				}
				sendFn(res)
				//if res.Err != nil {
				if _, err := f.WriteString(res.String() + "\n"); err != nil {
					console.Errorln(fmt.Sprintf("Error writing to confess_log for "+res.String(), err))
					os.Exit(1)
				}
				//}
			}
		}
	}()
}

type OpSequence struct {
	Ops []Op
}

func (n *nodeState) generateOpSequence() (seq OpSequence) {
	pfx := n.getRandomPfx()
	object := fmt.Sprintf("%s/%s", pfx, shortuuid.New())
	seq.Ops = append(seq.Ops, Op{Type: http.MethodPut, Key: object})
	for i := 0; i < 5; i++ {
		idx := rand.Intn(3)
		var op Op
		op.Key = object
		op.Prefix = pfx
		switch idx {
		case 0:
			op.Type = "LIST"
		case 1:
			op.Type = http.MethodGet
		case 2:
			op.Type = http.MethodHead
		}
		seq.Ops = append(seq.Ops, op)
	}
	seq.Ops = append(seq.Ops, Op{Type: http.MethodDelete, Key: object})
	return seq
}

func (n *nodeState) runTests(ctx context.Context) (err error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			seq := n.generateOpSequence()
			n.queueTest(seq)
		}
	}
}

// validate if op sequence has PUT and DELETE as first and last ops.
func validateOpSequence(seq OpSequence) error {
	if len(seq.Ops) < 2 {
		return errInvalidOpSeq
	}
	if seq.Ops[0].Type != http.MethodPut {
		return errInvalidOpSeq
	}
	if seq.Ops[len(seq.Ops)-1].Type != http.MethodDelete {
		return errInvalidOpSeq
	}
	return nil
}
func (n *nodeState) runOpSeq(ctx context.Context, seq OpSequence) {
	var oi minio.ObjectInfo
	if err := validateOpSequence(seq); err != nil {
		console.Errorln(err)
		return
	}
	for _, op := range seq.Ops {
		if op.Type == http.MethodPut {
			op.NodeIdx = rand.Intn(len(n.nodes))
			res := n.runTest(ctx, op.NodeIdx, op)
			if res.Err != nil { // discard all other ops in this sequence
				return
			}
			oi = res.data
			n.logCh <- res
			continue
		}
		if op.Type == http.MethodDelete {
			op.NodeIdx = rand.Intn(len(n.nodes))
			res := n.runTest(ctx, op.NodeIdx, op)
			n.logCh <- res
			continue
		}
		var wg sync.WaitGroup
		for i := 0; i < len(n.nodes); i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				op.NodeIdx = i
				op.ObjInfo.VersionID = oi.VersionID
				op.ObjInfo.ETag = oi.ETag
				op.ObjInfo.Size = oi.Size
				res2 := n.runTest(ctx, op.NodeIdx, op)
				n.logCh <- res2
			}(i)
		}
		wg.Wait()
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
			res = n.put(ctx, putOpts{
				Bucket:  bucket,
				Object:  op.Key,
				NodeIdx: idx,
			})
			return
		}
	case http.MethodGet:
		select {
		default:
			res = n.get(ctx, getOpts{
				Bucket:  bucket,
				Object:  op.Key,
				NodeIdx: idx,
				ObjInfo: op.ObjInfo,
			})
			return
		case <-ctx.Done():
			return
		}
	case http.MethodHead:
		select {
		default:

			res = n.stat(ctx, statOpts{
				Bucket:  bucket,
				Object:  op.Key,
				NodeIdx: idx,
				ObjInfo: op.ObjInfo,
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
			res = n.list(ctx, listOpts{
				Bucket:  bucket,
				Prefix:  op.Prefix,
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
	Bucket  string
	Object  string
	Size    int64
	NodeIdx int
	ObjInfo minio.ObjectInfo
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
	//if err == nil {
	// n.buf.lock.Lock()
	// if len(n.buf.objects) < bufSize {
	// 	n.buf.objects = append(n.buf.objects, Object{Key: o.Object, VersionID: oi.VersionID, ETag: oi.ETag})
	// } else {
	// 	// replace an object randomly in the buffer
	// 	idx := rand.Intn(len(n.buf.objects))
	// 	n.buf.objects[idx] = Object{Key: oi.Key, VersionID: oi.VersionID, ETag: oi.ETag}
	// }
	// n.buf.lock.Unlock()
	//}
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
	opts.SetMatchETag(o.ObjInfo.ETag)
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
	opts.SetMatchETag(o.ObjInfo.ETag)
	oi, err := node.client.StatObject(ctx, o.Bucket, o.Object, opts)
	if err == nil {
		if oi.ETag != o.ObjInfo.ETag ||
			oi.VersionID != o.ObjInfo.VersionID ||
			oi.Size != o.ObjInfo.Size {
			err = fmt.Errorf("metadata mismatch: %s, %s, %s,%s, %d, %d", oi.ETag, o.ObjInfo.ETag, oi.VersionID, o.ObjInfo.VersionID, oi.Size, o.Size)
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
