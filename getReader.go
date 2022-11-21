package main

import (
	"io"
	"io/ioutil"
	"math/rand"
)

func newRandomReader(seed, size int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(seed)), size)
}

// read data from file if it exists or optionally create a buffer of particular size
func getDataReader(size int64) io.ReadCloser {
	return ioutil.NopCloser(newRandomReader(size, size))
}
