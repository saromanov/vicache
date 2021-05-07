package vicache

import "sync"

type bucket struct {
	mu sync.RWMutex
	chunks [][]byte

}

// New provides initialization of the new bucket
func (b *bucket) New(maxBucketBytes uint) {
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks)
	b.m = make(map[uint64]uint64)
}
