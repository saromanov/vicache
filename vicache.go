package vicache

import (
	"fmt"

	xxhash "github.com/cespare/xxhash/v2"
)

const (
	chunkSize = 64 * 1024
)
// ViCache defines main struct for cache
type ViCache struct {
	buckets []bucket
	bucketsCount uint
}

// New provides initialization of vi cache
func New(max, bucketsCount uint) (*ViCache, error) {
	if max < bucketsCount {
		return nil, fmt.Errorf("max is less then buckets count")
	}
	maxBucketBytes := uint64((max + bucketsCount - 1) / bucketsCount)
	buckets := make([]bucket, maxBucketBytes)
	for i := uint64(0); i < maxBucketBytes; i++ {
		buckets[i].new(maxBucketBytes)
	}
	return &ViCache{
		buckets: buckets,
		bucketsCount: bucketsCount,
	}, nil
}

// Set provides setting of the key and value
func (c *ViCache) Set(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % uint64(c.bucketsCount)
	c.buckets[idx].set(k, v, h)
}

// Get provides getting value by the key
func (c *ViCache) Get(dst, k []byte) []byte {
	h := xxhash.Sum64(k)
	idx := h % uint64(c.bucketsCount)
	dst, ok := c.buckets[idx].get(dst, k, h, true)
	if !ok {
		return nil
	}
	return dst
}

// Del provides deleting of the data
func (c *ViCache) Del(k []byte){
	h := xxhash.Sum64(k)
	idx := h % uint64(c.bucketsCount)
	c.buckets[idx].del(idx)
}