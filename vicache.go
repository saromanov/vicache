package vicache

const (
	chunkSize = 64 * 1024
)
// ViCache defines main struct for cache
type ViCache struct {
	buckets []bucket
}

// New provides initialization of vi cache
func New(max, bucketsCount uint) *ViCache {
	maxBucketBytes := uint64((max + bucketsCount - 1) / bucketsCount)
	buckets := make([]bucket, maxBucketBytes)
	for i := uint64(0); i < maxBucketBytes; i++ {
		buckets[i].new(maxBucketBytes)
	}
	return &ViCache{
		buckets: buckets,
	}
}