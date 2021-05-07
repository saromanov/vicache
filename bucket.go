package vicache

import (
	"sync"
	"sync/atomic"
)

type bucket struct {
	mu         sync.RWMutex
	chunks     [][]byte
	m          map[uint64]uint64
	idx        uint64
	callsCount uint64
	gen        uint64
}

// New provides initialization of the new bucket
func (b *bucket) new(maxBytes uint64) {
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks)
	b.m = make(map[uint64]uint64)
}

// Set provides setting data to the bucket
func (b *bucket) set(k, v []byte, h uint64) {
	setCalls := atomic.AddUint64(&b.callsCount, 1)
	if setCalls%(1<<14) == 0 {
		b.Clean()
	}

	isTooBig := func() bool {
		if len(k) >= (1<<16) || len(v) >= (1<<16) {
			return true
		}
		return false
	}

	if isTooBig() {
		return
	}
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
	kvLen := uint64(len(kvLenBuf) + len(k) + len(v))
	if kvLen >= chunkSize {
		return
	}

	b.mu.Lock()
	idx, idxNew, chunkIdx, _ := b.genChunks()
	chunk := b.chunks[chunkIdx]
	if chunk == nil {
		chunk = getChunk()
		chunk = chunk[:0]
	}
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	b.chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx = idxNew
	b.mu.Unlock()
}

// genChuncs provides generation of chuncs
func (b *bucket) genChunks()(uint64, uint64, uint64, uint64) {
	idx := b.idx
	idxNew := idx + kvLen
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize
	if chunkIdxNew >= uint64(len(b.chunks)) {
		idx = 0
		idxNew = kvLen
		chunkIdx = 0
		b.gen++
		if b.gen&((1<<genSizeBits)-1) == 0 {
			b.gen++
		}
	} else {
		idx = chunkIdxNew * chunkSize
		idxNew = idx + kvLen
		chunkIdx = chunkIdxNew
	}
	b.chunks[chunkIdx] = b.chunks[chunkIdx][:0]
	return idx, idxNew, chunkIdx, chunkIdxNew
}
