package vicache

import (
	"sync"
	"sync/atomic"
)

type bucket struct {
	mu             sync.RWMutex
	chunks         [][]byte
	m              map[uint64]uint64
	idx            uint64
	callsCount     uint64
	gen            uint64
	maxGen         uint64
	bucketSizeBits uint64
	genSizeBits    uint64
	collisions     uint64
	misses         uint64
}

// New provides initialization of the new bucket
func (b *bucket) new(maxBytes uint64) {
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks)
	b.m = make(map[uint64]uint64)
	b.maxGen = 1024
	b.gen = 256
	b.genSizeBits = 128
}

// Set provides setting data to the bucket
func (b *bucket) set(k, v []byte, h uint64) {
	setCalls := atomic.AddUint64(&b.callsCount, 1)
	if setCalls%(1<<14) == 0 {
		b.clean()
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
	idx, idxNew, chunkIdx, _ := b.genChunks(kvLen)
	chunk := b.chunks[chunkIdx]
	if chunk == nil {
		//chunk = getChunk()
		chunk = chunk[:0]
	}
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	b.chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << b.bucketSizeBits)
	b.idx = idxNew
	b.mu.Unlock()
}

// genChuncs provides generation of chuncs
func (b *bucket) genChunks(kvLen uint64) (uint64, uint64, uint64, uint64) {
	idx := b.idx
	idxNew := idx + kvLen
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize
	if chunkIdxNew >= uint64(len(b.chunks)) {
		idx = 0
		idxNew = kvLen
		chunkIdx = 0
		b.gen++
		if b.gen&((1<<b.genSizeBits)-1) == 0 {
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

func (b *bucket) get(dst, k []byte, h uint64, returnDst bool) ([]byte, bool) {
	atomic.AddUint64(&b.callsCount, 1)
	found := false
	b.mu.RLock()
	defer func() {
		b.mu.RUnlock()
		if !found {
			atomic.AddUint64(&b.misses, 1)
		}
	}()
	v := b.m[h]
	bGen := b.gen & ((1 << b.genSizeBits) - 1)
	if v > 0 {
		gen := v >> b.bucketSizeBits
		idx := v & ((1 << b.bucketSizeBits) - 1)
		if gen == bGen && idx < b.idx || gen+1 == bGen && idx >= b.idx || gen == b.maxGen && bGen == 1 && idx >= b.idx {
			chunkIdx := idx / chunkSize
			if chunkIdx >= uint64(len(b.chunks)) {
				return dst, found
			}
			chunk := b.chunks[chunkIdx]
			idx %= chunkSize
			if idx+4 >= chunkSize {
				return dst, found
			}
			kvLenBuf := chunk[idx : idx+4]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4
			if idx+keyLen+valLen >= chunkSize {
				return dst, found
			}
			if string(k) == string(chunk[idx:idx+keyLen]) {
				idx += keyLen
				if returnDst {
					dst = append(dst, chunk[idx:idx+valLen]...)
				}
				found = true
			} else {
				atomic.AddUint64(&b.collisions, 1)
			}
		}
	}
	return dst, found
}

func (b *bucket) clean() {
	b.mu.Lock()
	defer b.mu.Unlock()
	bGen := b.gen & ((1 << b.genSizeBits) - 1)
	bIdx := b.idx
	for k, v := range b.m {
		gen := v >> b.bucketSizeBits
		idx := v & ((1 << b.bucketSizeBits) - 1)
		if gen == bGen && idx < bIdx || gen+1 == bGen && idx >= bIdx || gen == b.maxGen && bGen == 1 && idx >= bIdx {
			continue
		}
		b.del(k)
	}
}

func (b *bucket) del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}
