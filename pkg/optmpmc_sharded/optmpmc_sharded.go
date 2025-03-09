package optmpmc_sharded

import (
	"math/rand"
	"runtime"
	"sync/atomic"
)

// optCell is the padded slot used by each shard's ring buffer.
type optCell[T any] struct {
	sequence uint64
	value    T
	_pad     [48]byte // pad to roughly 64 bytes per cell
}

// shard holds one ring buffer plus its enqueue/dequeue positions.
type shard[T any] struct {
	_pad0      [8]uint64
	enqueuePos uint64
	_pad1      [7]uint64
	dequeuePos uint64
	_pad2      [7]uint64

	buffer   []optCell[T]
	mask     uint64
	capacity uint64
}

// ShardedOptimizedMPMCQueue is a lock‑free, sharded MPMC queue.
type ShardedOptimizedMPMCQueue[T any] struct {
	shards    []*shard[T]
	numShards uint64
	// totalCapacity is the logical capacity reported by FreeSlots()/UsedSlots().
	totalCapacity uint64
}

// New creates a new sharded queue with the specified total capacity.
// The number of shards is determined by
//
//	var numShards uint64 = uint64(runtime.NumCPU())
//
// and the logical capacity is partitioned among shards so that the sum equals the requested capacity.
// Each shard's internal buffer is allocated with a capacity that is a power‑of‑two.
func New[T any](capacity uint64) *ShardedOptimizedMPMCQueue[T] {
	const minShardCap = 64
	var numShards uint64 = uint64(runtime.NumCPU())
	if capacity < minShardCap {
		numShards = 1
	} else if capacity/minShardCap < numShards {
		numShards = capacity / minShardCap
		if numShards == 0 {
			numShards = 1
		}
	}

	// Partition the logical capacity among shards.
	// Compute the base logical capacity per shard.
	base := capacity / numShards
	// p is the largest power‑of‑two less than or equal to base.
	var p uint64 = 1
	for p*2 <= base {
		p *= 2
	}
	// k shards will receive 2*p slots and the rest will receive p slots.
	k := int(capacity/p) - int(numShards)

	shards := make([]*shard[T], numShards)
	for i := uint64(0); i < numShards; i++ {
		capForShard := p
		if int(i) < k {
			capForShard = 2 * p
		}
		shards[i] = newShard[T](capForShard)
	}

	return &ShardedOptimizedMPMCQueue[T]{
		shards:        shards,
		numShards:     numShards,
		totalCapacity: capacity, // logical capacity exactly as requested
	}
}

// newShard creates one shard with the given capacity (which must be a power‑of‑two).
func newShard[T any](capacity uint64) *shard[T] {
	s := &shard[T]{
		buffer:   make([]optCell[T], capacity),
		mask:     capacity - 1,
		capacity: capacity,
	}
	for i := uint64(0); i < capacity; i++ {
		s.buffer[i].sequence = i
	}
	return s
}

// Enqueue adds a value into one of the shards selected at random.
// It spins within the chosen shard until a slot is available.
func (q *ShardedOptimizedMPMCQueue[T]) Enqueue(val T) {
	for {
		// Use math/rand's per‑thread generator (Go 1.22's math/rand/v2) to pick a shard.
		shardIndex := uint64(rand.Int63()) % q.numShards
		s := q.shards[shardIndex]

		for {
			pos := atomic.LoadUint64(&s.enqueuePos)
			cell := &s.buffer[pos&s.mask]
			seq := atomic.LoadUint64(&cell.sequence)
			if int64(seq)-int64(pos) == 0 {
				if atomic.CompareAndSwapUint64(&s.enqueuePos, pos, pos+1) {
					cell.value = val
					atomic.StoreUint64(&cell.sequence, pos+1)
					return
				}
			} else if int64(seq)-int64(pos) < 0 {
				// This shard appears full; break to try a different shard.
				break
			} else {
				runtime.Gosched()
			}
		}
		runtime.Gosched()
	}
}

// Dequeue removes a value from one of the shards selected at random.
// If the chosen shard is empty and a check shows that all shards are empty,
// it immediately returns with false.
func (q *ShardedOptimizedMPMCQueue[T]) Dequeue() (T, bool) {
	for {
		shardIndex := uint64(rand.Int63()) % q.numShards
		s := q.shards[shardIndex]

		pos := atomic.LoadUint64(&s.dequeuePos)
		cell := &s.buffer[pos&s.mask]
		seq := atomic.LoadUint64(&cell.sequence)
		if int64(seq)-int64(pos+1) == 0 {
			if atomic.CompareAndSwapUint64(&s.dequeuePos, pos, pos+1) {
				ret := cell.value
				atomic.StoreUint64(&cell.sequence, pos+s.capacity)
				return ret, true
			}
		} else {
			if q.checkAllEmpty() {
				break
			}
		}
	}
	var zero T
	return zero, false
}

// FreeSlots returns how many logical slots remain free across all shards.
func (q *ShardedOptimizedMPMCQueue[T]) FreeSlots() uint64 {
	return q.totalCapacity - q.UsedSlots()
}

// UsedSlots returns the total number of used slots across shards.
func (q *ShardedOptimizedMPMCQueue[T]) UsedSlots() uint64 {
	var totalUsed uint64
	for _, s := range q.shards {
		enq := atomic.LoadUint64(&s.enqueuePos)
		deq := atomic.LoadUint64(&s.dequeuePos)
		totalUsed += enq - deq
	}
	return totalUsed
}

// checkAllEmpty returns true if every shard is empty.
func (q *ShardedOptimizedMPMCQueue[T]) checkAllEmpty() bool {
	for _, s := range q.shards {
		if atomic.LoadUint64(&s.enqueuePos) != atomic.LoadUint64(&s.dequeuePos) {
			return false
		}
	}
	return true
}
