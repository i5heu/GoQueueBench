package multiheadqueue

import (
	"runtime"
	"sync/atomic"
)

// cell represents a slot in a ring buffer, with padding to reduce false sharing.
type cell[T any] struct {
	_pad0    [8]uint64
	sequence uint64 // Expected sequence for synchronization.
	value    T      // Stored value.
	_pad1    [8]uint64
}

// shard is a single ring-buffer.
type shard[T any] struct {
	buffer     []cell[T]
	mask       uint64
	capacity   uint64
	enqueuePos uint64
	dequeuePos uint64
}

// NewShard creates a new shard with the given capacity (rounded up to a power of 2).
func NewShard[T any](capacity uint64) *shard[T] {
	if capacity&(capacity-1) != 0 {
		capPow := uint64(1)
		for capPow < capacity {
			capPow <<= 1
		}
		capacity = capPow
	}
	s := &shard[T]{
		buffer:   make([]cell[T], capacity),
		mask:     capacity - 1,
		capacity: capacity,
	}
	for i := uint64(0); i < capacity; i++ {
		s.buffer[i].sequence = i
	}
	return s
}

// Enqueue inserts a value into the shard.
func (s *shard[T]) Enqueue(val T) {
	for {
		pos := atomic.LoadUint64(&s.enqueuePos)
		c := &s.buffer[pos&s.mask]
		seq := atomic.LoadUint64(&c.sequence)
		// The cell is free when its sequence equals pos.
		if int64(seq)-int64(pos) == 0 {
			if atomic.CompareAndSwapUint64(&s.enqueuePos, pos, pos+1) {
				c.value = val
				atomic.StoreUint64(&c.sequence, pos+1)
				return
			}
		} else {
			runtime.Gosched()
		}
	}
}

// Dequeue removes and returns a value from the shard.
func (s *shard[T]) Dequeue() (T, bool) {
	const maxRetries = 32
	for retry := 0; retry < maxRetries; retry++ {
		pos := atomic.LoadUint64(&s.dequeuePos)
		c := &s.buffer[pos&s.mask]
		seq := atomic.LoadUint64(&c.sequence)
		// The cell is ready when its sequence equals pos+1.
		if int64(seq)-int64(pos+1) == 0 {
			if atomic.CompareAndSwapUint64(&s.dequeuePos, pos, pos+1) {
				val := c.value
				atomic.StoreUint64(&c.sequence, pos+s.capacity)
				return val, true
			}
			// CAS failed due to contention, retry immediately
			continue
		}
		// Check if shard is truly empty (enqueuePos == dequeuePos)
		enqPos := atomic.LoadUint64(&s.enqueuePos)
		if enqPos == pos {
			var zero T
			return zero, false
		}
		// Cell not ready yet (enqueue claimed slot but hasn't written yet)
		// Yield and retry
		runtime.Gosched()
	}
	// After max retries, return false to let caller try another shard
	var zero T
	return zero, false
}

// FreeSlots returns an approximate count of free slots in the shard.
func (s *shard[T]) FreeSlots() uint64 {
	enq := atomic.LoadUint64(&s.enqueuePos)
	deq := atomic.LoadUint64(&s.dequeuePos)
	return s.capacity - (enq - deq)
}

// UsedSlots returns an approximate count of occupied slots in the shard.
func (s *shard[T]) UsedSlots() uint64 {
	enq := atomic.LoadUint64(&s.enqueuePos)
	deq := atomic.LoadUint64(&s.dequeuePos)
	return enq - deq
}

// MultiHeadQueue is a sharded MPMC queue.
type MultiHeadQueue[T any] struct {
	shards       []*shard[T]
	numShards    uint64
	totalCap     uint64
	nextEnqShard uint64 // used for round-robin enqueues
	nextDeqShard uint64 // used for round-robin dequeues
}

// New creates a new MultiHeadQueue with the given total capacity and shard count.
// If numShards is zero, it defaults to runtime.NumCPU().
func New[T any](totalCapacity, numShards uint64) *MultiHeadQueue[T] {
	if numShards == 0 {
		numShards = uint64(runtime.NumCPU())
	}
	shardCap := totalCapacity / numShards
	if shardCap == 0 {
		shardCap = 1
	}
	shards := make([]*shard[T], numShards)
	for i := uint64(0); i < numShards; i++ {
		shards[i] = NewShard[T](shardCap)
	}
	return &MultiHeadQueue[T]{
		shards:    shards,
		numShards: numShards,
		totalCap:  shardCap * numShards,
	}
}

// Enqueue inserts a value by selecting a shard in round-robin fashion.
func (q *MultiHeadQueue[T]) Enqueue(val T) {
	idx := atomic.AddUint64(&q.nextEnqShard, 1) % q.numShards
	q.shards[idx].Enqueue(val)
}

// Dequeue scans all shards starting from a rotating index, retrying to find items.
func (q *MultiHeadQueue[T]) Dequeue() (T, bool) {
	// Try multiple rounds of scanning all shards
	const maxRounds = 16
	for round := 0; round < maxRounds; round++ {
		start := atomic.AddUint64(&q.nextDeqShard, 1) % q.numShards
		// Scan all shards in this round
		for i := uint64(0); i < q.numShards; i++ {
			idx := (start + i) % q.numShards
			if val, ok := q.shards[idx].Dequeue(); ok {
				return val, true
			}
		}
		// Check if all shards are truly empty before giving up
		totalUsed := uint64(0)
		for _, s := range q.shards {
			totalUsed += s.UsedSlots()
		}
		if totalUsed == 0 {
			var zero T
			return zero, false
		}
		// Items exist but aren't ready yet, yield and try again
		runtime.Gosched()
	}
	// After max rounds, return false to avoid infinite spinning
	var zero T
	return zero, false
}

// FreeSlots returns the total free slots across all shards.
func (q *MultiHeadQueue[T]) FreeSlots() uint64 {
	var free uint64
	for _, s := range q.shards {
		free += s.FreeSlots()
	}
	return free
}

// UsedSlots returns the total occupied slots across all shards.
func (q *MultiHeadQueue[T]) UsedSlots() uint64 {
	var used uint64
	for _, s := range q.shards {
		used += s.UsedSlots()
	}
	return used
}
