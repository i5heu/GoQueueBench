package optmpmc

import (
	"runtime"
	"sync/atomic"
)

// optCell represents one slot in the ring buffer with cache‑line padding.
type optCell[T any] struct {
	sequence uint64
	value    T
	_pad     [48]byte // pad to roughly 64 bytes per cell
}

// OptimizedMPMCQueue is a lock‑free MPMC queue with extra padding to reduce false sharing.
type OptimizedMPMCQueue[T any] struct {
	_pad0      [8]uint64    // padding to avoid false sharing
	enqueuePos uint64       // next enqueue position
	_pad1      [7]uint64    // complete a cache line
	dequeuePos uint64       // next dequeue position
	_pad2      [7]uint64    // additional padding
	buffer     []optCell[T] // ring buffer
	mask       uint64       // fast modulo (capacity - 1)
	capacity   uint64       // total capacity
}

// New creates a new OptimizedMPMCQueue with the given capacity (rounded up to a power of 2).
func New[T any](capacity uint64) *OptimizedMPMCQueue[T] {
	if capacity&(capacity-1) != 0 {
		capPow := uint64(1)
		for capPow < capacity {
			capPow <<= 1
		}
		capacity = capPow
	}
	q := &OptimizedMPMCQueue[T]{
		buffer:   make([]optCell[T], capacity),
		mask:     capacity - 1,
		capacity: capacity,
	}
	for i := uint64(0); i < capacity; i++ {
		q.buffer[i].sequence = i
	}
	return q
}

// Enqueue adds a value into the queue.
// It spins until a slot is available.
func (q *OptimizedMPMCQueue[T]) Enqueue(val T) {
	for {
		pos := atomic.LoadUint64(&q.enqueuePos)
		cell := &q.buffer[pos&q.mask]
		seq := atomic.LoadUint64(&cell.sequence)
		if int64(seq)-int64(pos) == 0 {
			if atomic.CompareAndSwapUint64(&q.enqueuePos, pos, pos+1) {
				cell.value = val
				atomic.StoreUint64(&cell.sequence, pos+1)
				return
			}
		} else {
			runtime.Gosched()
		}
	}
}

// Dequeue removes and returns a value from the queue.
// It retries if items are available but CAS fails due to contention.
func (q *OptimizedMPMCQueue[T]) Dequeue() (T, bool) {
	const maxRetries = 16
	for retry := 0; retry < maxRetries; retry++ {
		pos := atomic.LoadUint64(&q.dequeuePos)
		cell := &q.buffer[pos&q.mask]
		seq := atomic.LoadUint64(&cell.sequence)
		if int64(seq)-int64(pos+1) == 0 {
			if atomic.CompareAndSwapUint64(&q.dequeuePos, pos, pos+1) {
				ret := cell.value
				atomic.StoreUint64(&cell.sequence, pos+q.capacity)
				return ret, true
			}
			// CAS failed due to contention, retry immediately
			continue
		}
		// Check if queue is empty
		if atomic.LoadUint64(&q.enqueuePos) == pos {
			var zero T
			return zero, false
		}
		// Cell not ready yet, yield and retry
		runtime.Gosched()
	}
	var zero T
	return zero, false
}

// FreeSlots returns how many slots are free.
func (q *OptimizedMPMCQueue[T]) FreeSlots() uint64 {
	return q.capacity - q.UsedSlots()
}

// UsedSlots returns an approximate count of used slots.
func (q *OptimizedMPMCQueue[T]) UsedSlots() uint64 {
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	return enq - deq
}
