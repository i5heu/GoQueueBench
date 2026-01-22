package basicmpmc

import (
	"runtime"
	"sync/atomic"
)

// basicCell represents one slot in the ring buffer.
type basicCell[T any] struct {
	sequence uint64
	value    T
}

// BasicMPMCQueue is a bounded, lock‑free, multi‑producer/multi‑consumer queue.
type BasicMPMCQueue[T any] struct {
	buffer     []basicCell[T]
	mask       uint64
	capacity   uint64
	enqueuePos uint64
	dequeuePos uint64
}

// New creates a new BasicMPMCQueue with the given capacity (rounded up to a power of 2).
func New[T any](capacity uint64) *BasicMPMCQueue[T] {
	if capacity&(capacity-1) != 0 {
		capPow := uint64(1)
		for capPow < capacity {
			capPow <<= 1
		}
		capacity = capPow
	}
	q := &BasicMPMCQueue[T]{
		buffer:   make([]basicCell[T], capacity),
		mask:     capacity - 1,
		capacity: capacity,
	}
	for i := uint64(0); i < capacity; i++ {
		q.buffer[i].sequence = i
	}
	return q
}

// Enqueue inserts a value into the queue.
// It spins until a slot is available.
func (q *BasicMPMCQueue[T]) Enqueue(val T) {
	for {
		pos := atomic.LoadUint64(&q.enqueuePos)
		cell := &q.buffer[pos&q.mask]
		seq := atomic.LoadUint64(&cell.sequence)
		// If the cell is free to write (expected sequence equals pos)
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
func (q *BasicMPMCQueue[T]) Dequeue() (T, bool) {
	const maxRetries = 16
	for retry := 0; retry < maxRetries; retry++ {
		pos := atomic.LoadUint64(&q.dequeuePos)
		cell := &q.buffer[pos&q.mask]
		seq := atomic.LoadUint64(&cell.sequence)
		// If the cell is full (expected sequence equals pos+1)
		if int64(seq)-int64(pos+1) == 0 {
			if atomic.CompareAndSwapUint64(&q.dequeuePos, pos, pos+1) {
				ret := cell.value
				// Mark the cell as free by setting sequence to pos + capacity
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
func (q *BasicMPMCQueue[T]) FreeSlots() uint64 {
	// Approximate: capacity minus the number of used slots.
	return q.capacity - q.UsedSlots()
}

// UsedSlots returns an approximate count of used slots.
func (q *BasicMPMCQueue[T]) UsedSlots() uint64 {
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	return enq - deq
}
