package lightningqueue

import (
	"runtime"
	"sync/atomic"
)

// cell is one slot in the ring buffer.
// Padding is added before and after key fields to reduce false sharing.
type cell[T any] struct {
	_pad0    [8]uint64
	sequence uint64 // Expected sequence for synchronization.
	value    T      // Stored value.
	_pad1    [8]uint64
}

// LightningQueue is a bounded, lock‑free, multi‑producer/multi‑consumer FIFO queue.
type LightningQueue[T any] struct {
	_pad0      [8]uint64
	buffer     []cell[T]
	mask       uint64 // mask = capacity - 1; capacity is a power of 2.
	capacity   uint64
	_pad1      [8]uint64
	enqueuePos uint64 // Ticket for enqueues.
	_pad2      [8]uint64
	dequeuePos uint64 // Ticket for dequeues.
	_pad3      [8]uint64
}

// New creates a new LightningQueue with the specified capacity.
// If capacity is not a power of 2, it is rounded up.
func New[T any](capacity uint64) *LightningQueue[T] {
	if capacity&(capacity-1) != 0 {
		capPow := uint64(1)
		for capPow < capacity {
			capPow <<= 1
		}
		capacity = capPow
	}
	q := &LightningQueue[T]{
		buffer:   make([]cell[T], capacity),
		mask:     capacity - 1,
		capacity: capacity,
	}
	// Initialize each cell's sequence number to its index.
	for i := uint64(0); i < capacity; i++ {
		q.buffer[i].sequence = i
	}
	return q
}

// Enqueue inserts a value into the queue.
// It busy-waits until a slot becomes available.
func (q *LightningQueue[T]) Enqueue(val T) {
	for {
		pos := atomic.LoadUint64(&q.enqueuePos)
		cell := &q.buffer[pos&q.mask]
		seq := atomic.LoadUint64(&cell.sequence)
		// A cell is available when its sequence equals pos.
		if seq == pos {
			// Try to claim the slot.
			if atomic.CompareAndSwapUint64(&q.enqueuePos, pos, pos+1) {
				cell.value = val
				// Publish the value: update sequence to pos+1.
				atomic.StoreUint64(&cell.sequence, pos+1)
				return
			}
		} else {
			// Minimal yielding to reduce spin-loop overhead.
			runtime.Gosched()
		}
	}
}

// Dequeue removes and returns a value from the queue.
// If no value is available, it returns the zero value and false.
func (q *LightningQueue[T]) Dequeue() (T, bool) {
	for {
		pos := atomic.LoadUint64(&q.dequeuePos)
		cell := &q.buffer[pos&q.mask]
		seq := atomic.LoadUint64(&cell.sequence)
		// A cell is ready to be dequeued when its sequence equals pos+1.
		if seq == pos+1 {
			if atomic.CompareAndSwapUint64(&q.dequeuePos, pos, pos+1) {
				val := cell.value
				// Mark the cell as free by updating its sequence.
				atomic.StoreUint64(&cell.sequence, pos+q.capacity)
				return val, true
			}
		} else {
			// Check if the queue is truly empty.
			if atomic.LoadUint64(&q.enqueuePos)-pos == 0 {
				var zero T
				return zero, false
			}
			runtime.Gosched()
		}
	}
}

// FreeSlots returns an approximate count of free slots.
func (q *LightningQueue[T]) FreeSlots() uint64 {
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	return q.capacity - (enq - deq)
}

// UsedSlots returns an approximate count of occupied slots.
func (q *LightningQueue[T]) UsedSlots() uint64 {
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	return enq - deq
}
