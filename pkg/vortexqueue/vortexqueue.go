package vortexqueue

import (
	"runtime"
	"sync/atomic"
)

const spinIterations = 100 // iterations to spin before yielding

// vortexCell is one slot in the ring buffer.
// Padding is added to reduce false sharing.
type vortexCell[T any] struct {
	_pad0    [8]uint64
	sequence uint64 // expected sequence for synchronization
	value    T      // stored value
	_pad1    [8]uint64
}

// VortexQueue is a bounded, lock‑free, multi‑producer/multi‑consumer FIFO queue.
// It uses a refined spin‑wait loop to reduce overhead under high concurrency.
type VortexQueue[T any] struct {
	_pad0      [8]uint64
	buffer     []vortexCell[T]
	mask       uint64 // capacity - 1 (capacity is a power of 2)
	capacity   uint64
	_pad1      [8]uint64
	enqueuePos uint64 // ticket for enqueues
	_pad2      [8]uint64
	dequeuePos uint64 // ticket for dequeues
	_pad3      [8]uint64
}

// New creates a new VortexQueue with the given capacity (rounded up to the next power of 2).
func New[T any](capacity uint64) *VortexQueue[T] {
	if capacity&(capacity-1) != 0 {
		capPow := uint64(1)
		for capPow < capacity {
			capPow <<= 1
		}
		capacity = capPow
	}
	q := &VortexQueue[T]{
		buffer:   make([]vortexCell[T], capacity),
		mask:     capacity - 1,
		capacity: capacity,
	}
	// Initialize each cell's sequence to its index.
	for i := uint64(0); i < capacity; i++ {
		q.buffer[i].sequence = i
	}
	return q
}

// spinWait performs a tight spin loop and yields after a fixed number of iterations.
func spinWait() {
	for i := 0; i < spinIterations; i++ {
		// Intentionally empty.
	}
	runtime.Gosched()
}

// Enqueue inserts a value into the queue.
// It busy-waits until a slot is available.
func (q *VortexQueue[T]) Enqueue(val T) {
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
		}
		spinWait()
	}
}

// Dequeue removes and returns a value from the queue.
// If no value is available, it returns the zero value and false.
func (q *VortexQueue[T]) Dequeue() (T, bool) {
	for {
		pos := atomic.LoadUint64(&q.dequeuePos)
		cell := &q.buffer[pos&q.mask]
		seq := atomic.LoadUint64(&cell.sequence)
		// A cell is ready to be dequeued when its sequence equals pos+1.
		if seq == pos+1 {
			if atomic.CompareAndSwapUint64(&q.dequeuePos, pos, pos+1) {
				val := cell.value
				// Mark the cell as free by setting its sequence to pos + capacity.
				atomic.StoreUint64(&cell.sequence, pos+q.capacity)
				return val, true
			}
		} else {
			// If the queue is empty, return immediately.
			if atomic.LoadUint64(&q.enqueuePos)-pos == 0 {
				var zero T
				return zero, false
			}
		}
		spinWait()
	}
}

// FreeSlots returns an approximate count of free slots in the queue.
func (q *VortexQueue[T]) FreeSlots() uint64 {
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	return q.capacity - (enq - deq)
}

// UsedSlots returns an approximate count of occupied slots in the queue.
func (q *VortexQueue[T]) UsedSlots() uint64 {
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	return enq - deq
}
