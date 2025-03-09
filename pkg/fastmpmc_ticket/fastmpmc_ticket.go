package fastmpmc_ticket

import (
	"runtime"
	"sync/atomic"
)

// ticketCell represents one slot in the ring buffer.
// We pad before and after the key fields to help reduce false sharing.
type ticketCell[T any] struct {
	_pad0    [8]uint64
	sequence uint64 // Sequence number for synchronization.
	value    T      // The stored value.
	_pad1    [8]uint64
}

// FastMPMCQueueTicket is a bounded, lock‑free multi‑producer/multi‑consumer queue
// using a ticket‑based approach to reduce contention on the shared counters.
type FastMPMCQueueTicket[T any] struct {
	_pad0      [8]uint64
	buffer     []ticketCell[T]
	mask       uint64 // equals capacity - 1 (capacity is a power of 2)
	capacity   uint64
	_pad1      [8]uint64
	enqueuePos uint64 // ticket for enqueues
	_pad2      [8]uint64
	dequeuePos uint64 // ticket for dequeues
	_pad3      [8]uint64
}

// New creates a new FastMPMCQueueTicket with the specified capacity.
// If capacity is not a power of 2, it is rounded up.
func New[T any](capacity uint64) *FastMPMCQueueTicket[T] {
	// Round up capacity to the next power of 2 if necessary.
	if capacity&(capacity-1) != 0 {
		capPow := uint64(1)
		for capPow < capacity {
			capPow <<= 1
		}
		capacity = capPow
	}
	q := &FastMPMCQueueTicket[T]{
		buffer:   make([]ticketCell[T], capacity),
		mask:     capacity - 1,
		capacity: capacity,
	}
	// Initialize each cell's sequence to its index.
	for i := uint64(0); i < capacity; i++ {
		q.buffer[i].sequence = i
	}
	return q
}

// Enqueue inserts a value into the queue.
// It busy-waits until a slot becomes available.
func (q *FastMPMCQueueTicket[T]) Enqueue(val T) {
	// Reserve a slot by atomically incrementing enqueuePos.
	pos := atomic.AddUint64(&q.enqueuePos, 1) - 1
	cell := &q.buffer[pos&q.mask]
	// Wait until the cell is ready for writing.
	// The expected sequence for an empty slot is exactly 'pos'.
	for {
		seq := atomic.LoadUint64(&cell.sequence)
		if seq == pos {
			break
		}
		runtime.Gosched()
	}
	// Write the value.
	cell.value = val
	// Publish the value for consumers by setting the sequence to pos+1.
	atomic.StoreUint64(&cell.sequence, pos+1)
}

// Dequeue removes and returns a value from the queue.
// It busy-waits until a value is available or the queue is empty.
// The method first checks if the cell at the current dequeue position is ready.
// If not, it verifies whether the queue is truly empty (i.e. no producer holds a ticket for that slot).
func (q *FastMPMCQueueTicket[T]) Dequeue() (T, bool) {
	for {
		// Read the current dequeue position.
		pos := atomic.LoadUint64(&q.dequeuePos)
		cell := &q.buffer[pos&q.mask]
		seq := atomic.LoadUint64(&cell.sequence)
		// If the cell is ready (expected sequence equals pos+1), try to claim it.
		if seq == pos+1 {
			if atomic.CompareAndSwapUint64(&q.dequeuePos, pos, pos+1) {
				val := cell.value
				// Mark the cell as free by setting its sequence to pos + capacity.
				atomic.StoreUint64(&cell.sequence, pos+q.capacity)
				return val, true
			}
		} else {
			// Check if the queue is truly empty.
			enq := atomic.LoadUint64(&q.enqueuePos)
			if enq-pos == 0 {
				var zero T
				return zero, false
			}
		}
		runtime.Gosched()
	}
}

// FreeSlots returns an approximate count of free slots in the queue.
func (q *FastMPMCQueueTicket[T]) FreeSlots() uint64 {
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	return q.capacity - (enq - deq)
}

// UsedSlots returns an approximate count of occupied slots in the queue.
func (q *FastMPMCQueueTicket[T]) UsedSlots() uint64 {
	enq := atomic.LoadUint64(&q.enqueuePos)
	deq := atomic.LoadUint64(&q.dequeuePos)
	return enq - deq
}
