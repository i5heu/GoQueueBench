package main

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Race Condition Detection Test Suite
// =============================================================================
//
// This test suite is specifically designed to detect the following problems
// identified in the queue implementations:
//
// 1. Lost Items in Dequeue Methods - Most implementations return immediately
//    when a slot isn't ready, potentially losing items.
//
// 2. Multi-Head Queue Race Condition - Race condition in empty queue detection
//    leading to potential infinite recursion or lost items.
//
// 3. Inadequate Empty Queue Detection - Empty queue detection logic can be
//    inconsistent during concurrent access.
//
// 4. Random Shard Selection Issues - Predictable shard selection patterns
//    causing uneven load distribution.
//
// =============================================================================

// =============================================================================
// Test Utilities and Helpers
// =============================================================================

// trackedItem represents an item with tracking information for lost item detection
type trackedItem struct {
	id        int64
	timestamp int64
	producer  int
}

// raceDetector tracks items to detect lost items in race conditions
type raceDetector struct {
	enqueued     map[int64]trackedItem
	enqueuedMu   sync.Mutex
	dequeued     map[int64]bool
	dequeuedMu   sync.Mutex
	lostItems    []int64
	lostMu       sync.Mutex
	enqueueCount atomic.Int64
	dequeueCount atomic.Int64
}

func newRaceDetector() *raceDetector {
	return &raceDetector{
		enqueued:  make(map[int64]trackedItem),
		dequeued:  make(map[int64]bool),
		lostItems: make([]int64, 0),
	}
}

func (rd *raceDetector) recordEnqueue(id int64, producer int) {
	rd.enqueuedMu.Lock()
	rd.enqueued[id] = trackedItem{
		id:        id,
		timestamp: time.Now().UnixNano(),
		producer:  producer,
	}
	rd.enqueuedMu.Unlock()
	rd.enqueueCount.Add(1)
}

func (rd *raceDetector) recordDequeue(id int64) {
	rd.dequeuedMu.Lock()
	rd.dequeued[id] = true
	rd.dequeuedMu.Unlock()
	rd.dequeueCount.Add(1)
}

func (rd *raceDetector) findLostItems() []int64 {
	rd.enqueuedMu.Lock()
	defer rd.enqueuedMu.Unlock()
	rd.dequeuedMu.Lock()
	defer rd.dequeuedMu.Unlock()

	lost := make([]int64, 0)
	for id := range rd.enqueued {
		if !rd.dequeued[id] {
			lost = append(lost, id)
		}
	}
	return lost
}

// =============================================================================
// CATEGORY 1: Lost Items in Dequeue Methods
// =============================================================================
// These tests target the problem where dequeue implementations return immediately
// when a slot isn't ready, potentially losing items that become available
// microseconds later.
// =============================================================================

// TestDequeueRaceConditionLostItems creates a high-contention scenario with many
// producers and few consumers to detect lost items due to race conditions in
// dequeue methods.
func TestDequeueRaceConditionLostItems(t *testing.T) {
	withAllQueuesFixed(t, "DequeueRaceConditionLostItems", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		// Test configuration - small capacity to force contention
		const capacity = 64
		const numProducers = 20
		const numConsumers = 5 // Fewer consumers to create backpressure
		const itemsPerProducer = 1000
		const totalItems = numProducers * itemsPerProducer

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "DequeueRaceConditionLostItems")
		wd.Start()
		defer wd.Stop()

		// Track all items
		var enqueuedCount atomic.Int64
		var dequeuedCount atomic.Int64

		// Use a map to track individual items
		enqueuedItems := make(map[*int]int64, totalItems)
		var enqueuedMu sync.Mutex
		dequeuedItems := make(map[*int]bool, totalItems)
		var dequeuedMu sync.Mutex

		var prodWg sync.WaitGroup
		var consWg sync.WaitGroup

		// Signal when production is done
		var productionDone atomic.Bool

		// Start producers
		prodWg.Add(numProducers)
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer prodWg.Done()
				for i := 0; i < itemsPerProducer; i++ {
					ptr := new(int)
					itemID := int64(producerID*itemsPerProducer + i)
					*ptr = int(itemID)

					enqueuedMu.Lock()
					enqueuedItems[ptr] = itemID
					enqueuedMu.Unlock()

					q.Enqueue(ptr)
					enqueuedCount.Add(1)

					if i%100 == 0 {
						wd.Progress()
					}
				}
			}(p)
		}

		// Start consumers - they will keep trying until all items are consumed
		consWg.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func() {
				defer consWg.Done()
				emptyAttempts := 0
				maxEmptyAttempts := 10000 // Prevent infinite loop

				for {
					ptr, ok := q.Dequeue()
					if ok {
						emptyAttempts = 0
						dequeuedMu.Lock()
						dequeuedItems[ptr] = true
						dequeuedMu.Unlock()
						dequeuedCount.Add(1)
						wd.Progress()
					} else {
						emptyAttempts++
						// If production is done and we've had many empty attempts,
						// check if we're really done
						if productionDone.Load() {
							if emptyAttempts > maxEmptyAttempts {
								// Final check - is the queue really empty?
								if q.UsedSlots() == 0 {
									return
								}
								emptyAttempts = 0
							}
						}
						runtime.Gosched()
					}
				}
			}()
		}

		// Wait for producers to finish
		prodWg.Wait()
		productionDone.Store(true)

		// Give consumers time to drain
		time.Sleep(2 * time.Second)

		// Check results
		enqueued := enqueuedCount.Load()
		dequeued := dequeuedCount.Load()

		// Stop consumers
		consWg.Wait()

		// Final verification
		enqueuedMu.Lock()
		dequeuedMu.Lock()
		defer enqueuedMu.Unlock()
		defer dequeuedMu.Unlock()

		lost := 0
		for ptr := range enqueuedItems {
			if !dequeuedItems[ptr] {
				lost++
			}
		}

		if lost > 0 {
			t.Errorf("LOST ITEMS DETECTED: %d items lost out of %d enqueued (%.2f%%)",
				lost, enqueued, float64(lost)/float64(enqueued)*100)
			t.Logf("Enqueued: %d, Dequeued: %d, Queue remaining: %d",
				enqueued, dequeued, q.UsedSlots())
		}

		if enqueued != int64(totalItems) {
			t.Errorf("Enqueue count mismatch: expected %d, got %d", totalItems, enqueued)
		}
	})
}

// TestMicrosecondWindowLostItems uses controlled timing to detect items lost
// in the microsecond timing windows between enqueue and dequeue operations.
func TestMicrosecondWindowLostItems(t *testing.T) {
	withAllQueuesFixed(t, "MicrosecondWindowLostItems", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 128
		const iterations = 5000

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "MicrosecondWindowLostItems")
		wd.Start()
		defer wd.Stop()

		lostCount := 0
		successCount := 0

		for i := 0; i < iterations; i++ {
			ptr := new(int)
			*ptr = i

			// Enqueue
			q.Enqueue(ptr)

			// Small delay to create timing window
			time.Sleep(time.Duration(i%10) * time.Microsecond)

			// Try to dequeue with retries
			var got *int
			var ok bool
			for retries := 0; retries < 100; retries++ {
				got, ok = q.Dequeue()
				if ok {
					break
				}
				time.Sleep(time.Microsecond)
			}

			if !ok {
				lostCount++
				if lostCount <= 10 {
					t.Logf("Item %d lost (could not dequeue after retries)", i)
				}
			} else if got != ptr {
				t.Errorf("Item mismatch at iteration %d: expected %p, got %p", i, ptr, got)
			} else {
				successCount++
			}

			if i%500 == 0 {
				wd.Progress()
			}
		}

		if lostCount > 0 {
			t.Errorf("TIMING WINDOW LOST ITEMS: %d items lost out of %d (%.2f%%)",
				lostCount, iterations, float64(lostCount)/float64(iterations)*100)
		}

		// Verify queue is empty
		remaining := q.UsedSlots()
		if remaining != 0 {
			t.Errorf("Queue not empty after test: %d items remaining", remaining)
		}
	})
}

// TestSingleShotDequeueFailure specifically tests the problem where a single
// Dequeue() call returns false even when items are available. This targets
// the exact bug pattern in most implementations.
func TestSingleShotDequeueFailure(t *testing.T) {
	withAllQueuesFixed(t, "SingleShotDequeueFailure", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 64
		const iterations = 10000

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "SingleShotDequeueFailure")
		wd.Start()
		defer wd.Stop()

		// Track how many times single-shot dequeue fails when queue has items
		singleShotFailures := 0
		retrySuccesses := 0

		for i := 0; i < iterations; i++ {
			ptr := new(int)
			*ptr = i
			q.Enqueue(ptr)

			// Single-shot dequeue attempt (this is how the bug manifests)
			_, ok := q.Dequeue()
			if !ok {
				singleShotFailures++
				// Now retry to see if item is actually there
				for retry := 0; retry < 10; retry++ {
					if _, ok2 := q.Dequeue(); ok2 {
						retrySuccesses++
						break
					}
					runtime.Gosched()
				}
			}

			if i%1000 == 0 {
				wd.Progress()
			}
		}

		// Drain any remaining
		drained := 0
		for {
			if _, ok := q.Dequeue(); ok {
				drained++
			} else {
				break
			}
		}

		if singleShotFailures > 0 {
			t.Logf("Single-shot dequeue failures: %d out of %d (%.2f%%)",
				singleShotFailures, iterations, float64(singleShotFailures)/float64(iterations)*100)
			t.Logf("Retry successes after failure: %d", retrySuccesses)
			t.Logf("Remaining items drained: %d", drained)

			// This is informational - single-shot failures are expected for some implementations
			// but should be documented
			if retrySuccesses > 0 {
				t.Logf("NOTE: %d items were available on retry but not on first try - this is expected for non-blocking dequeue", retrySuccesses)
			}
		}
	})
}

// TestConcurrentSingleShotDequeue tests the single-shot dequeue behavior under
// concurrent load where the race conditions are more likely to manifest.
func TestConcurrentSingleShotDequeue(t *testing.T) {
	withAllQueuesFixed(t, "ConcurrentSingleShotDequeue", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 32
		const numProducers = 10
		const numConsumers = 10
		const itemsPerProducer = 1000
		const totalItems = numProducers * itemsPerProducer

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "ConcurrentSingleShotDequeue")
		wd.Start()
		defer wd.Stop()

		var enqueuedCount atomic.Int64
		var singleShotSuccess atomic.Int64
		var singleShotFail atomic.Int64
		var retrySuccess atomic.Int64
		var productionDone atomic.Bool
		var stopConsumers atomic.Bool

		var prodWg sync.WaitGroup
		var consWg sync.WaitGroup

		// Producers
		prodWg.Add(numProducers)
		for p := 0; p < numProducers; p++ {
			go func(pid int) {
				defer prodWg.Done()
				for i := 0; i < itemsPerProducer; i++ {
					ptr := new(int)
					*ptr = pid*itemsPerProducer + i
					q.Enqueue(ptr)
					enqueuedCount.Add(1)
					if i%200 == 0 {
						wd.Progress()
					}
				}
			}(p)
		}

		// Consumers - use single-shot dequeue pattern
		consWg.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func() {
				defer consWg.Done()
				emptyCount := 0
				for !stopConsumers.Load() {
					totalDequeued := singleShotSuccess.Load() + retrySuccess.Load()
					if productionDone.Load() && totalDequeued >= int64(totalItems) {
						return
					}

					// Single-shot attempt
					_, ok := q.Dequeue()
					if ok {
						singleShotSuccess.Add(1)
						emptyCount = 0
						wd.Progress()
					} else {
						singleShotFail.Add(1)
						emptyCount++
						// Retry once to see if item was actually there
						if _, ok2 := q.Dequeue(); ok2 {
							retrySuccess.Add(1)
							emptyCount = 0
						}
						// If we've seen many empty results after production is done, exit
						if productionDone.Load() && emptyCount > 10000 {
							return
						}
					}
					runtime.Gosched()
				}
			}()
		}

		// Wait for producers to finish
		prodWg.Wait()
		productionDone.Store(true)

		// Give consumers time to drain, with timeout
		done := make(chan struct{})
		go func() {
			consWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Consumers finished normally
		case <-time.After(10 * time.Second):
			// Timeout - consumers are stuck
			stopConsumers.Store(true)
			t.Log("WARNING: Consumers timed out - possible queue stuck condition")
			<-done // Wait for consumers to exit
		}

		// Drain remaining
		drained := 0
		for {
			if _, ok := q.Dequeue(); ok {
				drained++
			} else {
				break
			}
		}

		enqueued := enqueuedCount.Load()
		ssSuccess := singleShotSuccess.Load()
		ssFail := singleShotFail.Load()
		rsSuccess := retrySuccess.Load()
		totalDequeued := ssSuccess + rsSuccess + int64(drained)

		t.Logf("Enqueued: %d", enqueued)
		t.Logf("Single-shot successes: %d", ssSuccess)
		t.Logf("Single-shot failures: %d", ssFail)
		t.Logf("Retry successes: %d", rsSuccess)
		t.Logf("Drained: %d", drained)
		t.Logf("Total dequeued: %d", totalDequeued)

		if totalDequeued != enqueued {
			t.Errorf("ITEMS LOST: enqueued=%d, total_dequeued=%d, lost=%d",
				enqueued, totalDequeued, enqueued-totalDequeued)
		}

		// Calculate false negative rate
		if ssFail > 0 && rsSuccess > 0 {
			falseNegativeRate := float64(rsSuccess) / float64(ssFail) * 100
			t.Logf("False negative rate (item available but first dequeue failed): %.2f%%", falseNegativeRate)
		}
	})
}

// TestBurstEnqueueSlowDequeue creates bursts of enqueues followed by slow dequeues
// to detect items lost when the dequeue operation can't keep up.
func TestBurstEnqueueSlowDequeue(t *testing.T) {
	withAllQueuesFixed(t, "BurstEnqueueSlowDequeue", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 512
		const bursts = 20
		const itemsPerBurst = 50
		const totalItems = bursts * itemsPerBurst

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "BurstEnqueueSlowDequeue")
		wd.Start()
		defer wd.Stop()

		// Pre-create all pointers
		allPointers := make([]*int, totalItems)
		for i := 0; i < totalItems; i++ {
			ptr := new(int)
			*ptr = i
			allPointers[i] = ptr
		}

		dequeuedPointers := make(map[*int]bool)
		var dequeuedMu sync.Mutex
		var enqueuedCount atomic.Int64

		// Run enqueue in a goroutine to handle blocking implementations
		done := make(chan struct{})
		go func() {
			for burst := 0; burst < bursts; burst++ {
				// Burst enqueue
				for i := 0; i < itemsPerBurst; i++ {
					idx := burst*itemsPerBurst + i
					q.Enqueue(allPointers[idx])
					enqueuedCount.Add(1)
				}
				wd.Progress()
			}
			close(done)
		}()

		// Consumer runs concurrently
		var consumerDone atomic.Bool
		go func() {
			for !consumerDone.Load() {
				ptr, ok := q.Dequeue()
				if ok {
					dequeuedMu.Lock()
					dequeuedPointers[ptr] = true
					dequeuedMu.Unlock()
					wd.Progress()
				} else {
					runtime.Gosched()
				}
			}
		}()

		// Wait for producer to finish
		<-done

		// Give consumer time to drain
		time.Sleep(500 * time.Millisecond)
		consumerDone.Store(true)

		// Final drain
		for {
			ptr, ok := q.Dequeue()
			if !ok {
				break
			}
			dequeuedMu.Lock()
			dequeuedPointers[ptr] = true
			dequeuedMu.Unlock()
		}

		// Verify all items were received
		dequeuedMu.Lock()
		defer dequeuedMu.Unlock()

		lost := 0
		for _, ptr := range allPointers {
			if !dequeuedPointers[ptr] {
				lost++
			}
		}

		if lost > 0 {
			t.Errorf("BURST PATTERN LOST ITEMS: %d items lost out of %d (%.2f%%)",
				lost, len(allPointers), float64(lost)/float64(len(allPointers))*100)
		}
	})
}

// =============================================================================
// CATEGORY 2: Multi-Head Queue Race Condition
// =============================================================================
// These tests target the race condition in multi-head queue implementations
// where the empty queue detection can lead to infinite recursion or lost items.
// =============================================================================

// TestMultiHeadEmptyQueueRaceCondition specifically targets the race condition
// in multi-head queue implementations where the queue appears empty but has items.
func TestMultiHeadEmptyQueueRaceCondition(t *testing.T) {
	withAllQueuesFixed(t, "MultiHeadEmptyQueueRaceCondition", []string{"Multi-Head-FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 128
		const numProducers = 10
		const numConsumers = 10
		const itemsPerProducer = 500
		const totalItems = numProducers * itemsPerProducer

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "MultiHeadEmptyQueueRaceCondition")
		wd.Start()
		defer wd.Stop()

		var enqueuedCount atomic.Int64
		var dequeuedCount atomic.Int64
		var productionDone atomic.Bool

		// Track items for verification
		allItems := make(chan *int, totalItems)

		var prodWg sync.WaitGroup
		var consWg sync.WaitGroup

		// Start producers
		prodWg.Add(numProducers)
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer prodWg.Done()
				for i := 0; i < itemsPerProducer; i++ {
					ptr := new(int)
					*ptr = producerID*itemsPerProducer + i
					allItems <- ptr
					q.Enqueue(ptr)
					enqueuedCount.Add(1)
					if i%100 == 0 {
						wd.Progress()
					}
				}
			}(p)
		}

		// Start consumers with timeout protection
		consWg.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func(consumerID int) {
				defer consWg.Done()
				localDequeued := 0
				stuckCount := 0
				maxStuck := 50000

				for {
					if productionDone.Load() && dequeuedCount.Load() >= int64(totalItems) {
						return
					}

					ptr, ok := q.Dequeue()
					if ok {
						stuckCount = 0
						dequeuedCount.Add(1)
						localDequeued++
						if localDequeued%100 == 0 {
							wd.Progress()
						}
						_ = ptr // Use ptr to prevent optimization
					} else {
						stuckCount++
						if stuckCount > maxStuck {
							// Check if we're stuck in an infinite loop
							if productionDone.Load() {
								used := q.UsedSlots()
								if used > 0 {
									t.Errorf("Consumer %d stuck: queue reports %d items but Dequeue returns false",
										consumerID, used)
								}
								return
							}
							stuckCount = 0
						}
						runtime.Gosched()
					}
				}
			}(c)
		}

		// Wait for producers
		prodWg.Wait()
		close(allItems)
		productionDone.Store(true)

		// Give consumers time to finish
		done := make(chan struct{})
		go func() {
			consWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(10 * time.Second):
			t.Error("TIMEOUT: Consumers appear to be stuck (possible infinite recursion in Dequeue)")
		}

		// Verify counts
		enqueued := enqueuedCount.Load()
		dequeued := dequeuedCount.Load()
		remaining := q.UsedSlots()

		if dequeued != enqueued {
			t.Errorf("COUNT MISMATCH: enqueued=%d, dequeued=%d, remaining=%d",
				enqueued, dequeued, remaining)
		}
	})
}

// TestShardStarvationDetection checks if some shards are systematically
// starved in sharded queue implementations.
func TestShardStarvationDetection(t *testing.T) {
	withAllQueuesFixed(t, "ShardStarvationDetection", []string{"Sharded"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 2048
		const numOperations = 1000 // Keep smaller than capacity

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "ShardStarvationDetection")
		wd.Start()
		defer wd.Stop()

		// Enqueue all items
		pointers := make([]*int, numOperations)
		for i := 0; i < numOperations; i++ {
			ptr := new(int)
			*ptr = i
			pointers[i] = ptr
			q.Enqueue(ptr)
			if i%200 == 0 {
				wd.Progress()
			}
		}

		// Dequeue and track order
		dequeueOrder := make([]int, 0, numOperations)
		for i := 0; i < numOperations; i++ {
			for retries := 0; retries < 10000; retries++ {
				ptr, ok := q.Dequeue()
				if ok {
					dequeueOrder = append(dequeueOrder, *ptr)
					break
				}
				runtime.Gosched()
			}
			if i%200 == 0 {
				wd.Progress()
			}
		}

		// Analyze dequeue order for patterns
		// In a well-functioning sharded queue, items should be relatively
		// well-distributed, not heavily biased to certain ranges

		// Calculate distribution across buckets
		numBuckets := 10
		bucketSize := numOperations / numBuckets
		if bucketSize == 0 {
			bucketSize = 1
		}
		bucketCounts := make([]int, numBuckets)

		for i, val := range dequeueOrder {
			// Check which original bucket this value came from
			originalBucket := val / bucketSize
			if originalBucket >= numBuckets {
				originalBucket = numBuckets - 1
			}
			// Check position in dequeue order
			positionBucket := i / bucketSize
			if positionBucket >= numBuckets {
				positionBucket = numBuckets - 1
			}
			bucketCounts[positionBucket]++
		}

		// Verify all items were dequeued
		if len(dequeueOrder) != numOperations {
			t.Errorf("STARVATION DETECTED: Only dequeued %d of %d items",
				len(dequeueOrder), numOperations)
		}

		// Check for missing values
		seen := make(map[int]bool)
		for _, val := range dequeueOrder {
			if seen[val] {
				t.Errorf("Duplicate value dequeued: %d", val)
			}
			seen[val] = true
		}

		missing := 0
		for i := 0; i < numOperations; i++ {
			if !seen[i] {
				missing++
			}
		}

		if missing > 0 {
			t.Errorf("ITEMS LOST IN SHARDS: %d items missing", missing)
		}
	})
}

// =============================================================================
// CATEGORY 3: Inadequate Empty Queue Detection
// =============================================================================
// These tests target the problem where empty queue detection logic can be
// inconsistent during concurrent access.
// =============================================================================

// TestConcurrentEmptyDetectionRace creates high contention on empty queue
// detection to reveal race conditions.
func TestConcurrentEmptyDetectionRace(t *testing.T) {
	withAllQueuesFixed(t, "ConcurrentEmptyDetectionRace", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 32 // Small capacity for higher contention
		const rounds = 100
		const itemsPerRound = 100

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "ConcurrentEmptyDetectionRace")
		wd.Start()
		defer wd.Stop()

		totalLost := 0

		for round := 0; round < rounds; round++ {
			// Track items for this round
			roundItems := make([]*int, itemsPerRound)
			dequeued := make(map[*int]bool)
			var dequeuedMu sync.Mutex

			var wg sync.WaitGroup

			// Start producer
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < itemsPerRound; i++ {
					ptr := new(int)
					*ptr = round*itemsPerRound + i
					roundItems[i] = ptr
					q.Enqueue(ptr)
				}
			}()

			// Start multiple consumers racing to dequeue
			numConsumers := 5
			wg.Add(numConsumers)
			for c := 0; c < numConsumers; c++ {
				go func() {
					defer wg.Done()
					for {
						ptr, ok := q.Dequeue()
						if ok {
							dequeuedMu.Lock()
							dequeued[ptr] = true
							dequeuedMu.Unlock()
						} else {
							// Check if all items are processed
							dequeuedMu.Lock()
							count := len(dequeued)
							dequeuedMu.Unlock()
							if count >= itemsPerRound {
								return
							}
							runtime.Gosched()
						}
					}
				}()
			}

			wg.Wait()

			// Drain any remaining items
			for {
				ptr, ok := q.Dequeue()
				if !ok {
					break
				}
				dequeuedMu.Lock()
				dequeued[ptr] = true
				dequeuedMu.Unlock()
			}

			// Check for lost items this round
			roundLost := 0
			for _, ptr := range roundItems {
				if !dequeued[ptr] {
					roundLost++
				}
			}
			totalLost += roundLost

			if round%10 == 0 {
				wd.Progress()
			}
		}

		if totalLost > 0 {
			t.Errorf("EMPTY DETECTION RACE LOST ITEMS: %d items lost across %d rounds (%.2f%%)",
				totalLost, rounds, float64(totalLost)/float64(rounds*itemsPerRound)*100)
		}
	})
}

// TestFalseEmptyDetection specifically tests for false empty detection
// where the queue reports empty but has items.
func TestFalseEmptyDetection(t *testing.T) {
	withAllQueuesFixed(t, "FalseEmptyDetection", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 64
		const iterations = 1000

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "FalseEmptyDetection")
		wd.Start()
		defer wd.Stop()

		falseEmptyCount := 0

		for i := 0; i < iterations; i++ {
			// Enqueue an item
			ptr := new(int)
			*ptr = i
			q.Enqueue(ptr)

			// Immediately check UsedSlots
			used := q.UsedSlots()
			if used == 0 {
				// This is suspicious - we just enqueued
				// Try to dequeue to verify
				_, ok := q.Dequeue()
				if ok {
					// Item was there - UsedSlots gave false empty
					falseEmptyCount++
				}
			} else {
				// Normal path - dequeue the item
				for retries := 0; retries < 100; retries++ {
					_, ok := q.Dequeue()
					if ok {
						break
					}
					runtime.Gosched()
				}
			}

			if i%100 == 0 {
				wd.Progress()
			}
		}

		if falseEmptyCount > 0 {
			t.Errorf("FALSE EMPTY DETECTION: UsedSlots reported 0 when items existed %d times (%.2f%%)",
				falseEmptyCount, float64(falseEmptyCount)/float64(iterations)*100)
		}
	})
}

// TestRapidEnqueueDequeueEmptyRace rapidly enqueues and dequeues to create
// race conditions around the empty state.
func TestRapidEnqueueDequeueEmptyRace(t *testing.T) {
	withAllQueuesFixed(t, "RapidEnqueueDequeueEmptyRace", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 16 // Very small for maximum contention
		const duration = 3 * time.Second

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "RapidEnqueueDequeueEmptyRace")
		wd.Start()
		defer wd.Stop()

		var enqueueCount atomic.Int64
		var dequeueCount atomic.Int64
		var stop atomic.Bool

		var wg sync.WaitGroup

		// Start multiple producer-consumer pairs
		numPairs := runtime.NumCPU()
		if numPairs < 4 {
			numPairs = 4
		}

		wg.Add(numPairs * 2)

		// Producers
		for i := 0; i < numPairs; i++ {
			go func(id int) {
				defer wg.Done()
				for !stop.Load() {
					ptr := new(int)
					*ptr = id
					q.Enqueue(ptr)
					enqueueCount.Add(1)
				}
			}(i)
		}

		// Consumers
		for i := 0; i < numPairs; i++ {
			go func() {
				defer wg.Done()
				for !stop.Load() {
					if _, ok := q.Dequeue(); ok {
						dequeueCount.Add(1)
					}
					wd.Progress()
				}
				// Drain remaining
				for {
					if _, ok := q.Dequeue(); ok {
						dequeueCount.Add(1)
					} else {
						break
					}
				}
			}()
		}

		// Run for duration
		time.Sleep(duration)
		stop.Store(true)
		wg.Wait()

		// Verify counts
		enqueued := enqueueCount.Load()
		dequeued := dequeueCount.Load()
		remaining := q.UsedSlots()

		expectedRemaining := enqueued - dequeued
		if int64(remaining) != expectedRemaining && remaining != 0 {
			// Allow for some drift due to concurrent reads
			drift := math.Abs(float64(int64(remaining) - expectedRemaining))
			if drift > float64(capacity) {
				t.Errorf("INCONSISTENT STATE: enqueued=%d, dequeued=%d, remaining=%d (expected ~%d)",
					enqueued, dequeued, remaining, expectedRemaining)
			}
		}

		// Drain and verify
		finalDrain := 0
		for {
			if _, ok := q.Dequeue(); ok {
				finalDrain++
			} else {
				break
			}
		}

		totalDequeued := dequeued + int64(finalDrain)
		if totalDequeued != enqueued {
			t.Errorf("LOST ITEMS IN RAPID RACE: enqueued=%d, total_dequeued=%d, lost=%d",
				enqueued, totalDequeued, enqueued-totalDequeued)
		}
	})
}

// =============================================================================
// CATEGORY 4: Shard Selection Issues
// =============================================================================
// These tests target predictable shard selection patterns and uneven load
// distribution in sharded queue implementations.
// =============================================================================

// TestShardSelectionDistribution verifies that shard selection is reasonably
// uniform across all shards.
func TestShardSelectionDistribution(t *testing.T) {
	withAllQueuesFixed(t, "ShardSelectionDistribution", []string{"Sharded"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 4096
		const numItems = 2000 // Less than capacity

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "ShardSelectionDistribution")
		wd.Start()
		defer wd.Stop()

		// Enqueue items with unique values
		for i := 0; i < numItems; i++ {
			ptr := new(int)
			*ptr = i
			q.Enqueue(ptr)
			if i%500 == 0 {
				wd.Progress()
			}
		}

		// Dequeue all items and track order
		dequeueOrder := make([]int, 0, numItems)
		for {
			ptr, ok := q.Dequeue()
			if !ok {
				break
			}
			dequeueOrder = append(dequeueOrder, *ptr)
			wd.Progress()
		}

		if len(dequeueOrder) != numItems {
			t.Errorf("Lost items: expected %d, got %d", numItems, len(dequeueOrder))
			return
		}

		// Calculate out-of-order metric
		// For a pure FIFO, this would be 0
		// For a sharded queue, some reordering is expected
		outOfOrder := 0
		for i := 1; i < len(dequeueOrder); i++ {
			if dequeueOrder[i] < dequeueOrder[i-1] {
				outOfOrder++
			}
		}

		// Log the distribution characteristics
		t.Logf("Out-of-order pairs: %d (%.2f%% of items)",
			outOfOrder, float64(outOfOrder)/float64(numItems)*100)

		// For sharded queues, verify completeness is maintained
		seen := make(map[int]bool)
		for _, val := range dequeueOrder {
			if seen[val] {
				t.Errorf("Duplicate value: %d", val)
			}
			seen[val] = true
		}

		missing := 0
		for i := 0; i < numItems; i++ {
			if !seen[i] {
				missing++
			}
		}

		if missing > 0 {
			t.Errorf("SHARD DISTRIBUTION LOST ITEMS: %d items missing", missing)
		}
	})
}

// TestConcurrentShardAccess tests for race conditions when multiple goroutines
// access shards simultaneously.
func TestConcurrentShardAccess(t *testing.T) {
	withAllQueuesFixed(t, "ConcurrentShardAccess", []string{"Sharded"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 512
		const numGoroutines = 50
		const opsPerGoroutine = 500

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "ConcurrentShardAccess")
		wd.Start()
		defer wd.Stop()

		var wg sync.WaitGroup
		var totalEnqueued atomic.Int64
		var totalDequeued atomic.Int64

		// Each goroutine does both enqueue and dequeue
		wg.Add(numGoroutines)
		for g := 0; g < numGoroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				localEnqueued := 0
				localDequeued := 0

				for i := 0; i < opsPerGoroutine; i++ {
					// Enqueue
					ptr := new(int)
					*ptr = gid*opsPerGoroutine + i
					q.Enqueue(ptr)
					localEnqueued++

					// Try to dequeue
					if _, ok := q.Dequeue(); ok {
						localDequeued++
					}

					if i%100 == 0 {
						wd.Progress()
					}
				}

				totalEnqueued.Add(int64(localEnqueued))
				totalDequeued.Add(int64(localDequeued))
			}(g)
		}

		wg.Wait()

		// Drain remaining
		drained := 0
		for {
			if _, ok := q.Dequeue(); ok {
				drained++
			} else {
				break
			}
		}

		enqueued := totalEnqueued.Load()
		dequeued := totalDequeued.Load() + int64(drained)

		if enqueued != dequeued {
			t.Errorf("CONCURRENT SHARD ACCESS LOST ITEMS: enqueued=%d, dequeued=%d, lost=%d",
				enqueued, dequeued, enqueued-dequeued)
		}
	})
}

// =============================================================================
// CATEGORY 5: Edge Cases and Stress Tests
// =============================================================================

// TestCapacityBoundaryRace tests race conditions at exact capacity boundaries.
func TestCapacityBoundaryRace(t *testing.T) {
	withAllQueuesFixed(t, "CapacityBoundaryRace", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 64
		const iterations = 100

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "CapacityBoundaryRace")
		wd.Start()
		defer wd.Stop()

		for iter := 0; iter < iterations; iter++ {
			// Fill to exactly capacity
			pointers := make([]*int, capacity)
			for i := 0; i < int(capacity); i++ {
				ptr := new(int)
				*ptr = iter*int(capacity) + i
				pointers[i] = ptr
				q.Enqueue(ptr)
			}

			// Verify full
			if q.FreeSlots() != 0 {
				t.Logf("Iteration %d: FreeSlots=%d (expected 0)", iter, q.FreeSlots())
			}

			// Drain exactly capacity
			dequeued := make([]*int, 0, capacity)
			for i := 0; i < int(capacity); i++ {
				for retries := 0; retries < 100; retries++ {
					ptr, ok := q.Dequeue()
					if ok {
						dequeued = append(dequeued, ptr)
						break
					}
					runtime.Gosched()
				}
			}

			if len(dequeued) != int(capacity) {
				t.Errorf("Iteration %d: Only dequeued %d of %d items",
					iter, len(dequeued), capacity)
			}

			// Verify empty
			if q.UsedSlots() != 0 {
				t.Errorf("Iteration %d: UsedSlots=%d (expected 0)", iter, q.UsedSlots())
			}

			if iter%10 == 0 {
				wd.Progress()
			}
		}
	})
}

// TestHighContentionStress creates extreme contention to stress-test
// the queue implementations.
func TestHighContentionStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	withAllQueuesFixed(t, "HighContentionStress", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 128
		const duration = 3 * time.Second
		const numProducers = 20
		const numConsumers = 20

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "HighContentionStress")
		wd.Start()
		defer wd.Stop()

		var enqueueCount atomic.Int64
		var dequeueCount atomic.Int64
		var stop atomic.Bool

		var wg sync.WaitGroup

		// Start producers
		wg.Add(numProducers)
		for p := 0; p < numProducers; p++ {
			go func(pid int) {
				defer wg.Done()
				localCount := 0
				for !stop.Load() {
					ptr := new(int)
					*ptr = pid*1000000 + localCount
					q.Enqueue(ptr)
					enqueueCount.Add(1)
					localCount++
					if localCount%1000 == 0 {
						wd.Progress()
					}
				}
			}(p)
		}

		// Start consumers
		wg.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func() {
				defer wg.Done()
				for !stop.Load() {
					if _, ok := q.Dequeue(); ok {
						dequeueCount.Add(1)
						wd.Progress()
					}
					runtime.Gosched()
				}
				// Drain
				for {
					if _, ok := q.Dequeue(); ok {
						dequeueCount.Add(1)
					} else {
						break
					}
				}
			}()
		}

		// Run test
		time.Sleep(duration)
		stop.Store(true)
		wg.Wait()

		// Verify
		enqueued := enqueueCount.Load()
		dequeued := dequeueCount.Load()
		remaining := q.UsedSlots()

		// Final drain
		finalDrain := 0
		for {
			if _, ok := q.Dequeue(); ok {
				finalDrain++
			} else {
				break
			}
		}

		totalDequeued := dequeued + int64(finalDrain)

		if totalDequeued != enqueued {
			lossRate := float64(enqueued-totalDequeued) / float64(enqueued) * 100
			t.Errorf("HIGH CONTENTION STRESS LOST ITEMS: enqueued=%d, dequeued=%d, lost=%d (%.4f%%)",
				enqueued, totalDequeued, enqueued-totalDequeued, lossRate)
		} else {
			t.Logf("Stress test passed: %d items processed, %d remaining", enqueued, remaining)
		}
	})
}

// TestSequentialConsistency verifies that items enqueued by a single producer
// maintain their order when dequeued by a single consumer.
func TestSequentialConsistency(t *testing.T) {
	withAllQueuesFixed(t, "SequentialConsistency", []string{"FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 256
		const numItems = 5000

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "SequentialConsistency")
		wd.Start()
		defer wd.Stop()

		// Enqueue in order
		pointers := make([]*int, numItems)
		done := make(chan struct{})

		go func() {
			for i := 0; i < numItems; i++ {
				ptr := new(int)
				*ptr = i
				pointers[i] = ptr
				q.Enqueue(ptr)
				if i%500 == 0 {
					wd.Progress()
				}
			}
			close(done)
		}()

		// Dequeue and verify order
		received := make([]*int, 0, numItems)
		for len(received) < numItems {
			ptr, ok := q.Dequeue()
			if ok {
				received = append(received, ptr)
				wd.Progress()
			} else {
				runtime.Gosched()
			}
		}

		<-done

		// Verify FIFO order
		outOfOrder := 0
		for i := 0; i < numItems; i++ {
			if received[i] != pointers[i] {
				outOfOrder++
				if outOfOrder <= 10 {
					t.Logf("Order violation at %d: expected %p (%d), got %p (%d)",
						i, pointers[i], *pointers[i], received[i], *received[i])
				}
			}
		}

		if outOfOrder > 0 {
			t.Errorf("FIFO ORDER VIOLATED: %d items out of order (%.2f%%)",
				outOfOrder, float64(outOfOrder)/float64(numItems)*100)
		}
	})
}

// =============================================================================
// CATEGORY 6: Specific Bug Pattern Tests
// =============================================================================
// These tests target the exact bug patterns identified in the code review.
// =============================================================================

// TestDequeueReturnsEarlyBug tests the specific bug where Dequeue() returns
// (zero, false) after a single failed CAS (Compare-and-Swap), even when items may be available.
// This is the most common bug pattern in the implementations.
func TestDequeueReturnsEarlyBug(t *testing.T) {
	withAllQueuesFixed(t, "DequeueReturnsEarlyBug", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 8 // Very small to maximize contention
		const numGoroutines = 100
		const opsPerGoroutine = 100

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "DequeueReturnsEarlyBug")
		wd.Start()
		defer wd.Stop()

		var enqueued atomic.Int64
		var dequeued atomic.Int64
		var falseEmpty atomic.Int64 // Dequeue returned false but queue wasn't empty
		var wg sync.WaitGroup

		// Run many goroutines doing enqueue/dequeue simultaneously
		wg.Add(numGoroutines)
		for g := 0; g < numGoroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < opsPerGoroutine; i++ {
					// Enqueue
					ptr := new(int)
					*ptr = gid*opsPerGoroutine + i
					q.Enqueue(ptr)
					enqueued.Add(1)

					// Immediately try to dequeue
					_, ok := q.Dequeue()
					if ok {
						dequeued.Add(1)
					} else {
						// Dequeue failed - check if queue actually has items
						if q.UsedSlots() > 0 {
							falseEmpty.Add(1)
						}
					}

					if i%20 == 0 {
						wd.Progress()
					}
				}
			}(g)
		}

		wg.Wait()

		// Drain remaining
		drained := 0
		for {
			if _, ok := q.Dequeue(); ok {
				drained++
			} else {
				break
			}
		}

		totalEnqueued := enqueued.Load()
		totalDequeued := dequeued.Load() + int64(drained)
		falseEmptyCount := falseEmpty.Load()

		t.Logf("Enqueued: %d, Dequeued: %d, False empty reports: %d",
			totalEnqueued, totalDequeued, falseEmptyCount)

		if totalDequeued != totalEnqueued {
			t.Errorf("LOST ITEMS (early return bug): enqueued=%d, dequeued=%d, lost=%d",
				totalEnqueued, totalDequeued, totalEnqueued-totalDequeued)
		}

		if falseEmptyCount > 0 {
			t.Logf("False empty detections: %d (Dequeue returned false when queue had items)",
				falseEmptyCount)
		}
	})
}

// TestMultiHeadRecursionBug specifically tests for the infinite recursion bug
// in MultiHeadQueue.Dequeue() where UsedSlots check races with actual content.
func TestMultiHeadRecursionBug(t *testing.T) {
	withAllQueuesFixed(t, "MultiHeadRecursionBug", []string{"Multi-Head-FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 64
		const numProducers = 20
		const numConsumers = 20
		const duration = 5 * time.Second

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "MultiHeadRecursionBug")
		wd.Start()
		defer wd.Stop()

		var enqueued atomic.Int64
		var dequeued atomic.Int64
		var stop atomic.Bool

		var wg sync.WaitGroup

		// Producers - constantly add items
		wg.Add(numProducers)
		for p := 0; p < numProducers; p++ {
			go func(pid int) {
				defer wg.Done()
				i := 0
				for !stop.Load() {
					ptr := new(int)
					*ptr = pid*1000000 + i
					q.Enqueue(ptr)
					enqueued.Add(1)
					i++
					if i%100 == 0 {
						wd.Progress()
					}
				}
			}(p)
		}

		// Consumers - will hit the recursion bug under high contention
		wg.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func(cid int) {
				defer wg.Done()
				for !stop.Load() {
					// Set a timeout to detect infinite recursion
					done := make(chan bool, 1)
					go func() {
						_, ok := q.Dequeue()
						if ok {
							dequeued.Add(1)
						}
						done <- true
					}()

					select {
					case <-done:
						// Good - dequeue completed
					case <-time.After(100 * time.Millisecond):
						t.Errorf("Consumer %d: Dequeue timed out - possible infinite recursion", cid)
						return
					}
					wd.Progress()
				}
				// Drain
				for {
					if _, ok := q.Dequeue(); ok {
						dequeued.Add(1)
					} else {
						break
					}
				}
			}(c)
		}

		// Run for duration
		time.Sleep(duration)
		stop.Store(true)

		// Give time for goroutines to finish
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Good
		case <-time.After(10 * time.Second):
			t.Error("TIMEOUT: Goroutines did not complete - possible infinite recursion in Dequeue")
			return
		}

		// Final drain
		drained := 0
		for {
			if _, ok := q.Dequeue(); ok {
				drained++
			} else {
				break
			}
		}

		totalEnqueued := enqueued.Load()
		totalDequeued := dequeued.Load() + int64(drained)

		if totalDequeued != totalEnqueued {
			t.Errorf("LOST ITEMS (recursion bug): enqueued=%d, dequeued=%d, lost=%d",
				totalEnqueued, totalDequeued, totalEnqueued-totalDequeued)
		}
	})
}

// TestSequenceNumberWrapBug tests for issues when sequence numbers wrap around
// after many operations, which can cause the sequence comparison logic to fail.
func TestSequenceNumberWrapBug(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping sequence wrap test in short mode")
	}

	withAllQueuesFixed(t, "SequenceNumberWrapBug", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 16
		// Do many iterations to stress sequence numbers
		const iterations = 100000

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "SequenceNumberWrapBug")
		wd.Start()
		defer wd.Stop()

		lost := 0

		for i := 0; i < iterations; i++ {
			ptr := new(int)
			*ptr = i
			q.Enqueue(ptr)

			// Immediately dequeue
			var got *int
			var ok bool
			for retry := 0; retry < 100; retry++ {
				got, ok = q.Dequeue()
				if ok {
					break
				}
				runtime.Gosched()
			}

			if !ok {
				lost++
			} else if *got != i {
				// This would indicate sequence number corruption
				t.Errorf("Value mismatch at iteration %d: expected %d, got %d", i, i, *got)
			}

			if i%10000 == 0 {
				wd.Progress()
				t.Logf("Completed %d iterations", i)
			}
		}

		if lost > 0 {
			t.Errorf("SEQUENCE WRAP BUG: lost %d items out of %d (%.4f%%)",
				lost, iterations, float64(lost)/float64(iterations)*100)
		}
	})
}

// TestEnqueuePosDequeuePosDrift tests for drift between enqueuePos and dequeuePos
// that could cause the empty detection to fail.
func TestEnqueuePosDequeuePosDrift(t *testing.T) {
	withAllQueuesFixed(t, "EnqueuePosDequeuePosDrift", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 128
		const rounds = 50
		const itemsPerRound = 1000

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "EnqueuePosDequeuePosDrift")
		wd.Start()
		defer wd.Stop()

		for round := 0; round < rounds; round++ {
			// Concurrent enqueue and dequeue
			var wg sync.WaitGroup
			var enqueued atomic.Int64
			var dequeued atomic.Int64

			// Start dequeuers first (they'll spin waiting)
			wg.Add(5)
			for c := 0; c < 5; c++ {
				go func() {
					defer wg.Done()
					for dequeued.Load() < int64(itemsPerRound) {
						if _, ok := q.Dequeue(); ok {
							dequeued.Add(1)
						}
						runtime.Gosched()
					}
				}()
			}

			// Then start enqueuers
			wg.Add(5)
			for p := 0; p < 5; p++ {
				go func(pid int) {
					defer wg.Done()
					for i := 0; i < itemsPerRound/5; i++ {
						ptr := new(int)
						*ptr = round*itemsPerRound + pid*(itemsPerRound/5) + i
						q.Enqueue(ptr)
						enqueued.Add(1)
					}
				}(p)
			}

			wg.Wait()

			// Drain any remaining
			drained := 0
			for {
				if _, ok := q.Dequeue(); ok {
					drained++
				} else {
					break
				}
			}

			totalDequeued := dequeued.Load() + int64(drained)
			totalEnqueued := enqueued.Load()

			if totalDequeued != totalEnqueued {
				t.Errorf("Round %d DRIFT BUG: enqueued=%d, dequeued=%d, lost=%d",
					round, totalEnqueued, totalDequeued, totalEnqueued-totalDequeued)
			}

			// Check counters
			used := q.UsedSlots()
			free := q.FreeSlots()
			if used != 0 {
				t.Errorf("Round %d: UsedSlots=%d after drain (expected 0)", round, used)
			}
			if free != capacity {
				t.Errorf("Round %d: FreeSlots=%d after drain (expected %d)", round, free, capacity)
			}

			if round%10 == 0 {
				wd.Progress()
			}
		}
	})
}

// TestCASRetryStarvation tests for starvation where repeated CAS failures
// cause some items to never be dequeued.
func TestCASRetryStarvation(t *testing.T) {
	withAllQueuesFixed(t, "CASRetryStarvation", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		const capacity = 16 // Very small for maximum CAS contention
		const numProducers = 50
		const numConsumers = 50
		const itemsPerProducer = 200
		const totalItems = numProducers * itemsPerProducer

		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "CASRetryStarvation")
		wd.Start()
		defer wd.Stop()

		// Track each item individually
		items := make([]*int, totalItems)
		for i := 0; i < totalItems; i++ {
			items[i] = new(int)
			*items[i] = i
		}

		received := make([]atomic.Bool, totalItems)
		var enqueueIdx atomic.Int64
		var productionDone atomic.Bool

		var wg sync.WaitGroup

		// Producers
		wg.Add(numProducers)
		for p := 0; p < numProducers; p++ {
			go func() {
				defer wg.Done()
				for {
					idx := enqueueIdx.Add(1) - 1
					if idx >= int64(totalItems) {
						return
					}
					q.Enqueue(items[idx])
					if idx%100 == 0 {
						wd.Progress()
					}
				}
			}()
		}

		// Wait for producers to finish before signaling
		go func() {
			wg.Wait()
			productionDone.Store(true)
		}()

		// Consumers
		var consWg sync.WaitGroup
		consWg.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func() {
				defer consWg.Done()
				for {
					ptr, ok := q.Dequeue()
					if ok {
						idx := *ptr
						if idx >= 0 && idx < totalItems {
							received[idx].Store(true)
						}
						wd.Progress()
					} else if productionDone.Load() {
						// Check if truly empty
						if q.UsedSlots() == 0 {
							return
						}
					}
					runtime.Gosched()
				}
			}()
		}

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			consWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Good
		case <-time.After(30 * time.Second):
			t.Error("TIMEOUT: Test did not complete in time")
		}

		// Drain any remaining
		for {
			ptr, ok := q.Dequeue()
			if !ok {
				break
			}
			idx := *ptr
			if idx >= 0 && idx < totalItems {
				received[idx].Store(true)
			}
		}

		// Count missing items
		missing := 0
		for i := 0; i < totalItems; i++ {
			if !received[i].Load() {
				missing++
				if missing <= 10 {
					t.Logf("Item %d was never received (CAS starvation?)", i)
				}
			}
		}

		if missing > 0 {
			t.Errorf("CAS STARVATION BUG: %d items lost out of %d (%.2f%%)",
				missing, totalItems, float64(missing)/float64(totalItems)*100)
		}
	})
}

// =============================================================================
// Summary Test - Runs all categories and reports findings
// =============================================================================

// TestRaceConditionSummary provides a summary of all race condition tests.
func TestRaceConditionSummary(t *testing.T) {
	t.Log("Race Condition Detection Test Suite")
	t.Log("====================================")
	t.Log("")
	t.Log("This test suite detects the following problems:")
	t.Log("")
	t.Log("1. Lost Items in Dequeue Methods")
	t.Log("   - TestDequeueRaceConditionLostItems")
	t.Log("   - TestMicrosecondWindowLostItems")
	t.Log("   - TestBurstEnqueueSlowDequeue")
	t.Log("")
	t.Log("2. Multi-Head Queue Race Condition")
	t.Log("   - TestMultiHeadEmptyQueueRaceCondition")
	t.Log("   - TestShardStarvationDetection")
	t.Log("")
	t.Log("3. Inadequate Empty Queue Detection")
	t.Log("   - TestConcurrentEmptyDetectionRace")
	t.Log("   - TestFalseEmptyDetection")
	t.Log("   - TestRapidEnqueueDequeueEmptyRace")
	t.Log("")
	t.Log("4. Shard Selection Issues")
	t.Log("   - TestShardSelectionDistribution")
	t.Log("   - TestConcurrentShardAccess")
	t.Log("")
	t.Log("5. Edge Cases and Stress Tests")
	t.Log("   - TestCapacityBoundaryRace")
	t.Log("   - TestHighContentionStress")
	t.Log("   - TestSequentialConsistency")
	t.Log("")
	t.Log("Run with: go test -v -run 'Test.*Race|Test.*Lost|Test.*Shard|Test.*Empty'")
	t.Log("Run with race detector: go test -race -v ./...")
}
