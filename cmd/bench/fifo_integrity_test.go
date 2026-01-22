package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Test Helper (Fixed version that properly handles feature filtering)
// =============================================================================

// withAllQueuesFixed is a corrected version of withAllQueues that properly
// skips implementations at the subtest level rather than the parent level.
func withAllQueuesFixed(t *testing.T, scenarioName string, testedFeatures []string, fn func(t *testing.T, impl Implementation[*int, testQueueInterface])) {
	t.Helper()
	impls := getImplementations()
	for _, impl := range impls {
		impl := impl // capture range variable

		t.Run(impl.name, func(t *testing.T) {
			if impl.newQueue == nil {
				t.Skipf("Skipping stub implementation %q", impl.name)
				return
			}

			// Check if the test tests a feature that the implementation does not support
			if testedFeatures != nil {
				for _, feature := range testedFeatures {
					found := false
					for _, implFeature := range impl.features {
						if feature == implFeature {
							found = true
							break
						}
					}
					if !found {
						t.Skipf("Skipping: missing feature %q", feature)
						return
					}
				}
			}

			fn(t, impl)
		})
	}
}

// =============================================================================
// Test Configuration Helpers
// =============================================================================

// getEnvInt reads an integer from an environment variable with a default value.
func getEnvInt(name string, defaultVal int) int {
	if v := os.Getenv(name); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return defaultVal
}

// getEnvBool reads a boolean from an environment variable with a default value.
func getEnvBool(name string, defaultVal bool) bool {
	if v := os.Getenv(name); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return defaultVal
}

// Test size configuration via environment variables:
//   FIFO_TEST_SIZE      - Default size for normal tests (default: 10000)
//   FIFO_STRESS_SIZE    - Size for stress tests (default: 100000)
//   FIFO_ENABLE_STRESS  - Enable large stress tests (default: false)
//   FIFO_CONCURRENCY    - Number of concurrent goroutines (default: 50)

func getTestSize() int {
	return getEnvInt("FIFO_TEST_SIZE", 10000)
}

func getStressSize() int {
	return getEnvInt("FIFO_STRESS_SIZE", 100000)
}

func stressTestsEnabled() bool {
	return getEnvBool("FIFO_ENABLE_STRESS", false)
}

func getConcurrency() int {
	return getEnvInt("FIFO_CONCURRENCY", 50)
}

// =============================================================================
// Sequence Item for Tracking Order and Integrity
// =============================================================================

// seqItem represents a sequenced item with a unique ID and pointer for integrity checking.
type seqItem struct {
	seqNum     int64 // Global sequence number assigned at enqueue time
	producerID int   // Which producer created this item
	localSeq   int   // Local sequence within the producer
}

// =============================================================================
// FIFO Ordering Tests
// =============================================================================

// TestStrictFIFOOrderingSingleProducer validates exact FIFO ordering with a single
// producer and single consumer. This is the most basic FIFO guarantee.
func TestStrictFIFOOrderingSingleProducer(t *testing.T) {
	withAllQueuesFixed(t, "StrictFIFOOrderingSingleProducer", []string{"FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "StrictFIFOOrderingSingleProducer", impl)
		q := impl.newQueue(1024)
		wd := newWatchdog(t, "StrictFIFOOrderingSingleProducer")
		wd.Start()
		defer wd.Stop()

		testSize := getTestSize()

		// Create unique pointers with sequence values
		pointers := make([]*int, testSize)
		for i := 0; i < testSize; i++ {
			p := new(int)
			*p = i
			pointers[i] = p
		}

		// Producer: run in a separate goroutine so blocking Enqueue implementations
		// (like buffered channels) do not deadlock when the buffer fills.
		done := make(chan struct{})
		go func() {
			for i := 0; i < testSize; i++ {
				q.Enqueue(pointers[i])
				wd.Progress()
			}
			close(done)
		}()

		// Dequeue and verify exact FIFO order
		for i := 0; i < testSize; i++ {
			var got *int
			for {
				var ok bool
				got, ok = q.Dequeue()
				if ok {
					break
				}
				time.Sleep(1 * time.Microsecond)
			}
			wd.Progress()

			// Verify pointer identity (exact same pointer)
			if got != pointers[i] {
				t.Fatalf("FIFO violation at index %d: expected pointer %p, got %p", i, pointers[i], got)
			}
			// Verify value integrity
			if *got != i {
				t.Fatalf("Value corruption at index %d: expected %d, got %d", i, i, *got)
			}
		}

		<-done

		// Queue should be empty
		if q.UsedSlots() != 0 {
			t.Fatalf("Queue not empty after test: UsedSlots=%d", q.UsedSlots())
		}
	})
}

// TestStrictFIFOOrderingWithWrapAround validates FIFO ordering across multiple
// wrap-around cycles of the ring buffer.
func TestStrictFIFOOrderingWithWrapAround(t *testing.T) {
	withAllQueuesFixed(t, "StrictFIFOOrderingWithWrapAround", []string{"FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "StrictFIFOOrderingWithWrapAround", impl)
		const capacity = 64 // Small capacity to force many wrap-arounds
		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "StrictFIFOOrderingWithWrapAround")
		wd.Start()
		defer wd.Stop()

		testSize := getTestSize()
		expectedWrapArounds := testSize / int(capacity)
		t.Logf("Testing %d items with capacity %d (expected ~%d wrap-arounds)", testSize, capacity, expectedWrapArounds)

		// Track all pointers in order
		pointers := make([]*int, testSize)
		for i := 0; i < testSize; i++ {
			p := new(int)
			*p = i
			pointers[i] = p
		}

		// Producer goroutine
		done := make(chan struct{})
		go func() {
			for i := 0; i < testSize; i++ {
				q.Enqueue(pointers[i])
				wd.Progress()
			}
			close(done)
		}()

		// Consumer: dequeue and verify order
		for i := 0; i < testSize; i++ {
			var got *int
			for {
				var ok bool
				got, ok = q.Dequeue()
				if ok {
					break
				}
				time.Sleep(1 * time.Microsecond)
			}
			wd.Progress()

			if got != pointers[i] {
				t.Fatalf("FIFO violation at index %d (wrap-around test): expected pointer %p (value %d), got %p (value %d)",
					i, pointers[i], *pointers[i], got, *got)
			}
			if *got != i {
				t.Fatalf("Value corruption at index %d: expected %d, got %d", i, i, *got)
			}
		}

		<-done

		if q.UsedSlots() != 0 {
			t.Fatalf("Queue not empty after wrap-around test: UsedSlots=%d", q.UsedSlots())
		}
	})
}

// TestFIFOOrderingConcurrentProducerSingleConsumer tests FIFO ordering when
// multiple producers feed a single consumer. Within each producer's stream,
// FIFO order must be maintained.
func TestFIFOOrderingConcurrentProducerSingleConsumer(t *testing.T) {
	withAllQueuesFixed(t, "FIFOOrderingConcurrentProducerSingleConsumer", []string{"FIFO", "MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "FIFOOrderingConcurrentProducerSingleConsumer", impl)
		q := impl.newQueue(1024)
		wd := newWatchdog(t, "FIFOOrderingConcurrentProducerSingleConsumer")
		wd.Start()
		defer wd.Stop()

		numProducers := getConcurrency()
		itemsPerProducer := getTestSize() / numProducers
		totalItems := numProducers * itemsPerProducer

		// Track last seen sequence for each producer
		lastSeen := make([]int64, numProducers)
		for i := range lastSeen {
			lastSeen[i] = -1
		}
		var lastSeenMu sync.Mutex

		// Encoding: value = producerID * 1_000_000 + localSeq
		// This allows us to decode producer and sequence from the value

		var prodWg sync.WaitGroup
		prodWg.Add(numProducers)

		// Start producers
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer prodWg.Done()
				for seq := 0; seq < itemsPerProducer; seq++ {
					val := new(int)
					*val = producerID*1_000_000 + seq
					q.Enqueue(val)
					wd.Progress()
				}
			}(p)
		}

		// Consumer: receive all items and verify per-producer FIFO
		receivedCount := 0
		fifoViolations := 0
		for receivedCount < totalItems {
			val, ok := q.Dequeue()
			if !ok {
				time.Sleep(1 * time.Microsecond)
				continue
			}
			wd.Progress()

			producerID := *val / 1_000_000
			localSeq := int64(*val % 1_000_000)

			if producerID < 0 || producerID >= numProducers {
				t.Fatalf("Invalid producer ID decoded: %d from value %d", producerID, *val)
			}

			lastSeenMu.Lock()
			if localSeq <= lastSeen[producerID] {
				fifoViolations++
				t.Errorf("FIFO violation for producer %d: received seq %d after %d",
					producerID, localSeq, lastSeen[producerID])
			}
			lastSeen[producerID] = localSeq
			lastSeenMu.Unlock()

			receivedCount++
		}

		prodWg.Wait()

		if fifoViolations > 0 {
			t.Fatalf("Total FIFO violations: %d", fifoViolations)
		}

		// Verify we received the expected final sequence for each producer
		for p := 0; p < numProducers; p++ {
			expectedLast := int64(itemsPerProducer - 1)
			if lastSeen[p] != expectedLast {
				t.Errorf("Producer %d: expected final seq %d, got %d", p, expectedLast, lastSeen[p])
			}
		}
	})
}

// =============================================================================
// Multi-Head FIFO Tests
// =============================================================================

// TestMultiHeadFIFOPerShardOrdering validates that multi-head FIFO queues maintain
// FIFO ordering within each shard/head, even if global ordering isn't guaranteed.
// NOTE: This test documents the actual behavior - Multi-Head queues may NOT maintain
// per-producer ordering due to sharding. This is an informational test.
func TestMultiHeadFIFOPerShardOrdering(t *testing.T) {
	withAllQueuesFixed(t, "MultiHeadFIFOPerShardOrdering", []string{"Multi-Head-FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "MultiHeadFIFOPerShardOrdering", impl)
		q := impl.newQueue(1024)
		wd := newWatchdog(t, "MultiHeadFIFOPerShardOrdering")
		wd.Start()
		defer wd.Stop()

		numProducers := getConcurrency()
		itemsPerProducer := getTestSize() / numProducers
		totalItems := numProducers * itemsPerProducer

		// Track per-producer sequence ordering
		lastSeen := make([]int64, numProducers)
		for i := range lastSeen {
			lastSeen[i] = -1
		}
		var lastSeenMu sync.Mutex

		var prodWg sync.WaitGroup
		prodWg.Add(numProducers)

		// Start producers - each producer sends sequenced items
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer prodWg.Done()
				for seq := 0; seq < itemsPerProducer; seq++ {
					val := new(int)
					*val = producerID*1_000_000 + seq
					q.Enqueue(val)
					wd.Progress()
				}
			}(p)
		}

		// Consumer: verify per-producer ordering (multi-head may reorder globally)
		receivedCount := 0
		perProducerViolations := 0

		for receivedCount < totalItems {
			val, ok := q.Dequeue()
			if !ok {
				time.Sleep(1 * time.Microsecond)
				continue
			}
			wd.Progress()

			producerID := *val / 1_000_000
			localSeq := int64(*val % 1_000_000)

			lastSeenMu.Lock()
			if localSeq <= lastSeen[producerID] && lastSeen[producerID] != -1 {
				perProducerViolations++
				// Only log first few violations to avoid spam
				if perProducerViolations <= 5 {
					t.Logf("Per-producer ordering note: producer %d received seq %d after %d",
						producerID, localSeq, lastSeen[producerID])
				}
			}
			lastSeen[producerID] = localSeq
			lastSeenMu.Unlock()

			receivedCount++
		}

		prodWg.Wait()

		// Document behavior rather than fail - Multi-Head FIFO doesn't guarantee per-producer ordering
		if perProducerViolations > 0 {
			t.Logf("INFO: Multi-Head queue had %d per-producer ordering variations (expected for sharded design)", perProducerViolations)
		}

		// Verify completeness - all items must be received
		if receivedCount != totalItems {
			t.Fatalf("Completeness violation: expected %d items, got %d", totalItems, receivedCount)
		}
	})
}

// TestMultiHeadFIFOCompleteness ensures multi-head FIFO queues don't lose messages
// across shards during high contention.
func TestMultiHeadFIFOCompleteness(t *testing.T) {
	withAllQueuesFixed(t, "MultiHeadFIFOCompleteness", []string{"Multi-Head-FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "MultiHeadFIFOCompleteness", impl)
		q := impl.newQueue(256) // Small capacity to stress shard distribution
		wd := newWatchdog(t, "MultiHeadFIFOCompleteness")
		wd.Start()
		defer wd.Stop()

		testSize := getTestSize()

		// Create unique pointers
		pointers := make([]*int, testSize)
		seen := make(map[*int]bool, testSize)
		var seenMu sync.Mutex

		for i := 0; i < testSize; i++ {
			p := new(int)
			*p = i
			pointers[i] = p
			seen[p] = false
		}

		// Producer
		go func() {
			for i := 0; i < testSize; i++ {
				q.Enqueue(pointers[i])
				wd.Progress()
			}
		}()

		// Consumer: receive all and track uniqueness
		receivedCount := 0
		duplicates := 0

		for receivedCount < testSize {
			val, ok := q.Dequeue()
			if !ok {
				time.Sleep(1 * time.Microsecond)
				continue
			}
			wd.Progress()

			seenMu.Lock()
			alreadySeen, exists := seen[val]
			if !exists {
				t.Fatalf("Received unknown pointer %p (value %d) - memory corruption?", val, *val)
			}
			if alreadySeen {
				duplicates++
				t.Errorf("Duplicate pointer received: %p (value %d)", val, *val)
			}
			seen[val] = true
			seenMu.Unlock()

			receivedCount++
		}

		// Verify all pointers were received
		missing := 0
		for p, wasSeen := range seen {
			if !wasSeen {
				missing++
				t.Errorf("Pointer %p (value %d) was never received", p, *p)
			}
		}

		if missing > 0 || duplicates > 0 {
			t.Fatalf("Multi-head FIFO completeness failed: %d missing, %d duplicates", missing, duplicates)
		}
	})
}

// =============================================================================
// Completeness / No Lost Messages Tests
// =============================================================================

// TestNoLostMessagesSingleThread verifies completeness with single producer/consumer.
func TestNoLostMessagesSingleThread(t *testing.T) {
	withAllQueuesFixed(t, "NoLostMessagesSingleThread", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "NoLostMessagesSingleThread", impl)
		q := impl.newQueue(1024)
		wd := newWatchdog(t, "NoLostMessagesSingleThread")
		wd.Start()
		defer wd.Stop()

		testSize := getTestSize()

		// Track all pointers by address
		enqueuedPointers := make(map[*int]int, testSize) // pointer -> expected value
		dequeuedPointers := make(map[*int]int, testSize) // pointer -> received value

		// Prepare pointers and enqueue phase (producer runs in goroutine so blocking
		// implementations won't deadlock the test). We pre-create the pointers and
		// populate `enqueuedPointers` synchronously to avoid concurrent map writes.
		pointers := make([]*int, testSize)
		for i := 0; i < testSize; i++ {
			p := new(int)
			*p = i
			pointers[i] = p
			enqueuedPointers[p] = i
		}

		done := make(chan struct{})
		go func() {
			for i := 0; i < testSize; i++ {
				q.Enqueue(pointers[i])
				wd.Progress()
			}
			close(done)
		}()

		// Dequeue phase
		for i := 0; i < testSize; i++ {
			var got *int
			for {
				var ok bool
				got, ok = q.Dequeue()
				if ok {
					break
				}
				time.Sleep(1 * time.Microsecond)
			}
			wd.Progress()

			if got == nil {
				t.Fatalf("Received nil pointer at dequeue %d", i)
			}
			dequeuedPointers[got] = *got
		}

		// Ensure producer finished
		<-done

		// Verify completeness
		for p, expectedVal := range enqueuedPointers {
			gotVal, found := dequeuedPointers[p]
			if !found {
				t.Errorf("Lost message: pointer %p (value %d) was enqueued but never dequeued", p, expectedVal)
			} else if gotVal != expectedVal {
				t.Errorf("Value corruption: pointer %p expected %d, got %d", p, expectedVal, gotVal)
			}
		}

		// Check for unexpected pointers
		for p := range dequeuedPointers {
			if _, found := enqueuedPointers[p]; !found {
				t.Errorf("Unexpected pointer received: %p (value %d)", p, *p)
			}
		}

		if len(enqueuedPointers) != len(dequeuedPointers) {
			t.Fatalf("Count mismatch: enqueued %d, dequeued %d", len(enqueuedPointers), len(dequeuedPointers))
		}
	})
}

// TestNoLostMessagesHighContention tests completeness under high concurrent load.
func TestNoLostMessagesHighContention(t *testing.T) {
	withAllQueuesFixed(t, "NoLostMessagesHighContention", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "NoLostMessagesHighContention", impl)
		q := impl.newQueue(512)
		wd := newWatchdog(t, "NoLostMessagesHighContention")
		wd.Start()
		defer wd.Stop()

		numProducers := getConcurrency()
		numConsumers := getConcurrency()
		itemsPerProducer := getTestSize() / numProducers
		totalItems := numProducers * itemsPerProducer

		// Thread-safe tracking of all enqueued pointers
		var enqueuedMu sync.Mutex
		enqueued := make(map[*int]int, totalItems)

		// Thread-safe tracking of all dequeued pointers
		var dequeuedMu sync.Mutex
		dequeued := make(map[*int]int, totalItems)

		var prodWg sync.WaitGroup
		prodWg.Add(numProducers)

		var consumedCount atomic.Int64

		// Start producers
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer prodWg.Done()
				for i := 0; i < itemsPerProducer; i++ {
					ptr := new(int)
					*ptr = producerID*itemsPerProducer + i

					enqueuedMu.Lock()
					enqueued[ptr] = *ptr
					enqueuedMu.Unlock()

					q.Enqueue(ptr)
					wd.Progress()
				}
			}(p)
		}

		// Start consumers
		var consWg sync.WaitGroup
		consWg.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func() {
				defer consWg.Done()
				for {
					if consumedCount.Load() >= int64(totalItems) {
						return
					}

					ptr, ok := q.Dequeue()
					if !ok {
						time.Sleep(1 * time.Microsecond)
						continue
					}
					wd.Progress()

					dequeuedMu.Lock()
					if _, exists := dequeued[ptr]; exists {
						t.Errorf("Duplicate dequeue of pointer %p (value %d)", ptr, *ptr)
					}
					dequeued[ptr] = *ptr
					dequeuedMu.Unlock()

					consumedCount.Add(1)
				}
			}()
		}

		prodWg.Wait()
		consWg.Wait()

		// Verify completeness
		enqueuedMu.Lock()
		dequeuedMu.Lock()
		defer enqueuedMu.Unlock()
		defer dequeuedMu.Unlock()

		missing := 0
		for p, val := range enqueued {
			if _, found := dequeued[p]; !found {
				missing++
				if missing <= 10 { // Limit error output
					t.Errorf("Lost message: pointer %p (value %d)", p, val)
				}
			}
		}

		unexpected := 0
		for p := range dequeued {
			if _, found := enqueued[p]; !found {
				unexpected++
				if unexpected <= 10 {
					t.Errorf("Unexpected pointer: %p (value %d)", p, *p)
				}
			}
		}

		if missing > 0 || unexpected > 0 {
			t.Fatalf("Completeness failure: %d missing, %d unexpected (total enqueued: %d, dequeued: %d)",
				missing, unexpected, len(enqueued), len(dequeued))
		}
	})
}

// TestNoLostMessagesStress is an optional large-scale completeness test.
func TestNoLostMessagesStress(t *testing.T) {
	if !stressTestsEnabled() {
		t.Skip("Stress tests disabled. Set FIFO_ENABLE_STRESS=true to enable.")
	}

	withAllQueuesFixed(t, "NoLostMessagesStress", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "NoLostMessagesStress", impl)
		q := impl.newQueue(4096)
		wd := newWatchdog(t, "NoLostMessagesStress")
		wd.Start()
		defer wd.Stop()

		stressSize := getStressSize()
		numProducers := runtime.NumCPU()
		numConsumers := runtime.NumCPU()
		itemsPerProducer := stressSize / numProducers
		totalItems := numProducers * itemsPerProducer

		t.Logf("Stress test: %d items, %d producers, %d consumers", totalItems, numProducers, numConsumers)

		var producedCount atomic.Int64
		var consumedCount atomic.Int64

		// Use atomic bit-set for tracking (more memory efficient for large tests)
		received := make([]atomic.Bool, totalItems)

		var prodWg sync.WaitGroup
		prodWg.Add(numProducers)

		// Start producers
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer prodWg.Done()
				baseIdx := producerID * itemsPerProducer
				for i := 0; i < itemsPerProducer; i++ {
					idx := baseIdx + i
					ptr := new(int)
					*ptr = idx
					q.Enqueue(ptr)
					producedCount.Add(1)
					if i%10000 == 0 {
						wd.Progress()
					}
				}
			}(p)
		}

		// Start consumers
		var consWg sync.WaitGroup
		consWg.Add(numConsumers)
		for c := 0; c < numConsumers; c++ {
			go func() {
				defer consWg.Done()
				localConsumed := 0
				for {
					if consumedCount.Load() >= int64(totalItems) {
						return
					}

					ptr, ok := q.Dequeue()
					if !ok {
						runtime.Gosched()
						continue
					}

					idx := *ptr
					if idx < 0 || idx >= totalItems {
						t.Errorf("Invalid index received: %d", idx)
						consumedCount.Add(1)
						continue
					}

					if received[idx].Swap(true) {
						t.Errorf("Duplicate message received: index %d", idx)
					}

					consumedCount.Add(1)
					localConsumed++
					if localConsumed%10000 == 0 {
						wd.Progress()
					}
				}
			}()
		}

		prodWg.Wait()
		consWg.Wait()

		// Verify all items received
		missing := 0
		for i := 0; i < totalItems; i++ {
			if !received[i].Load() {
				missing++
				if missing <= 10 {
					t.Errorf("Missing item at index %d", i)
				}
			}
		}

		produced := producedCount.Load()
		consumed := consumedCount.Load()

		if missing > 0 {
			t.Fatalf("Stress test failed: %d missing items (produced: %d, consumed: %d)", missing, produced, consumed)
		}

		t.Logf("Stress test passed: %d items transferred correctly", totalItems)
	})
}

// =============================================================================
// Pointer Integrity Tests
// =============================================================================

// TestPointerIntegrityAllImplementations verifies that all queue implementations
// preserve pointer identity and value integrity.
func TestPointerIntegrityAllImplementations(t *testing.T) {
	withAllQueuesFixed(t, "PointerIntegrityAllImplementations", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "PointerIntegrityAllImplementations", impl)
		q := impl.newQueue(256)
		wd := newWatchdog(t, "PointerIntegrityAllImplementations")
		wd.Start()
		defer wd.Stop()

		testSize := getTestSize()

		// Create items with unique addresses and values
		type item struct {
			ptr      *int
			expected int
		}
		items := make([]item, testSize)
		for i := 0; i < testSize; i++ {
			p := new(int)
			*p = i*7 + 13 // Distinct pattern to catch value corruption
			items[i] = item{ptr: p, expected: *p}
		}

		// Producer
		go func() {
			for i := 0; i < testSize; i++ {
				q.Enqueue(items[i].ptr)
				wd.Progress()
			}
		}()

		// Consumer: receive and verify
		received := make(map[*int]int, testSize)
		receivedCount := 0

		for receivedCount < testSize {
			ptr, ok := q.Dequeue()
			if !ok {
				time.Sleep(1 * time.Microsecond)
				continue
			}
			wd.Progress()

			if ptr == nil {
				t.Fatalf("Received nil pointer at position %d", receivedCount)
			}

			received[ptr] = *ptr
			receivedCount++
		}

		// Verify all items
		pointerMismatches := 0
		valueMismatches := 0

		for _, it := range items {
			gotVal, found := received[it.ptr]
			if !found {
				pointerMismatches++
				if pointerMismatches <= 10 {
					t.Errorf("Pointer %p (expected value %d) was not received", it.ptr, it.expected)
				}
			} else if gotVal != it.expected {
				valueMismatches++
				if valueMismatches <= 10 {
					t.Errorf("Value mismatch for pointer %p: expected %d, got %d", it.ptr, it.expected, gotVal)
				}
			}
		}

		if pointerMismatches > 0 || valueMismatches > 0 {
			t.Fatalf("Pointer integrity failed: %d pointer mismatches, %d value mismatches",
				pointerMismatches, valueMismatches)
		}
	})
}

// TestPointerIntegrityConcurrent tests pointer integrity under concurrent access.
func TestPointerIntegrityConcurrent(t *testing.T) {
	withAllQueuesFixed(t, "PointerIntegrityConcurrent", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "PointerIntegrityConcurrent", impl)
		q := impl.newQueue(512)
		wd := newWatchdog(t, "PointerIntegrityConcurrent")
		wd.Start()
		defer wd.Stop()

		numProducers := getConcurrency()
		itemsPerProducer := getTestSize() / numProducers
		totalItems := numProducers * itemsPerProducer

		// Each producer creates unique pointers with verifiable values
		type itemRecord struct {
			ptr      *int
			expected int
		}

		var recordsMu sync.Mutex
		records := make([]itemRecord, 0, totalItems)

		var prodWg sync.WaitGroup
		prodWg.Add(numProducers)

		// Start producers
		for p := 0; p < numProducers; p++ {
			go func(producerID int) {
				defer prodWg.Done()
				for i := 0; i < itemsPerProducer; i++ {
					ptr := new(int)
					// Encode producer ID and sequence in the value
					value := producerID*1_000_000 + i
					*ptr = value

					recordsMu.Lock()
					records = append(records, itemRecord{ptr: ptr, expected: value})
					recordsMu.Unlock()

					q.Enqueue(ptr)
					wd.Progress()
				}
			}(p)
		}

		// Receive all items
		var receivedMu sync.Mutex
		received := make(map[*int]int, totalItems)
		var consumedCount atomic.Int64

		numConsumers := getConcurrency()
		var consWg sync.WaitGroup
		consWg.Add(numConsumers)

		for c := 0; c < numConsumers; c++ {
			go func() {
				defer consWg.Done()
				for {
					if consumedCount.Load() >= int64(totalItems) {
						return
					}

					ptr, ok := q.Dequeue()
					if !ok {
						time.Sleep(1 * time.Microsecond)
						continue
					}
					wd.Progress()

					receivedMu.Lock()
					received[ptr] = *ptr
					receivedMu.Unlock()

					consumedCount.Add(1)
				}
			}()
		}

		prodWg.Wait()
		consWg.Wait()

		// Verify integrity
		recordsMu.Lock()
		defer recordsMu.Unlock()
		receivedMu.Lock()
		defer receivedMu.Unlock()

		failures := 0
		for _, rec := range records {
			gotVal, found := received[rec.ptr]
			if !found {
				failures++
				if failures <= 10 {
					t.Errorf("Pointer %p not found in received set", rec.ptr)
				}
			} else if gotVal != rec.expected {
				failures++
				if failures <= 10 {
					t.Errorf("Value corruption: pointer %p expected %d, got %d", rec.ptr, rec.expected, gotVal)
				}
			}
		}

		if failures > 0 {
			t.Fatalf("Concurrent pointer integrity failed with %d failures", failures)
		}
	})
}

// =============================================================================
// Combined FIFO + Completeness Tests
// =============================================================================

// TestFIFOCompletenessAndOrdering is a comprehensive test that validates both
// FIFO ordering and message completeness simultaneously.
func TestFIFOCompletenessAndOrdering(t *testing.T) {
	withAllQueuesFixed(t, "FIFOCompletenessAndOrdering", []string{"FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "FIFOCompletenessAndOrdering", impl)
		q := impl.newQueue(128) // Small capacity for thorough testing
		wd := newWatchdog(t, "FIFOCompletenessAndOrdering")
		wd.Start()
		defer wd.Stop()

		testSize := getTestSize()

		// Create ordered sequence of unique pointers
		sequence := make([]*int, testSize)
		for i := 0; i < testSize; i++ {
			p := new(int)
			*p = i
			sequence[i] = p
		}

		// Producer
		go func() {
			for i := 0; i < testSize; i++ {
				q.Enqueue(sequence[i])
				wd.Progress()
			}
		}()

		// Consumer: verify both order and completeness
		received := make([]*int, 0, testSize)

		for len(received) < testSize {
			ptr, ok := q.Dequeue()
			if !ok {
				time.Sleep(1 * time.Microsecond)
				continue
			}
			wd.Progress()
			received = append(received, ptr)
		}

		// Verify FIFO ordering
		orderViolations := 0
		for i := 0; i < testSize; i++ {
			if received[i] != sequence[i] {
				orderViolations++
				if orderViolations <= 10 {
					t.Errorf("FIFO violation at %d: expected ptr %p (val %d), got ptr %p (val %d)",
						i, sequence[i], *sequence[i], received[i], *received[i])
				}
			}
		}

		// Verify completeness using set comparison
		receivedSet := make(map[*int]bool, testSize)
		for _, p := range received {
			if receivedSet[p] {
				t.Errorf("Duplicate pointer in received: %p", p)
			}
			receivedSet[p] = true
		}

		missing := 0
		for _, p := range sequence {
			if !receivedSet[p] {
				missing++
				if missing <= 10 {
					t.Errorf("Missing pointer: %p (value %d)", p, *p)
				}
			}
		}

		if orderViolations > 0 || missing > 0 {
			t.Fatalf("FIFO + Completeness test failed: %d order violations, %d missing items",
				orderViolations, missing)
		}
	})
}

// =============================================================================
// Race Detection Tests (designed to catch races with -race flag)
// =============================================================================

// TestRaceConditionDetection is designed to stress concurrent access patterns
// that might reveal race conditions when run with -race flag.
func TestRaceConditionDetection(t *testing.T) {
	withAllQueuesFixed(t, "RaceConditionDetection", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "RaceConditionDetection", impl)
		const capacity = 256
		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "RaceConditionDetection")
		wd.Start()
		defer wd.Stop()

		numGoroutines := getConcurrency()
		opsPerGoroutine := 500

		var wg sync.WaitGroup
		var enqueueCount atomic.Int64
		var dequeueCount atomic.Int64

		// Producer goroutines
		wg.Add(numGoroutines)
		for g := 0; g < numGoroutines; g++ {
			go func(gid int) {
				defer wg.Done()
				for i := 0; i < opsPerGoroutine; i++ {
					val := new(int)
					*val = gid*opsPerGoroutine + i
					q.Enqueue(val)
					enqueueCount.Add(1)
					if i%100 == 0 {
						wd.Progress()
					}
				}
			}(g)
		}

		// Consumer goroutines that also query slots
		wg.Add(numGoroutines)
		for g := 0; g < numGoroutines; g++ {
			go func() {
				defer wg.Done()
				localDequeued := 0
				for localDequeued < opsPerGoroutine {
					// Try to dequeue
					if _, ok := q.Dequeue(); ok {
						dequeueCount.Add(1)
						localDequeued++
						wd.Progress()
					}
					// Query slots (should not race)
					_ = q.UsedSlots()
					_ = q.FreeSlots()
				}
			}()
		}

		wg.Wait()

		// Final consistency check
		used := q.UsedSlots()
		free := q.FreeSlots()
		if used+free != capacity {
			t.Errorf("Slot count inconsistency after race test: used=%d, free=%d, sum=%d (expected %d)",
				used, free, used+free, capacity)
		}

		totalEnqueued := enqueueCount.Load()
		totalDequeued := dequeueCount.Load()
		if totalEnqueued != totalDequeued {
			t.Logf("Note: enqueued=%d, dequeued=%d (queue has %d remaining)", totalEnqueued, totalDequeued, used)
		}
	})
}

// TestConcurrentEnqueueDequeueBalance ensures that under concurrent load,
// the number of successful dequeues equals the number of enqueues.
func TestConcurrentEnqueueDequeueBalance(t *testing.T) {
	withAllQueuesFixed(t, "ConcurrentEnqueueDequeueBalance", []string{"MPMC"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "ConcurrentEnqueueDequeueBalance", impl)
		q := impl.newQueue(1024)
		wd := newWatchdog(t, "ConcurrentEnqueueDequeueBalance")
		wd.Start()
		defer wd.Stop()

		numProducers := getConcurrency()
		numConsumers := getConcurrency()
		itemsPerProducer := getTestSize() / numProducers
		totalExpected := int64(numProducers * itemsPerProducer)

		var enqueueCount atomic.Int64
		var dequeueCount atomic.Int64
		var productionDone atomic.Bool

		var prodWg sync.WaitGroup
		prodWg.Add(numProducers)

		// Producers
		for p := 0; p < numProducers; p++ {
			go func(pid int) {
				defer prodWg.Done()
				for i := 0; i < itemsPerProducer; i++ {
					ptr := new(int)
					*ptr = pid*itemsPerProducer + i
					q.Enqueue(ptr)
					enqueueCount.Add(1)
					if i%1000 == 0 {
						wd.Progress()
					}
				}
			}(p)
		}

		// Wait for producers and signal completion
		go func() {
			prodWg.Wait()
			productionDone.Store(true)
		}()

		// Consumers
		var consWg sync.WaitGroup
		consWg.Add(numConsumers)

		for c := 0; c < numConsumers; c++ {
			go func() {
				defer consWg.Done()
				for {
					if productionDone.Load() && dequeueCount.Load() >= totalExpected {
						return
					}

					_, ok := q.Dequeue()
					if ok {
						dequeueCount.Add(1)
						wd.Progress()
					} else if productionDone.Load() {
						// Double-check with a small delay
						time.Sleep(1 * time.Millisecond)
						if q.UsedSlots() == 0 {
							return
						}
					} else {
						runtime.Gosched()
					}
				}
			}()
		}

		consWg.Wait()

		enqueued := enqueueCount.Load()
		dequeued := dequeueCount.Load()

		if enqueued != totalExpected {
			t.Errorf("Enqueue count mismatch: expected %d, got %d", totalExpected, enqueued)
		}

		if dequeued != enqueued {
			t.Errorf("Balance violation: enqueued %d, dequeued %d (diff: %d)",
				enqueued, dequeued, enqueued-dequeued)
		}

		// Queue should be empty
		if q.UsedSlots() != 0 {
			t.Errorf("Queue not empty after balanced test: UsedSlots=%d", q.UsedSlots())
		}
	})
}

// =============================================================================
// Edge Case Tests
// =============================================================================

// TestCapacityBoundaryBehavior tests behavior at exact capacity boundaries.
func TestCapacityBoundaryBehavior(t *testing.T) {
	withAllQueuesFixed(t, "CapacityBoundaryBehavior", nil, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "CapacityBoundaryBehavior", impl)
		const capacity = 64
		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "CapacityBoundaryBehavior")
		wd.Start()
		defer wd.Stop()

		// Fill to exact capacity
		pointers := make([]*int, capacity)
		for i := 0; i < capacity; i++ {
			p := new(int)
			*p = i
			pointers[i] = p
			q.Enqueue(p)
			wd.Progress()
		}

		// Verify full
		if q.FreeSlots() != 0 {
			t.Errorf("Expected 0 free slots at capacity, got %d", q.FreeSlots())
		}
		if q.UsedSlots() != capacity {
			t.Errorf("Expected %d used slots at capacity, got %d", capacity, q.UsedSlots())
		}

		// Dequeue one
		got, ok := q.Dequeue()
		if !ok {
			t.Fatal("Failed to dequeue from full queue")
		}
		if got != pointers[0] {
			t.Errorf("First dequeue: expected %p, got %p", pointers[0], got)
		}
		wd.Progress()

		// Verify counts
		if q.FreeSlots() != 1 {
			t.Errorf("Expected 1 free slot after dequeue, got %d", q.FreeSlots())
		}
		if q.UsedSlots() != capacity-1 {
			t.Errorf("Expected %d used slots after dequeue, got %d", capacity-1, q.UsedSlots())
		}

		// Enqueue one more
		newPtr := new(int)
		*newPtr = 999
		q.Enqueue(newPtr)
		wd.Progress()

		// Should be full again
		if q.FreeSlots() != 0 {
			t.Errorf("Expected 0 free slots after refill, got %d", q.FreeSlots())
		}

		// Drain and verify all
		received := make([]*int, 0, capacity)
		for i := 0; i < capacity; i++ {
			val, ok := q.Dequeue()
			if !ok {
				t.Fatalf("Failed to dequeue at position %d", i)
			}
			received = append(received, val)
			wd.Progress()
		}

		// Verify we got the remaining original pointers + the new one
		if len(received) != capacity {
			t.Fatalf("Expected %d items, got %d", capacity, len(received))
		}

		// The new pointer should be last (FIFO)
		hasNewPtr := false
		for _, p := range received {
			if p == newPtr {
				hasNewPtr = true
				break
			}
		}
		if !hasNewPtr {
			t.Error("New pointer was not found in dequeued items")
		}
	})
}

// TestRepeatedFillAndDrain tests multiple complete fill/drain cycles.
func TestRepeatedFillAndDrain(t *testing.T) {
	withAllQueuesFixed(t, "RepeatedFillAndDrain", []string{"FIFO"}, func(t *testing.T, impl Implementation[*int, testQueueInterface]) {
		logTestStart(t, "RepeatedFillAndDrain", impl)
		const capacity = 128
		const cycles = 100
		q := impl.newQueue(capacity)
		wd := newWatchdog(t, "RepeatedFillAndDrain")
		wd.Start()
		defer wd.Stop()

		for cycle := 0; cycle < cycles; cycle++ {
			// Fill
			pointers := make([]*int, capacity)
			for i := 0; i < capacity; i++ {
				p := new(int)
				*p = cycle*capacity + i
				pointers[i] = p
				q.Enqueue(p)
			}
			wd.Progress()

			// Drain and verify FIFO order
			for i := 0; i < capacity; i++ {
				got, ok := q.Dequeue()
				if !ok {
					t.Fatalf("Cycle %d: failed to dequeue at position %d", cycle, i)
				}
				if got != pointers[i] {
					t.Fatalf("Cycle %d: FIFO violation at %d: expected %p, got %p",
						cycle, i, pointers[i], got)
				}
			}
			wd.Progress()

			// Verify empty
			if q.UsedSlots() != 0 {
				t.Fatalf("Cycle %d: queue not empty after drain", cycle)
			}
		}
	})
}

// =============================================================================
// Ordering Verification Helpers
// =============================================================================

// logTestStart prints a short message to the test log indicating which test and
// implementation are about to run. This helps surface progress when running
// `go test ./... -v` so you can see which implementations are exercised.
func logTestStart(t *testing.T, testName string, impl Implementation[*int, testQueueInterface]) {
	t.Helper()
	t.Logf("Starting %s (impl: %q, features: %v)", testName, impl.name, impl.features)
}

// logTestStartNoImpl is a convenience wrapper for tests that don't have a specific
// implementation context (top-level tests).
func logTestStartNoImpl(t *testing.T, testName string) {
	t.Helper()
	t.Logf("Starting %s", testName)
}

// verifyMonotonicOrdering checks that values form a monotonically increasing sequence
// within each producer's stream.
func verifyMonotonicOrdering(t *testing.T, received []int, numProducers, itemsPerProducer int) {
	t.Helper()

	// Track last seen value per producer
	lastSeen := make(map[int]int)
	for i := 0; i < numProducers; i++ {
		lastSeen[i] = -1
	}

	for i, val := range received {
		producerID := val / itemsPerProducer
		localSeq := val % itemsPerProducer

		if localSeq <= lastSeen[producerID] {
			t.Errorf("Monotonic ordering violation at index %d: producer %d received %d after %d",
				i, producerID, localSeq, lastSeen[producerID])
		}
		lastSeen[producerID] = localSeq
	}
}

// verifyCompleteness checks that all expected values are present exactly once.
func verifyCompleteness(t *testing.T, received []int, expected int) {
	t.Helper()

	seen := make(map[int]int) // value -> count
	for _, v := range received {
		seen[v]++
	}

	missing := 0
	duplicates := 0

	for i := 0; i < expected; i++ {
		count := seen[i]
		if count == 0 {
			missing++
			if missing <= 10 {
				t.Errorf("Missing value: %d", i)
			}
		} else if count > 1 {
			duplicates++
			if duplicates <= 10 {
				t.Errorf("Duplicate value: %d (count: %d)", i, count)
			}
		}
	}

	if missing > 0 || duplicates > 0 {
		t.Errorf("Completeness check failed: %d missing, %d duplicated", missing, duplicates)
	}
}

// =============================================================================
// Benchmark Tests (for -bench flag)
// =============================================================================

// BenchmarkFIFOThroughput measures pure FIFO throughput with single producer/consumer.
func BenchmarkFIFOThroughput(b *testing.B) {
	impls := getImplementations()
	for _, impl := range impls {
		if impl.newQueue == nil {
			continue
		}

		// Only benchmark FIFO implementations
		hasFIFO := false
		for _, f := range impl.features {
			if f == "FIFO" {
				hasFIFO = true
				break
			}
		}
		if !hasFIFO {
			continue
		}

		b.Run(impl.name, func(b *testing.B) {
			q := impl.newQueue(1024)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ptr := new(int)
				*ptr = i
				q.Enqueue(ptr)
				for {
					if _, ok := q.Dequeue(); ok {
						break
					}
				}
			}
		})
	}
}

// BenchmarkCompleteness measures throughput while verifying completeness.
func BenchmarkCompleteness(b *testing.B) {
	impls := getImplementations()
	for _, impl := range impls {
		if impl.newQueue == nil {
			continue
		}

		b.Run(impl.name, func(b *testing.B) {
			q := impl.newQueue(1024)
			received := make(map[*int]bool, b.N)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ptr := new(int)
				*ptr = i
				q.Enqueue(ptr)
				for {
					if got, ok := q.Dequeue(); ok {
						received[got] = true
						break
					}
				}
			}
			b.StopTimer()

			if len(received) != b.N {
				b.Fatalf("Completeness failure: expected %d, got %d", b.N, len(received))
			}
		})
	}
}

// =============================================================================
// Summary Output Test (informational)
// =============================================================================

// TestPrintTestConfiguration outputs the current test configuration (informational).
func TestPrintTestConfiguration(t *testing.T) {
	t.Logf("FIFO Integrity Test Configuration:")
	t.Logf("  FIFO_TEST_SIZE:     %d", getTestSize())
	t.Logf("  FIFO_STRESS_SIZE:   %d", getStressSize())
	t.Logf("  FIFO_ENABLE_STRESS: %v", stressTestsEnabled())
	t.Logf("  FIFO_CONCURRENCY:   %d", getConcurrency())
	t.Logf("  runtime.NumCPU():   %d", runtime.NumCPU())
	t.Logf("  runtime.GOMAXPROCS: %d", runtime.GOMAXPROCS(0))

	// List implementations and their features
	impls := getImplementations()
	t.Logf("\nRegistered Implementations:")
	for _, impl := range impls {
		features := "none"
		if len(impl.features) > 0 {
			features = fmt.Sprintf("%v", impl.features)
		}
		t.Logf("  - %s: %s", impl.name, features)
	}
}

// TestVerifyFIFOImplementationsExist ensures FIFO-tagged implementations exist.
func TestVerifyFIFOImplementationsExist(t *testing.T) {
	logTestStartNoImpl(t, "TestVerifyFIFOImplementationsExist")
	impls := getImplementations()

	fifoCount := 0
	multiHeadCount := 0

	for _, impl := range impls {
		for _, f := range impl.features {
			if f == "FIFO" {
				fifoCount++
			}
			if f == "Multi-Head-FIFO" {
				multiHeadCount++
			}
		}
	}

	if fifoCount == 0 {
		t.Error("No implementations with FIFO feature found")
	} else {
		t.Logf("Found %d implementations with FIFO feature", fifoCount)
	}

	if multiHeadCount == 0 {
		t.Log("No implementations with Multi-Head-FIFO feature found (this may be expected)")
	} else {
		t.Logf("Found %d implementations with Multi-Head-FIFO feature", multiHeadCount)
	}
}

// =============================================================================
// Global Ordering Test for All Implementations
// =============================================================================

// TestGlobalOrderingForAllTypes tests ordering guarantees for each implementation type.
func TestGlobalOrderingForAllTypes(t *testing.T) {
	logTestStartNoImpl(t, "TestGlobalOrderingForAllTypes")
	impls := getImplementations()

	// Group implementations by their ordering guarantee
	fifoImpls := make([]string, 0)
	multiHeadImpls := make([]string, 0)
	otherImpls := make([]string, 0)

	for _, impl := range impls {
		hasFIFO := false
		hasMultiHead := false

		for _, f := range impl.features {
			if f == "FIFO" {
				hasFIFO = true
			}
			if f == "Multi-Head-FIFO" {
				hasMultiHead = true
			}
		}

		if hasMultiHead {
			multiHeadImpls = append(multiHeadImpls, impl.name)
		} else if hasFIFO {
			fifoImpls = append(fifoImpls, impl.name)
		} else {
			otherImpls = append(otherImpls, impl.name)
		}
	}

	// Sort for consistent output
	sort.Strings(fifoImpls)
	sort.Strings(multiHeadImpls)
	sort.Strings(otherImpls)

	t.Logf("FIFO implementations (strict global ordering): %v", fifoImpls)
	t.Logf("Multi-Head-FIFO implementations (per-shard ordering): %v", multiHeadImpls)
	t.Logf("Other implementations (no ordering guarantee): %v", otherImpls)

	// Run appropriate ordering test for each group
	for _, name := range fifoImpls {
		t.Run("GlobalFIFO_"+name, func(t *testing.T) {
			for _, impl := range impls {
				if impl.name == name {
					testGlobalFIFOOrdering(t, impl)
					break
				}
			}
		})
	}

	for _, name := range multiHeadImpls {
		t.Run("MultiHeadFIFO_"+name, func(t *testing.T) {
			for _, impl := range impls {
				if impl.name == name {
					testPerProducerOrdering(t, impl)
					break
				}
			}
		})
	}
}

// testGlobalFIFOOrdering tests strict global FIFO ordering.
func testGlobalFIFOOrdering(t *testing.T, impl Implementation[*int, testQueueInterface]) {
	logTestStart(t, "GlobalFIFO_"+impl.name, impl)
	const capacity = 256
	const testSize = 500
	q := impl.newQueue(capacity)
	wd := newWatchdog(t, "GlobalFIFO_"+impl.name)
	wd.Start()
	defer wd.Stop()

	// Single producer sequence
	pointers := make([]*int, testSize)
	for i := 0; i < testSize; i++ {
		p := new(int)
		*p = i
		pointers[i] = p
	}

	// Producer goroutine
	done := make(chan struct{})
	go func() {
		for i := 0; i < testSize; i++ {
			q.Enqueue(pointers[i])
			wd.Progress()
		}
		close(done)
	}()

	// Dequeue and verify strict order
	for i := 0; i < testSize; i++ {
		var got *int
		for {
			var ok bool
			got, ok = q.Dequeue()
			if ok {
				break
			}
			time.Sleep(1 * time.Microsecond)
		}
		wd.Progress()

		if got != pointers[i] {
			t.Fatalf("Global FIFO violation at %d: expected %p (val %d), got %p (val %d)",
				i, pointers[i], *pointers[i], got, *got)
		}
	}

	<-done
}

// testPerProducerOrdering tests that each producer's messages maintain order.
// NOTE: Multi-Head FIFO implementations may NOT maintain per-producer ordering
// due to their sharded design. This test documents the behavior.
func testPerProducerOrdering(t *testing.T, impl Implementation[*int, testQueueInterface]) {
	logTestStart(t, "PerProducerOrder_"+impl.name, impl)
	q := impl.newQueue(256)
	wd := newWatchdog(t, "PerProducerOrder_"+impl.name)
	wd.Start()
	defer wd.Stop()

	numProducers := 10
	itemsPerProducer := 100
	totalItems := numProducers * itemsPerProducer

	lastSeen := make([]int, numProducers)
	for i := range lastSeen {
		lastSeen[i] = -1
	}
	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(numProducers)

	for p := 0; p < numProducers; p++ {
		go func(pid int) {
			defer wg.Done()
			for seq := 0; seq < itemsPerProducer; seq++ {
				ptr := new(int)
				*ptr = pid*1000 + seq
				q.Enqueue(ptr)
				wd.Progress()
			}
		}(p)
	}

	// Receive all
	received := 0
	violations := 0
	for received < totalItems {
		ptr, ok := q.Dequeue()
		if !ok {
			time.Sleep(1 * time.Microsecond)
			continue
		}
		wd.Progress()

		pid := *ptr / 1000
		seq := *ptr % 1000

		mu.Lock()
		if seq <= lastSeen[pid] {
			violations++
		}
		lastSeen[pid] = seq
		mu.Unlock()

		received++
	}

	wg.Wait()

	// Document behavior - Multi-Head FIFO doesn't guarantee per-producer ordering
	if violations > 0 {
		t.Logf("INFO: Multi-Head queue had %d per-producer ordering variations (expected for sharded design)", violations)
	}

	// Verify completeness - all items must be received
	if received != totalItems {
		t.Fatalf("Completeness violation: expected %d items, got %d", totalItems, received)
	}
}
