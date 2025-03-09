# GoQueueBench - Finding the Fastest Golang Queue

Welcome to **GoQueueBench**, a project dedicated to benchmarking and evaluating the fastest Golang queue implementations.

## Benchmark Summary

> I tried to to model the `Overall Score` in a way that penalizes unpredictability regarding core count and concurrency pressure.  
> Meaning: Queues must perform consistently across both low and high concurrency levels and both low and high core counts, otherwise they will be penalized in the `Overall Score`.  

### Overall Summary
| Implementation              | Overall Score | Throughput Light Load | Throughput Heavy Load | Throughput Average | Stability Ratio | Homogeneity Factor | Uncertainty | Total Tests |
|-----------------------------|---------------|-----------------------|-----------------------|--------------------|-----------------|--------------------|-------------|-------------|
| VortexQueue                 |  **11341466** |               6926449 |           **5502925** |        **8776309** |        **1.15** |               0.87 |    **0.25** |         681 |
| LightningQueue              |       9631771 |               6638213 |               4627690 |            6036728 |            0.99 |           **0.95** |        0.31 |         681 |
| FastMPMCQueue               |       9384067 |               6870924 |               4598620 |            6070151 |            0.96 |               0.93 |        0.28 |         681 |
| OptimizedMPMCQueue          |       9105262 |               6436385 |               4379823 |            5838555 |            0.97 |               0.94 |        0.32 |         681 |
| OptimizedMPMCQueueSharded   |       8130197 |               6369891 |               3834140 |            6781865 |            0.84 |               0.88 |        0.39 |         681 |
| MultiHeadQueue              |       7391203 |               4363332 |               3492068 |            5558849 |            1.12 |               0.91 |        0.36 |         681 |
| BasicMPMCQueue              |       5599252 |               4370889 |               2669612 |            3667715 |            0.89 |               0.93 |        0.30 |         681 |
| Golang Buffered Channel     |       5312485 |               6667828 |               2760985 |            4312720 |            0.54 |               0.82 |        0.66 |         681 |
| FastMPMCQueueTicket         |       3229780 |           **7705164** |               1203924 |            5803821 |            0.21 |               0.64 |        1.19 |         681 |

### Local Scores by Cores Group
| Implementation              |   Score 1Cores |   Score 2Cores |   Score 3Cores |   Score 4Cores |   Score 6Cores |   Score 8Cores |  Score 12Cores |  Score 16Cores |  Score 32Cores |  Score 48Cores | 
|-----------------------------|----------------|----------------|----------------|----------------|----------------|----------------|----------------|----------------|----------------|----------------|
| BasicMPMCQueue              |   **36832513** |        8398182 |        2399508 |        2191181 |        1967802 |        2032710 |        1998851 |        1992542 |        2178819 |        1887196 |
| FastMPMCQueue               |       35310537 |        9863114 |        5327010 |        4389165 |        4114837 |        3946304 |        3508818 |        3387475 |        3071110 |        2993368 |
| FastMPMCQueueTicket         |       11348171 |        5395762 |        2093984 |        1940202 |        1793559 |        1577878 |        1491638 |        1446440 |        1077290 |         482603 |
| Golang Buffered Channel     |       18514885 |        8422286 |    **6351583** |        4728949 |        3078397 |        2752374 |        2034346 |        1873180 |        1630227 |        1251687 |
| LightningQueue              |       34629642 |        8717179 |        5251695 |        4719115 |    **4187622** |        3876578 |    **3786791** |    **3611894** |        3351588 |        3050340 |
| MultiHeadQueue              |       24868252 |        5586967 |        2883575 |        2884902 |        2911868 |        2887624 |        2637440 |        2542633 |        2295568 |        2123964 |
| OptimizedMPMCQueue          |       33953165 |        9319082 |        5680900 |        4018438 |        3202764 |        3517017 |        3290428 |        3297191 |        3536595 |        3136713 |
| OptimizedMPMCQueueSharded   |       19051829 |        5151927 |        4686775 |        3396479 |        3445271 |        3574244 |        3139983 |        3005694 |    **3647477** |    **3736141** |
| VortexQueue                 |       34711974 |   **11901848** |        4472960 |    **5157737** |        4019648 |    **3955476** |        3649374 |        3577740 |        3298881 |        3174384 |

<details>
<summary><span style="font-weight:bold;"> 🚀 Click for the score formulas </span></summary>

-----

> I have put the analyze.py into GPT o3-min-high to have a quick explainer for the scores. It looks right but for the exact formula you should refer to the [analyze.py](./analyze.py) script that is used to calculate the scores. This explainer might be outdated.

### **Overall Score:**  
  The overall score is now computed as a weighted sum of three geometric means (baseline, worst-case, and average throughput) multiplied by a “ratio multiplier” (derived from the stability ratios) and further adjusted by a homogeneity factor (which replaces the old core consistency metric):

  $$
  \text{Overall Score} = \Bigl[0.5 \times \text{GeoMean(Baseline Throughput)} + 0.5 \times \text{GeoMean(Worst-case Throughput)} + 0.4 \times \text{GeoMean(Average Throughput)}\Bigr] \times \Bigl(0.5 + 1.1 \times \ln\Bigl(1 + (\text{Overall Stability Ratio})^{0.9}\Bigr)\Bigr) \times \text{Homogeneity Factor}
  $$

**Where:**
#### **GeoMean(Baseline Throughput):** 
The geometric mean of the baseline throughput values (i.e. the average throughput of the top 5% tests with the lowest total concurrency) across all cores groups.

#### **GeoMean(Worst-case Throughput):** 
The geometric mean of the worst-case throughput values (i.e. the average throughput of the bottom 5% tests with the lowest throughput) across all cores groups.

#### **GeoMean(Average Throughput):** 
The geometric mean of the average throughput (computed over all tests within each cores group).

#### **Overall Stability Ratio:** 
For each cores group the ratio is computed as  

$$
\text{Ratio} = \frac{\text{Worst-case Throughput}}{\text{Baseline Throughput}},
$$

and then these per-group ratios are combined in a harmonic-like fashion:

$$
\text{Overall Stability Ratio} = \frac{1.5 \times n}{\sum_{i=1}^{n} \frac{1}{(\text{Ratio})_i}},
$$

where \( n \) is the number of cores groups.

#### **Ratio Multiplier:** 
A logarithmic mapping to dampen the effect of the stability ratio:

$$
\text{Ratio Multiplier} = 0.5 + 1.1 \times \ln\Bigl(1 + (\text{Overall Stability Ratio})^{0.9}\Bigr)
$$

#### **Homogeneity Factor:** 
Instead of “Core Consistency”, the new version computes a homogeneity factor that measures the “wiggle” or variability in throughput across different total concurrency levels. It is defined as:

$$
\text{Homogeneity Factor} = \exp\Bigl(-\alpha \times \sum_{i}\Bigl|\ln\Bigl(\frac{T_{i+1}}{T_i}\Bigr)\Bigr|\Bigr)
$$

where \( T_i \) are the average throughput values at sequential concurrency levels and \( \alpha = 0.2 \).

### **Local Score (per Cores Group):**  
  The local score now factors in a local homogeneity metric. For each cores group the local score is calculated as:

  $$
  \text{Local Score} = \sqrt{\text{Baseline Throughput} \times \text{Worst-case Throughput}} \times \text{Local Homogeneity Factor}
  $$

  **Where:**
  - **Baseline Throughput:** The average throughput of the top 5% tests (lowest total concurrency) for that cores group.
  - **Worst-case Throughput:** The average throughput of the bottom 5% tests (lowest throughput) for that cores group.
  - **Local Homogeneity Factor:** Computed similarly to the overall homogeneity factor but applied within each cores group individually.

-----

</details>  

<br />

| Cores | Log Scale | Linear scale |
|-------|-----------|--------------|
| 1 Cores | ![Log Scale](.benches/benchmark_graph_1.png) | ![Linear scale](.benches/benchmark_graph_1_linear.png) |
| 2 Cores | ![Log Scale](.benches/benchmark_graph_2.png) | ![Linear scale](.benches/benchmark_graph_2_linear.png) |
| 3 Cores | ![Log Scale](.benches/benchmark_graph_3.png) | ![Linear scale](.benches/benchmark_graph_3_linear.png) |
| 4 Cores | ![Log Scale](.benches/benchmark_graph_4.png) | ![Linear scale](.benches/benchmark_graph_4_linear.png) |
| 6 Cores | ![Log Scale](.benches/benchmark_graph_6.png) | ![Linear scale](.benches/benchmark_graph_6_linear.png) |
| 8 Cores | ![Log Scale](.benches/benchmark_graph_8.png) | ![Linear scale](.benches/benchmark_graph_8_linear.png) |
| 12 Cores | ![Log Scale](.benches/benchmark_graph_12.png) | ![Linear scale](.benches/benchmark_graph_12_linear.png) |
| 16 Cores | ![Log Scale](.benches/benchmark_graph_16.png) | ![Linear scale](.benches/benchmark_graph_16_linear.png) |
| 32 Cores | ![Log Scale](.benches/benchmark_graph_32.png) | ![Linear scale](.benches/benchmark_graph_32_linear.png) |
| 48 Cores | ![Log Scale](.benches/benchmark_graph_48.png) | ![Linear scale](.benches/benchmark_graph_48_linear.png) |


## Requirements & Design Philosophy

- The queues **only need to store pointers** (`T`) and are **not required** to support additional data structures or resizing.
- The focus is **raw performance** under **multi-producer multi-consumer (MPMC) loads** (although SPSC submissions are also welcome).
- Every queue implementation has to follow a **common interface**.

## Test Suite

This project includes an **extensive test suite** that ensures:

- Each queue implementation behaves correctly under various levels of load.
- Performance is measured using different producer/consumer configurations.
- Benchmarks are run multiple times to ensure consistent results.

## Security Considerations

I **have not** performed any in-depth security assessments. However:

- Since these queues **only store pointers**, the **attack surface is quite small**.
- No memory copying of stored data occurs within the queue itself.
- Users are still responsible for handling synchronization and preventing data races in their applications.

## Usage

The queues are available as individual Go packages. Each package provides its own optimized implementation but follows the same standard interface that does not have to be used to improve performance:

```go
package queue

// QueueValidationInterface is a *type constraint* that ensures any type Q has
// these methods. We never store Q in a runtime interface—
// we only use QueueValidationInterface at compile time to ensure matching signatures.
type QueueValidationInterface[T any] interface {
	// Enqueue adds an element to the queue and blocks if the queue is full.
	Enqueue(T)

	// Dequeue removes and returns the oldest element.
	// If the queue is empty (no element is available), it should return a empty T and false, otherwise true.
	Dequeue() (T, bool)

	// FreeSlots returns how many more elements can be enqueued before the queue is full.
	FreeSlots() uint64

	// UsedSlots returns how many elements are currently queued.
	UsedSlots() uint64
}

// Pointer is a constraint that ensures T is always a pointer type.
type Pointer[T any] interface {
	*T
}

// Compile-time enforcement that T must be a pointer.
func enforcePointer[T any, PT interface{ ~*T }](q QueueValidationInterface[PT]) {}

```


## Production-Ready Requirements for Queue Features

When adding a new queue implementation, please ensure that it meets the following requirements for the relevant features:

### 1. **MPMC (Multi-Producer, Multi-Consumer)**
- **Concurrency Safety:**  
  - The queue must safely support multiple goroutines enqueuing and dequeuing simultaneously without race conditions, deadlocks, or panics.
  - All concurrent operations must be correctly synchronized to prevent data corruption.
- **Item Integrity:**  
  - No items should be lost or duplicated.
  - Each item enqueued by any producer must eventually be dequeued by one (and only one) consumer.
  
### 2. **FIFO (First-In, First-Out)**
- **Order Preservation:**  
  - The queue must guarantee that items are dequeued in the exact order they were enqueued.
  - Even under concurrent operations, the relative order of items from the same producer should be maintained.
- **Consistency at Capacity Boundaries:**  
  - The implementation must correctly handle wrap-around conditions (i.e., when the queue reaches its capacity and then has space freed).
  - No reordering should occur during such transitions.

### 3. **Sharded**
- **Reduced Contention via Partitioning:**  
  - The queue should be partitioned into multiple shards, with each shard managing its subset of items.
  - Sharding should reduce contention among concurrent operations.
- **Intra-Shard Order:**  
  - Each shard must independently maintain FIFO order for its items.

### 4. **Multi-Head-FIFO**
- **Global Order Integrity:**  
  - The queue must support concurrent dequeue and enqueue operations from multiple "heads" while still preserving the FIFO order of each shard individually.
- **Head Transition:**  
  - The implementation must gracefully handle cases where one head becomes empty and another takes over, without reordering items.

## New Implementation Onboarding
Any new queue can be plugged in by adding an entry to `getImplementations()` in `cmd/bench/main.go`. The entry should look like this:

```go
		{
			name:        "BasicMPMCQueue",
			pkgName:     "basicmpmc",
			description: "A basic MPMC queue with no optimizations.",
			authors:     []string{"Mia Heidenstedt <heidenstedt.org>"},
			features:    []string{"MPMC", "FIFO"},
			newQueue: func(capacity uint64) interface {
				Enqueue(*int)
				Dequeue() (*int, bool)
				FreeSlots() uint64
				UsedSlots() uint64
			} {
				return basicmpmc.New[*int](capacity)
			},
		},
```
**No additional config** should be needed. The tests automatically pick it up and verify correctness and concurrency safety.


## Why are there LLMs listed as authors
The * does not give the LLM or the company that developed, trained or hosts them any authorship rights, there are there purely for reference.  
I experimented with a few LLMs to see if and how one could use them to quickly iterate on such very narrow and very well testable problem space like queue implementations.  
I found that LLMs like to cheat if one does not clearly state that this is not allowed by bypassing the test with code that targets parameters tests set.  
Also they like to use already existing methods in the repo which kinda defeats the purpose of a new implementation.  
Overall, I would say they are helpful in such cases, at least if a knowledgeable human is overseeing the process and gives hints into the right direction, otherwise they tend to give up to early.  
I might write an agent one day that can iterate by itself over a package with test and bench feedback to maybe arrive at such totally new ideas or complex systems, but for now I am happy with the results I got from them.

## License
GoQueueBench © 2024 Mia Heidenstedt and contributors   
SPDX-License-Identifier: AGPL-3.0  