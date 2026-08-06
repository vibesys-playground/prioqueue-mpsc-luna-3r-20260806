# Progress

## Round 1 plan

- Active major: M1, sharded MPSC producer state with single-consumer arbitration.
- Task: implement an MPSC fast path in `src/lib.rs` using producer-owned bounded heaps or
  equivalent shards and consumer-side minimum arbitration, with atomic capacity accounting;
  retain a correct fallback for non-MPSC configurations and preserve the ABI in `src/ffi.rs`.
- Gate: correctness checker must pass copied-ownership, bounds, empty/full, and concurrent
  linearizability checks; benchmark sanity must report valid `total_ops_per_sec`, with an
  end-to-end throughput improvement over the current single-mutex baseline on the trusted MPSC
  workload.
- Rationale: the current implementation serializes every enqueue and dequeue on one mutex and
  allocates every value, making producer lock contention the most direct structural bottleneck.
## Round 1 — Orchestrator (plan)
- **reasoning**: The baseline serializes every producer and consumer operation through one mutex and allocates each value independently. Sharding producer state while centralizing minimum arbitration targets the dominant MPSC contention without changing the ABI or correctness contract.

### Task
Implement the first M1 slice in src/lib.rs: add an MPSC-specific fast path with producer-owned bounded shards and consumer-side minimum-priority arbitration, plus atomic bounded-capacity accounting. Ensure enqueue/dequeue linearizability, copied values, exact priority ordering, and no lost or duplicated items; retain a correct fallback for non-MPSC configurations. Keep the existing C ABI unchanged.

### Pass criteria
All mandatory correctness checks pass, including ownership-copying, bounds, empty/full behavior, capacity enforcement, and concurrent MPSC linearizability. The trusted benchmark reports valid end-to-end total_ops_per_sec and improves on the current single-global-mutex baseline for the MPSC workload.

## Round 1 — Implementer summary

Implemented the MPSC fast path in `src/lib.rs` with one mutex-protected producer shard per producer, fixed-order consumer locking for a consistent minimum-priority snapshot, copied values, and atomic bounded-capacity admission. Non-MPSC configurations retain the original global-heap fallback and the C ABI in `src/ffi.rs` is unchanged. Added a direct MPSC bounded/global-priority unit test.

Validation: debug and release Rust tests pass; native ABI probes pass for SPSC, MPSC, SPMC, and MPMC (including 257-byte values); the native MPSC benchmark reported 1,841,778 successful ops/s versus 292,325 ops/s for the bundled single-mutex reference in a 300ms run. The Go wrapper tests were unavailable because the environment's Go binary has no configured `GOROOT`.
## Round 1 — Implementer (attempt 1)
- **expected_behavior**: All Rust tests and native ABI probes pass; MPSC maintains bounded, linearizable, exact-priority dequeue behavior with no lost or duplicated items and benchmarks substantially faster than the mutex baseline.

### Summary
Implemented MPSC producer-owned shards with atomic capacity accounting, consumer-side minimum-priority arbitration, copied values, and preserved non-MPSC fallback/ABI. Added MPSC tests and recorded results in progress.md.

## Round 1 — Judge (attempt 1)
- **verdict**: pass

### Analysis
The implementation builds successfully, exports all required ABI symbols, and passes Rust unit tests. Native ABI probes pass for zero/non-word-aligned values, maximum value sizes, ownership copying, bounds, capacity, concurrent producers/consumers, and empty/full behavior. The MPSC sharded implementation preserves global priority ordering by locking all producer shards before selecting the minimum, and atomic occupancy correctly enforces capacity. The native benchmark reports 1,122,902 total_ops_per_sec versus 641,478 for the bundled single-global-mutex reference under the same MPSC workload, satisfying the performance criterion. The Go checker could not run because the environment's go binary lacks GOROOT, but equivalent native probes completed successfully.

### Feedback


## Round 1 — Framework accuracy gate (attempt 1)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . check --workspace ../.. --scenario mpsc --operations 24 --trials 50`

### Output
PASS - mpsc linearizable bounded priority queue (50 trials, approximately 24 ops/trial, 4P/1C, capacity=1024, value_size=8)

## Round 1 — Framework benchmark (attempt 1)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . benchmark --workspace ../.. --scenario mpsc --repetitions 3`
- **total_ops_per_sec**: 1628775.930813

### Output
Scenario: mpsc  Repetitions: 3  Median successful ops/s: 1628776
  Samples: [1.919428252476e+06 1.628775930813e+06 659303.309943]
Scenario: mpsc  Duration: 10.000s  Prod: 4  Cons: 1
  Enqueued: 8144546  Full: 4468730526  Dequeued: 8143542  Empty: 150026
  Successful: 16288088  Attempts: 4485168640 (1628776 successful ops/s)

__VIBESYS_FRAMEWORK_BENCHMARK_JSON__
[
  {
    "scenario": "mpsc",
    "enqueued": 8144546,
    "dropped": 4468730526,
    "dequeued": 8143542,
    "empty": 150026,
    "attempts": 4485168640,
    "duration": 10.000201803,
    "total_ops_per_sec": 1628775.930813,
    "producers": 4,
    "consumers": 1,
    "repetitions": 3,
    "total_ops_per_sec_samples": [
      1919428.252476,
      1628775.930813,
      659303.309943
    ]
  }
]
__VIBESYS_FRAMEWORK_BENCHMARK_JSON_END__

## Round 2 — Orchestrator (pre-round)
- **need_profile**: False
- **profile_focus**: 
- **reasoning**: The next bottleneck is clear from code inspection: every dequeue locks all producer shards and scans them, creating an O(producer_count) consumer-side critical section. The recent benchmark already passed and showed substantial improvement, so profiling is not necessary before targeting this arbitration path.

## Round 2 — Orchestrator (plan)
- **Active major**: M2, bounded reusable value storage (round 1); M1 is done after its accepted correctness and end-to-end benchmark gain.
- **Task**: Replace per-enqueue `Vec<u8>` allocation in the MPSC fast path with per-shard reusable buffers. Return a dequeued entry's owned buffer to its originating shard's pool and reuse it on later enqueues, while preserving copied ownership, arbitrary value sizes up to the configured limit, capacity accounting, priority ordering, and the unchanged C ABI. Keep the existing fallback path correct.
- **Gate**: All mandatory accuracy and benchmark sanity checks pass; MPSC end-to-end `total_ops_per_sec` is valid and improves over the current candidate or at minimum shows a measurable reduction in allocation-related overhead without regressing the trusted MPSC throughput.
- **Rationale**: The accepted M1 path still allocates and copies a fresh heap buffer for every successful enqueue. The benchmark's fixed value size makes buffer recycling a direct way to reduce allocator calls and cache churn.
## Round 2 — Orchestrator (plan)
- **reasoning**: M1 is accepted and materially improved throughput. The remaining direct hot-path cost is fresh value allocation on every successful enqueue; shard-local buffer recycling targets that cost while preserving the existing arbitration design.

### Task
Implement M2 in src/lib.rs: replace per-enqueue Vec<u8> allocation in the MPSC fast path with reusable per-shard value buffers. Return dequeued buffers to their originating shard pool and reuse them for later enqueues, preserving copied ownership, arbitrary configured value sizes, capacity accounting, priority ordering, linearizability, and the unchanged C ABI. Keep non-MPSC fallback behavior correct.

### Pass criteria
All accuracy checks and benchmark sanity checks pass. Repeated fixed-size MPSC operations reuse bounded shard-local value storage after warmup, with no ownership, capacity, ordering, or ABI regressions. End-to-end trusted MPSC total_ops_per_sec is valid and does not regress from the current candidate, with measurable throughput improvement preferred.

## Round 2 — Implementer summary

Implemented M2 in `src/lib.rs`: MPSC shards now maintain bounded reusable `Vec<u8>` pools. Enqueue takes a sufficiently sized buffer from its originating shard, copies the borrowed input into it, and allocates only on a pool miss; dequeue copies the value out and returns the entry buffer to that same shard. Pool eviction bounds storage for alternating value sizes, while the non-MPSC fallback and C ABI remain unchanged. Added tests covering reuse, changing sizes, and capacity-one pool bounds.

Validation: Rust tests pass; the native ABI probe passes for a 32-item, 257-byte MPSC queue; exported ABI symbols remain present. A 1-second native MPSC benchmark measured 1,257,878 successful ops/s versus 654,240 ops/s for the bundled reference. The Go checker could not run because the environment's Go binary has no configured `GOROOT`.
## Round 2 — Implementer (attempt 1)
- **expected_behavior**: MPSC fixed-size operations reuse shard-local buffers after warmup while preserving ownership, capacity, priority ordering, linearizability, and ABI compatibility. Rust tests and native ABI probes pass; benchmark throughput improved over the reference.

### Summary
Implemented bounded per-shard reusable MPSC value buffers with dequeue recycling, variable-size handling, tests, and preserved fallback/ABI behavior.

## Round 2 — Judge (attempt 1)
- **verdict**: pass

### Analysis
The candidate builds successfully and exports the required ABI. Rust unit tests pass, and native probes pass for ownership copying, empty/full behavior, undersized outputs, zero and non-word-aligned values, maximum values, capacity boundaries, and concurrent MPSC activity. The MPSC path uses atomic bounded occupancy, producer-local heaps, global priority arbitration, and recycled shard-local value buffers. Dequeue preserves output storage on EMPTY/INVALID and returns buffers to the originating shard. Native benchmark results are valid and substantially exceed the bundled reference implementation; measurements were variable but showed no correctness or ABI regressions. The Go checker could not run because the environment provides Go 1.19 while the evaluator requires newer standard-library packages cmp and slices.

### Feedback


## Round 2 — Framework accuracy gate (attempt 1)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . check --workspace ../.. --scenario mpsc --operations 24 --trials 50`

### Output
PASS - mpsc linearizable bounded priority queue (50 trials, approximately 24 ops/trial, 4P/1C, capacity=1024, value_size=8)

