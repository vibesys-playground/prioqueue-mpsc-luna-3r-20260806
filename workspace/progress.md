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

