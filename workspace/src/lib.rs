#![deny(unsafe_op_in_unsafe_fn)]

mod ffi;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

const ABI_VERSION: u32 = 1;
const STATUS_OK: u32 = 0;
const STATUS_FULL: u32 = 1;
const STATUS_EMPTY: u32 = 2;
const STATUS_INVALID: u32 = 3;
const STATUS_INTERNAL_ERROR: u32 = 4;
const INLINE_VALUE_SIZE: usize = 8;

struct SpinMutex<T> {
    locked: AtomicBool,
    value: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for SpinMutex<T> {}
unsafe impl<T: Send> Sync for SpinMutex<T> {}

struct SpinGuard<'a, T> {
    mutex: &'a SpinMutex<T>,
}

impl<T> SpinMutex<T> {
    fn new(value: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    #[inline]
    fn lock(&self) -> SpinGuard<'_, T> {
        let mut spins = 0_u32;
        loop {
            if self
                .locked
                .compare_exchange_weak(false, true, AtomicOrdering::Acquire, AtomicOrdering::Relaxed)
                .is_ok()
            {
                break;
            }
            if spins < 128 {
                spins += 1;
                std::hint::spin_loop();
            } else {
                spins = 0;
                std::thread::yield_now();
            }
        }
        SpinGuard { mutex: self }
    }
}

impl<T> Deref for SpinGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // The guard owns the lock, so the value cannot be accessed mutably by
        // another thread while this reference is alive.
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T> DerefMut for SpinGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<T> Drop for SpinGuard<'_, T> {
    fn drop(&mut self) {
        self.mutex.locked.store(false, AtomicOrdering::Release);
    }
}

enum Value {
    Inline { len: u8, bytes: [u8; INLINE_VALUE_SIZE] },
    Heap(Vec<u8>),
}

impl Value {
    fn try_copy(data: &[u8]) -> Result<Self, ()> {
        if data.len() <= INLINE_VALUE_SIZE {
            let mut bytes = [0_u8; INLINE_VALUE_SIZE];
            bytes[..data.len()].copy_from_slice(data);
            Ok(Self::Inline {
                len: data.len() as u8,
                bytes,
            })
        } else {
            let mut value = Vec::new();
            value.try_reserve_exact(data.len()).map_err(|_| ())?;
            value.extend_from_slice(data);
            Ok(Self::Heap(value))
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => *len as usize,
            Self::Heap(value) => value.len(),
        }
    }

    #[inline]
    fn copy_to(&self, output: &mut [u8]) {
        match self {
            Self::Inline { len, bytes } => output[..*len as usize].copy_from_slice(&bytes[..*len as usize]),
            Self::Heap(value) => output[..value.len()].copy_from_slice(value),
        }
    }
}

struct Entry {
    priority: u64,
    value: Value,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.priority.cmp(&self.priority)
    }
}

struct Values {
    heap: BinaryHeap<Entry>,
}

struct Shard {
    heap: BinaryHeap<Entry>,
}

struct MpscShard {
    lock: SpinMutex<Shard>,
    min_priority: AtomicU64,
    has_item: AtomicBool,
}

struct MpscState {
    capacity: usize,
    // Reservations are made before copying the caller's value.  They make
    // admission bounded without putting producers behind a common mutex.
    occupied: AtomicUsize,
    shards: Vec<MpscShard>,
}

struct QueueState {
    capacity: usize,
    max_value_size: usize,
    values: Mutex<Values>,
    mpsc: Option<Arc<MpscState>>,
}

pub struct Queue {
    producer_count: u32,
    consumer_count: u32,
    state: Arc<QueueState>,
}

impl Queue {
    fn new(
        capacity: u64,
        max_value_size: u64,
        producer_count: u32,
        consumer_count: u32,
    ) -> Result<Self, u32> {
        if capacity == 0 || max_value_size == 0 || producer_count == 0 || consumer_count == 0 {
            return Err(STATUS_INVALID);
        }
        let capacity = usize::try_from(capacity).map_err(|_| STATUS_INVALID)?;
        let max_value_size = usize::try_from(max_value_size).map_err(|_| STATUS_INVALID)?;
        let mut heap = BinaryHeap::new();
        heap.try_reserve_exact(capacity)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        let mpsc = if producer_count > 1 && consumer_count == 1 {
            let mut shards = Vec::with_capacity(producer_count as usize);
            for _ in 0..producer_count {
                let mut heap = BinaryHeap::new();
                heap.try_reserve(1)
                    .map_err(|_| STATUS_INTERNAL_ERROR)?;
                shards.push(MpscShard {
                    lock: SpinMutex::new(Shard { heap }),
                    min_priority: AtomicU64::new(u64::MAX),
                    has_item: AtomicBool::new(false),
                });
            }
            Some(Arc::new(MpscState {
                capacity,
                occupied: AtomicUsize::new(0),
                shards,
            }))
        } else {
            None
        };
        Ok(Self {
            producer_count,
            consumer_count,
            state: Arc::new(QueueState {
                capacity,
                max_value_size,
                values: Mutex::new(Values {
                    heap,
                }),
                mpsc,
            }),
        })
    }

    fn producer(&self, id: u32) -> Result<Producer, u32> {
        (id < self.producer_count)
            .then(|| Producer {
                state: Arc::clone(&self.state),
                mpsc: self.state.mpsc.clone(),
                shard_id: id as usize,
            })
            .ok_or(STATUS_INVALID)
    }

    fn consumer(&self, id: u32) -> Result<Consumer, u32> {
        (id < self.consumer_count)
            .then(|| Consumer {
                state: Arc::clone(&self.state),
                mpsc: self.state.mpsc.clone(),
            })
            .ok_or(STATUS_INVALID)
    }
}

pub struct Producer {
    state: Arc<QueueState>,
    mpsc: Option<Arc<MpscState>>,
    shard_id: usize,
}

impl Producer {
    fn try_enqueue(&self, priority: u64, data: &[u8]) -> u32 {
        if data.len() > self.state.max_value_size {
            return STATUS_INVALID;
        }
        if let Some(mpsc) = &self.mpsc {
            return self.try_enqueue_mpsc(mpsc, priority, data);
        }
        let Ok(mut values) = self.state.values.lock() else {
            return STATUS_INTERNAL_ERROR;
        };
        if values.heap.len() == self.state.capacity {
            return STATUS_FULL;
        }
        let Ok(value) = Value::try_copy(data) else {
            return STATUS_INTERNAL_ERROR;
        };
        values.heap.push(Entry { priority, value });
        STATUS_OK
    }

    fn try_enqueue_mpsc(&self, mpsc: &MpscState, priority: u64, data: &[u8]) -> u32 {
        // A fetch-add is cheaper than a reservation CAS in the contended
        // full-queue benchmark. Temporary over-reservation is rolled back;
        // the reservation itself remains the capacity linearization point.
        if mpsc.capacity == usize::MAX {
            let mut occupied = mpsc.occupied.load(AtomicOrdering::Acquire);
            loop {
                if occupied >= mpsc.capacity {
                    return STATUS_FULL;
                }
                match mpsc.occupied.compare_exchange_weak(
                    occupied,
                    occupied + 1,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(actual) => occupied = actual,
                }
            }
        } else {
            let occupied = mpsc.occupied.fetch_add(1, AtomicOrdering::AcqRel);
            if occupied >= mpsc.capacity {
                mpsc.occupied.fetch_sub(1, AtomicOrdering::Release);
                return STATUS_FULL;
            }
        }

        let Ok(value) = Value::try_copy(data) else {
            mpsc.occupied.fetch_sub(1, AtomicOrdering::Release);
            return STATUS_INTERNAL_ERROR;
        };
        let shard_state = &mpsc.shards[self.shard_id];
        let mut shard = shard_state.lock.lock();
        if shard.heap.len() == shard.heap.capacity() {
            if shard.heap.try_reserve(1).is_err() {
                mpsc.occupied.fetch_sub(1, AtomicOrdering::Release);
                return STATUS_INTERNAL_ERROR;
            }
        }
        shard.heap.push(Entry { priority, value });
        shard_state.min_priority.store(
            shard.heap.peek().unwrap().priority,
            AtomicOrdering::Release,
        );
        shard_state.has_item.store(true, AtomicOrdering::Release);
        STATUS_OK
    }
}

pub struct Consumer {
    state: Arc<QueueState>,
    mpsc: Option<Arc<MpscState>>,
}

impl Consumer {
    fn try_dequeue(&self, output: &mut [u8]) -> Result<usize, u32> {
        if let Some(mpsc) = &self.mpsc {
            return self.try_dequeue_mpsc(mpsc, output);
        }
        let Ok(mut values) = self.state.values.lock() else {
            return Err(STATUS_INTERNAL_ERROR);
        };
        let Some(entry) = values.heap.peek() else {
            return Err(STATUS_EMPTY);
        };
        if entry.value.len() > output.len() {
            return Err(STATUS_INVALID);
        }
        entry.value.copy_to(output);
        let length = entry.value.len();
        values.heap.pop();
        Ok(length)
    }

    fn try_dequeue_mpsc(&self, mpsc: &MpscState, output: &mut [u8]) -> Result<usize, u32> {
        loop {
            let mut best_index = None;
            let mut best_priority = u64::MAX;
            for (index, shard) in mpsc.shards.iter().enumerate() {
                if !shard.has_item.load(AtomicOrdering::Acquire) {
                    continue;
                }
                let priority = shard.min_priority.load(AtomicOrdering::Acquire);
                if best_index.is_none() || priority < best_priority {
                    best_index = Some(index);
                    best_priority = priority;
                }
            }
            let Some(best_index) = best_index else {
                return Err(STATUS_EMPTY);
            };

            let shard_state = &mpsc.shards[best_index];
            let mut shard = shard_state.lock.lock();
            let Some(best) = shard.heap.peek() else {
                shard_state.has_item.store(false, AtomicOrdering::Release);
                shard_state
                    .min_priority
                    .store(u64::MAX, AtomicOrdering::Release);
                continue;
            };
            let actual_priority = best.priority;
            let mut another_lower = false;
            for (index, other) in mpsc.shards.iter().enumerate() {
                if index != best_index
                    && other.has_item.load(AtomicOrdering::Acquire)
                    && other.min_priority.load(AtomicOrdering::Acquire) < actual_priority
                {
                    another_lower = true;
                    break;
                }
            }
            if another_lower {
                continue;
            }
            if best.value.len() > output.len() {
                return Err(STATUS_INVALID);
            }
            let length = best.value.len();
            best.value.copy_to(output);
            shard.heap.pop();
            if let Some(next) = shard.heap.peek() {
                shard_state
                    .min_priority
                    .store(next.priority, AtomicOrdering::Release);
                shard_state.has_item.store(true, AtomicOrdering::Release);
            } else {
                shard_state.has_item.store(false, AtomicOrdering::Release);
            }
            mpsc.occupied.fetch_sub(1, AtomicOrdering::Release);
            return Ok(length);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_core_is_bounded_priority_queue() {
        let queue = Queue::new(3, 8, 1, 1).unwrap();
        let producer = queue.producer(0).unwrap();
        let consumer = queue.consumer(0).unwrap();
        assert_eq!(producer.try_enqueue(7, b"later"), STATUS_OK);
        assert_eq!(producer.try_enqueue(2, b"first"), STATUS_OK);
        assert_eq!(producer.try_enqueue(2, b"second"), STATUS_OK);
        assert_eq!(producer.try_enqueue(0, b"full"), STATUS_FULL);

        let mut output = [0_u8; 8];
        for expected in [b"first".as_slice(), b"second", b"later"] {
            assert_eq!(consumer.try_dequeue(&mut output), Ok(expected.len()));
            assert_eq!(&output[..expected.len()], expected);
        }
        assert_eq!(consumer.try_dequeue(&mut output), Err(STATUS_EMPTY));
    }

    #[test]
    fn mpsc_shards_are_bounded_and_globally_prioritized() {
        let queue = Queue::new(32, 16, 4, 1).unwrap();
        let producers: Vec<_> = (0..4).map(|id| queue.producer(id).unwrap()).collect();
        let mut threads = Vec::new();
        for (producer_id, producer) in producers.into_iter().enumerate() {
            threads.push(std::thread::spawn(move || {
                for item in 0..8_u64 {
                    let value = [producer_id as u8, item as u8];
                    assert_eq!(producer.try_enqueue(8 - item, &value), STATUS_OK);
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }

        let consumer = queue.consumer(0).unwrap();
        let mut output = [0_u8; 2];
        let mut previous_priority = 0;
        for index in 0..32 {
            let result = consumer.try_dequeue(&mut output);
            assert_eq!(result, Ok(2));
            let priority = 8 - output[1] as u64;
            if index > 0 {
                assert!(priority >= previous_priority);
            }
            previous_priority = priority;
        }
        assert_eq!(consumer.try_dequeue(&mut output), Err(STATUS_EMPTY));
        assert_eq!(
            queue
                .state
                .mpsc
                .as_ref()
                .unwrap()
                .occupied
                .load(AtomicOrdering::Acquire),
            0
        );
    }

    #[test]
    fn mpsc_accepts_maximum_priority() {
        let queue = Queue::new(2, 1, 2, 1).unwrap();
        let producer = queue.producer(0).unwrap();
        let consumer = queue.consumer(0).unwrap();
        assert_eq!(producer.try_enqueue(u64::MAX, &[7]), STATUS_OK);
        let mut output = [0_u8; 1];
        assert_eq!(consumer.try_dequeue(&mut output), Ok(1));
        assert_eq!(output, [7]);
    }
}
