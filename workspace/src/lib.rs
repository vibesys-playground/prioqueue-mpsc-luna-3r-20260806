#![deny(unsafe_op_in_unsafe_fn)]

mod ffi;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

const ABI_VERSION: u32 = 1;
const STATUS_OK: u32 = 0;
const STATUS_FULL: u32 = 1;
const STATUS_EMPTY: u32 = 2;
const STATUS_INVALID: u32 = 3;
const STATUS_INTERNAL_ERROR: u32 = 4;

struct Entry {
    priority: u64,
    sequence: u64,
    value: Vec<u8>,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
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
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

struct Values {
    next_sequence: u64,
    heap: BinaryHeap<Entry>,
}

struct Shard {
    heap: BinaryHeap<Entry>,
}

struct MpscState {
    capacity: usize,
    // Reservations are made before copying the caller's value.  They make
    // admission bounded without putting producers behind a common mutex.
    occupied: AtomicUsize,
    next_sequence: AtomicU64,
    shards: Vec<Mutex<Shard>>,
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
                shards.push(Mutex::new(Shard { heap }));
            }
            Some(Arc::new(MpscState {
                capacity,
                occupied: AtomicUsize::new(0),
                next_sequence: AtomicU64::new(0),
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
                    next_sequence: 0,
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
        let mut value = Vec::new();
        if value.try_reserve_exact(data.len()).is_err() {
            return STATUS_INTERNAL_ERROR;
        }
        value.extend_from_slice(data);
        let sequence = values.next_sequence;
        values.next_sequence = values.next_sequence.wrapping_add(1);
        values.heap.push(Entry {
            priority,
            sequence,
            value,
        });
        STATUS_OK
    }

    fn try_enqueue_mpsc(&self, mpsc: &MpscState, priority: u64, data: &[u8]) -> u32 {
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

        let mut value = Vec::new();
        if value.try_reserve_exact(data.len()).is_err() {
            mpsc.occupied.fetch_sub(1, AtomicOrdering::Release);
            return STATUS_INTERNAL_ERROR;
        }
        value.extend_from_slice(data);
        let sequence = mpsc.next_sequence.fetch_add(1, AtomicOrdering::Relaxed);
        let Ok(mut shard) = mpsc.shards[self.shard_id].lock() else {
            mpsc.occupied.fetch_sub(1, AtomicOrdering::Release);
            return STATUS_INTERNAL_ERROR;
        };
        if shard.heap.try_reserve(1).is_err() {
            mpsc.occupied.fetch_sub(1, AtomicOrdering::Release);
            return STATUS_INTERNAL_ERROR;
        }
        shard.heap.push(Entry {
            priority,
            sequence,
            value,
        });
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
        output[..entry.value.len()].copy_from_slice(&entry.value);
        let length = entry.value.len();
        values.heap.pop();
        Ok(length)
    }

    fn try_dequeue_mpsc(&self, mpsc: &MpscState, output: &mut [u8]) -> Result<usize, u32> {
        // Taking locks in shard order gives the consumer a consistent view;
        // producers never hold more than their own shard lock.
        let mut shards = Vec::with_capacity(mpsc.shards.len());
        for shard in &mpsc.shards {
            let Ok(shard) = shard.lock() else {
                return Err(STATUS_INTERNAL_ERROR);
            };
            shards.push(shard);
        }
        let Some((best_index, best)) = shards
            .iter()
            .enumerate()
            .filter_map(|(index, shard)| shard.heap.peek().map(|entry| (index, entry)))
            .min_by(|(_, left), (_, right)| {
                left.priority
                    .cmp(&right.priority)
                    .then_with(|| left.sequence.cmp(&right.sequence))
            })
        else {
            return Err(STATUS_EMPTY);
        };
        if best.value.len() > output.len() {
            return Err(STATUS_INVALID);
        }
        output[..best.value.len()].copy_from_slice(&best.value);
        let length = best.value.len();
        shards[best_index].heap.pop();
        mpsc.occupied.fetch_sub(1, AtomicOrdering::Release);
        Ok(length)
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
}
