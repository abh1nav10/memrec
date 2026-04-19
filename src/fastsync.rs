#![allow(dead_code)]

use crossbeam_utils::CachePadded;
use std::alloc::{self, Layout};
use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering, fence};

const EMPTY: u8 = 0;
const FULL: u8 = 1;

thread_local! {
    static COUNTER: Cell<usize> = const { Cell::new(0) };
}

/// Stores a pointer to the allocation.
/// Gets dropped when all the senders and the receiver is dropped.
///
/// NUM_BATCH must be a power of two.
#[repr(C)]
struct Data<const NUM_BATCH: usize, const BATCH_SIZE: usize, T> {
    /// Since these flags are not CachePadded, in a highly contentious scenario, all cores will be
    /// hammering the same cacheline and performance will degrade very heavily. However, in case of
    /// COMPIO, since we are optimizing for the case where the queue is mostly empty as cross-thread
    /// wakeups are rare, this will make it pretty easy for the executor to check which slots are
    /// ready, as all flags can be loaded on the same cache line and the check can happen with a
    /// `Relaxed` load. Also, since the executor checks the queue in every iteration, the flag will
    /// stay hot in one of the caches which will make it even faster. Also, the executor only checks
    /// the flag instead of checking the offset field of Slot which reduces the snatching away of
    /// the cacheline by the executor from the Exclusive state to Shared state when the threads are about
    /// to write to it.
    flags: [AtomicU8; NUM_BATCH],
    slot_ptrs: NonNull<Slot<BATCH_SIZE, T>>,
}

unsafe impl<const N: usize, const B: usize, T: Send> Send for Data<N, B, T> {}
unsafe impl<const N: usize, const B: usize, T: Send> Sync for Data<N, B, T> {}

impl<const N: usize, const B: usize, T> Drop for Data<N, B, T> {
    fn drop(&mut self) {
        let ptr = self.slot_ptrs.as_ptr();
        (0..N).for_each(|i| {
            let slot = unsafe { &(*ptr.add(i)) };

            let ptr = slot.ptr.as_ptr();
            let num_occupied = slot.counter.load(Ordering::Acquire);
            (0..num_occupied).for_each(|i| unsafe {
                let offset = ptr.add(i);
                (*offset).assume_init_drop();
            });

            let ptr = ptr as *mut u8;
            let layout =
                Layout::array::<MaybeUninit<T>>(B).expect("Batch size is less than isize::MAX");
            unsafe { alloc::dealloc(ptr, layout) };
        });

        let ptr = ptr as *mut u8;
        let layout =
            Layout::array::<Slot<B, T>>(N).expect("Number of batches is less than isize::MAX");

        unsafe { alloc::dealloc(ptr, layout) };
    }
}

/// Sender is both `Send` and `Sync`. It is also cloneable.
pub struct Sender<const N: usize, const B: usize, T> {
    data: Arc<Data<N, B, T>>,
}

impl<const N: usize, const B: usize, T> Clone for Sender<N, B, T> {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
        }
    }
}

/// Receiver is `Send` but not `Sync`. It also does not implement the `Clone` trait.
pub struct Receiver<const N: usize, const B: usize, T> {
    data: Arc<Data<N, B, T>>,
    /// This field will only be accessed by the consumer thread that pops elements out. It has been
    /// added to ensure better cache locality.
    iteration: Cell<bool>,
}

impl<const N: usize, const B: usize, T> Receiver<N, B, T> {
    pub fn drain_with(&self, mut f: impl FnMut(T)) {
        // We have to pop the elements, and also reset the offset of the batches that we drain, back
        // to zero.
        let ptrs = self.data.slot_ptrs.as_ptr();

        let current = self.iteration.get();
        self.iteration.set(!current);

        // Since `Slot` is greater than the size of a cache line, and the executor checks all of
        // them for readiness, by the time it reaches the last Slot, the initial slots that it
        // checked might not be present in any of the caches. Checking in the reverse order of the
        // previous iteration offers better temporal cache locality.
        if current {
            self.data.flags.iter().enumerate().for_each(|(i, flag)| {
                if flag.load(Ordering::Relaxed) == FULL {
                    fence(Ordering::Acquire);

                    let slot = unsafe { &(*ptrs.add(i)) };
                    let ptr = slot.ptr.as_ptr();

                    (0..B).for_each(|j| {
                        let value = unsafe { (*ptr.add(j)).assume_init_read() };
                        f(value);
                    });

                    self.data.flags[i].store(EMPTY, Ordering::Relaxed);
                    // Resetting sets the offset back to 0 with a Release Ordering in order to
                    // ensure that the operations prior to the resetting are not reordered to
                    // happen after it.
                    slot.reset();
                }
            });
        } else {
            self.data
                .flags
                .iter()
                .enumerate()
                .rev()
                .for_each(|(i, flag)| {
                    if flag.load(Ordering::Relaxed) == FULL {
                        fence(Ordering::Acquire);

                        let slot = unsafe { &(*ptrs.add(i)) };
                        let ptr = slot.ptr.as_ptr();

                        (0..B).for_each(|j| {
                            let value = unsafe { (*ptr.add(j)).assume_init_read() };
                            f(value);
                        });

                        self.data.flags[i].store(EMPTY, Ordering::Relaxed);
                        slot.reset();
                    }
                });
        }
    }
}

impl<const NUM_BATCH: usize, const BATCH_SIZE: usize, T> Sender<NUM_BATCH, BATCH_SIZE, T> {
    pub fn push(&self, mut value: T) -> OpResult<T> {
        let ptrs = self.data.slot_ptrs.as_ptr();
        let current = COUNTER.get();

        let mask = NUM_BATCH - 1;
        let mut new = (current + 1) & mask;
        COUNTER.set((new + 1) & mask);

        while new != current {
            let slot = unsafe { &(*ptrs.add(new)) };

            match slot.push(value) {
                OpResult::Success => return OpResult::Success,
                OpResult::BackPressure(v) => {
                    value = v;
                }
                OpResult::Completed => {
                    self.data.flags[new].store(FULL, Ordering::Release);
                    return OpResult::Success;
                }
            }
            new = (new + 1) & mask;
        }
        OpResult::BackPressure(value)
    }
}

pub fn mpsc<const NUM_BATCH: usize, const BATCH_SIZE: usize, T>() -> (
    Sender<NUM_BATCH, BATCH_SIZE, T>,
    Receiver<NUM_BATCH, BATCH_SIZE, T>,
) {
    assert!(NUM_BATCH < isize::MAX as usize);
    assert!(NUM_BATCH.is_power_of_two());

    let layout =
        Layout::array::<Slot<BATCH_SIZE, T>>(NUM_BATCH).expect("NUM_BATCH is less than isize::MAX");
    let ptr = unsafe { alloc::alloc(layout) };

    let ptr = ptr as *mut Slot<BATCH_SIZE, T>;

    (0..NUM_BATCH).for_each(|i| {
        let slot = Slot::<BATCH_SIZE, T>::new();
        unsafe {
            let offset = ptr.add(i);
            offset.write(slot);
        }
    });

    let ptr = match NonNull::new(ptr) {
        Some(ptr) => ptr,
        None => alloc::handle_alloc_error(layout),
    };

    let flags: [AtomicU8; NUM_BATCH] = std::array::from_fn(|_| AtomicU8::new(EMPTY));

    let data = Arc::new(Data {
        slot_ptrs: ptr,
        flags,
    });

    let sender = Sender {
        data: Arc::clone(&data),
    };

    let receiver = Receiver {
        data,
        iteration: Cell::new(false),
    };
    (sender, receiver)
}

/// `SIZE` referes to the size of the allocation.
struct Slot<const SIZE: usize, T> {
    /// Pointer to the start of the allocation.
    /// Not cachepadding this to reduce the amount of time taken by the executor to go through the
    /// elements when the slot is full as more elements can be on a cacheline.
    ptr: NonNull<MaybeUninit<T>>,

    /// The current offset into the allocation.
    offset: CachePadded<AtomicUsize>,

    /// The threads update this flag with an AcqRel ordering in order to ensure that we establish a
    /// transitive happens before relationship with all prior writes.
    counter: CachePadded<AtomicUsize>,
}

#[derive(Debug)]
pub enum OpResult<T> {
    Success,
    BackPressure(T),
    Completed,
}

impl<const SIZE: usize, T> Slot<SIZE, T> {
    fn new() -> Self {
        assert!(size_of::<T>() > 0, "Zero sized types are not supported!");
        assert!(SIZE < isize::MAX as usize);

        let layout = Layout::array::<MaybeUninit<T>>(SIZE).expect("Size if less than `isize::MAX`");
        let ptr = unsafe { alloc::alloc(layout) };

        let ptr = match NonNull::new(ptr as *mut MaybeUninit<T>) {
            Some(ptr) => ptr,
            None => alloc::handle_alloc_error(layout),
        };

        Self {
            ptr,
            offset: CachePadded::new(AtomicUsize::new(0)),
            counter: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    fn push(&self, value: T) -> OpResult<T> {
        let mut current = self.offset.load(Ordering::Relaxed);
        // The offset can never go beyond SIZE.
        if current == SIZE {
            return OpResult::BackPressure(value);
        }
        loop {
            match self.offset.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let ptr = self.ptr.as_ptr();

                    unsafe {
                        let offset = ptr.add(current);
                        (*offset).write(value);
                    };

                    // This increment is necessary because this is what helps establish a happens
                    // before relationship with the writes by other threads.
                    self.counter.fetch_add(1, Ordering::AcqRel);

                    if current == (SIZE - 1) {
                        // We store with a Release flag because that ensures that when the executor
                        // loads and sees ready, it applies an Acquire fence which establishes a
                        // happens before relationship with this Release store and since the
                        // fetch_add previously cannot be reordered to happen after this store
                        // because of the Release Ordering, the executor are guaranteed to see all prior
                        // writes.
                        break OpResult::Completed;
                    }
                    break OpResult::Success;
                }
                Err(n) => {
                    if n >= SIZE {
                        break OpResult::BackPressure(value);
                    } else {
                        current = n;
                    }
                }
            }
        }
    }

    fn reset(&self) {
        self.counter.store(0, Ordering::Relaxed);
        // Ordering::Release because we have to ensure that none of the draining is reordered to
        // happen after the store to the index.
        self.offset.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::mpsc;
    use std::sync::{Arc, Barrier};
    use std::time::Instant;

    // Criterion benchmarks in the benches directory.
    #[test]
    fn measure_fastsync() {
        let (tx, rx) = mpsc::<32, 2, _>();
        let barrier = Arc::new(Barrier::new(6));

        let handles = (0..5)
            .map(|_| {
                let cloned = tx.clone();
                let barrier = Arc::clone(&barrier);

                std::thread::spawn(move || {
                    barrier.wait();

                    for i in 0..4 {
                        let _ = cloned.push(i);
                    }
                })
            })
            .collect::<Vec<_>>();

        let time = Instant::now();

        barrier.wait();

        let mut count = 0;
        for _ in 0..8 {
            rx.drain_with(|_| count += 1);
        }

        let elapsed = time.elapsed().as_micros();

        println!("{elapsed}");

        println!("{count}");
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn measure_arrayqueue() {
        let queue = Arc::new(crossbeam::queue::ArrayQueue::new(64));
        let barrier = Arc::new(Barrier::new(6));

        let handles = (0..5)
            .map(|_| {
                let cloned = Arc::clone(&queue);
                let barrier = Arc::clone(&barrier);

                std::thread::spawn(move || {
                    barrier.wait();

                    for i in 0..4 {
                        let _ = cloned.push(i);
                    }
                })
            })
            .collect::<Vec<_>>();

        let time = Instant::now();

        barrier.wait();

        let mut count = 0;
        for _ in 0..8 {
            while queue.pop().is_some() {
                count += 1;
            }
        }

        let elapsed = time.elapsed().as_micros();

        println!("{elapsed}");

        println!("{count}");
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
