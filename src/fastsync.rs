#![allow(dead_code)]

use crossbeam_utils::CachePadded;
use std::alloc::{self, Layout};
use std::cell::Cell;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};

thread_local! {
    static COUNTER: Cell<usize> = const { Cell::new(0) };
}

/// A data structure with batch receiving.
///
/// NUM_BATCH must be a power of two.
pub struct FastSync<const NUM_BATCH: usize, const BATCH_SIZE: usize, T> {
    slot_ptrs: NonNull<Slot<BATCH_SIZE, T>>,
}

unsafe impl<const N: usize, const B: usize, T: Send> Send for FastSync<N, B, T> {}
unsafe impl<const N: usize, const B: usize, T: Send> Sync for FastSync<N, B, T> {}

impl<const NUM_BATCH: usize, const BATCH_SIZE: usize, T> Default
    for FastSync<NUM_BATCH, BATCH_SIZE, T>
where
    T: Copy,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<const NUM_BATCH: usize, const BATCH_SIZE: usize, T> FastSync<NUM_BATCH, BATCH_SIZE, T>
where
    T: Copy,
{
    pub fn new() -> Self {
        assert!(NUM_BATCH < isize::MAX as usize);

        let layout = Layout::array::<Slot<BATCH_SIZE, T>>(NUM_BATCH)
            .expect("NUM_BATCH is less than isize::MAX");
        let ptr = unsafe { alloc::alloc(layout) };

        let ptr = ptr as *mut Slot<BATCH_SIZE, T>;

        for i in 0..NUM_BATCH {
            let slot = Slot::<BATCH_SIZE, T>::new();
            unsafe {
                let offset = ptr.add(i);
                offset.write(slot);
            }
        }

        let ptr = match NonNull::new(ptr) {
            Some(ptr) => ptr,
            None => alloc::handle_alloc_error(layout),
        };

        Self { slot_ptrs: ptr }
    }

    pub fn push(&self, value: T) -> OpResult<T> {
        let ptrs = self.slot_ptrs.as_ptr();
        let current = COUNTER.get();

        let mask = NUM_BATCH - 1;
        let mut new = (current + 1) & mask;
        COUNTER.set((new + 3) & mask);

        while new != current {
            let slot = unsafe { &(*ptrs.add(new)) };

            match slot.push(value) {
                OpResult::Success => return OpResult::Success,
                OpResult::BackPressure(_) => {}
            }
            new = (new + 1) & mask;
        }
        OpResult::BackPressure(value)
    }

    pub fn drain_with(&self, mut f: impl FnMut(T)) {
        // We have to pop the elements, and also reset the offset of the batches that we drain, back
        // to zero.
        let ptrs = self.slot_ptrs.as_ptr();
        for i in 0..NUM_BATCH {
            let slot = unsafe { &(*ptrs.add(i)) };

            if slot.is_ready() {
                // Acquire fence to ensure that we get to read what was written.
                fence(Ordering::Acquire);

                let ptr = slot.ptr.as_ptr();

                for j in 0..BATCH_SIZE {
                    // T is Copy
                    let value = unsafe { *(ptr.add(j)) };
                    f(value);
                }
                slot.reset();
            }
        }
    }
}

/// `SIZE` referes to the size of the allocation.
struct Slot<const SIZE: usize, T> {
    /// Pointer to the start of the allocation.
    /// We do not use CachePadded for T because this data structure is being designed to be used in
    /// a way where there are multiple batches to spread contention but the size of the batch is not
    /// so high and therefore cachepadding will only come at the cost of delayed memory accesses and
    /// nothing else.
    ptr: NonNull<T>,

    /// The current offset into the allocation.
    offset: CachePadded<AtomicUsize>,

    /// The threads update this flag with an AcqRel ordering in order to ensure that we establish a
    /// transitive happens before relationship with all prior writes.
    counter: CachePadded<AtomicUsize>,

    /// Flag to signal that the batch is ready to be recieved. We use this flag in order to avoid
    /// any form of contention on the offset by the executor if the corresponding flag is not ready
    /// to be received.
    flag: CachePadded<AtomicBool>,
}

#[derive(Debug)]
pub enum OpResult<T> {
    Success,
    BackPressure(T),
}

impl<const SIZE: usize, T> Slot<SIZE, T> {
    fn new() -> Self {
        assert!(size_of::<T>() > 0, "Zero sized types are not supported!");
        assert!(SIZE < isize::MAX as usize);

        let layout = Layout::array::<T>(SIZE).expect("Size if less than `isize::MAX`");
        let ptr = unsafe { alloc::alloc(layout) };

        let ptr = match NonNull::new(ptr as *mut T) {
            Some(ptr) => ptr,
            None => alloc::handle_alloc_error(layout),
        };

        Self {
            ptr,
            offset: CachePadded::new(AtomicUsize::new(0)),
            counter: CachePadded::new(AtomicUsize::new(0)),
            flag: CachePadded::new(AtomicBool::new(false)),
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
                        ptr::write(offset, value);
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
                        self.flag.store(true, Ordering::Release);
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

    fn is_ready(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.flag.store(false, Ordering::Relaxed);
        // Ordering::Release because we have to ensure that none of the draining is reordered to
        // happen after the store to the index.
        self.offset.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn multi() {
        let fastsync = Arc::new(FastSync::<5, 5, _>::new());

        let handles = (0..5)
            .map(|_| {
                let cloned = Arc::clone(&fastsync);
                std::thread::spawn(move || {
                    for i in 0..4 {
                        cloned.push(i);
                    }
                })
            })
            .collect::<Vec<_>>();

        for _ in 0..4 {
            fastsync.drain_with(|n| println!("{n}"));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    use std::sync::Barrier;
    use std::time::Instant;
    #[test]
    fn measure_fastsync() {
        let fastsync = Arc::new(FastSync::<32, 2, _>::new());
        let barrier = Arc::new(Barrier::new(6));

        let handles = (0..5)
            .map(|_| {
                let cloned = Arc::clone(&fastsync);
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
            fastsync.drain_with(|_| count += 1);
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
        let queue = Arc::new(crossbeam_queue::ArrayQueue::new(64));
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
