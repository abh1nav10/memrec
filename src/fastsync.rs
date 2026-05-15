#![allow(dead_code)]

use crossbeam_utils::CachePadded;
use std::alloc::{self, Layout};
use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};

const EMPTY: u8 = 0;
const FULL: u8 = 1;

thread_local! {
    static COUNTER: Cell<usize> = const { Cell::new(0) };
}

/// Stores a pointer to the allocation.
/// Gets dropped when all the senders and the receiver is dropped.
#[derive(Debug)]
#[repr(C)]
struct Data<T> {
    flag: AtomicU64,
    slot_ptr: NonNull<Slot<T>>,
    size: u8,
}

unsafe impl<T: Send> Send for Data<T> {}
unsafe impl<T: Send> Sync for Data<T> {}

impl<T> Drop for Data<T> {
    fn drop(&mut self) {
        let ptr = self.slot_ptr.as_ptr();
        (0..self.size).for_each(|i| {
            let slot = unsafe { &(*ptr.add(i as usize)) };
            let ptr = slot.ptr.as_ptr();
            let num_occupied = slot.counter.load(Ordering::Acquire);
            (0..num_occupied).for_each(|i| unsafe {
                let offset = ptr.add(i);
                (*offset).assume_init_drop();
            });

            let ptr = ptr as *mut u8;
            let layout = Layout::array::<MaybeUninit<T>>(slot.size)
                .expect("Batch size is less than isize::MAX");
            unsafe { alloc::dealloc(ptr, layout) };
        });

        let ptr = ptr as *mut u8;
        let layout = Layout::array::<Slot<T>>(self.size as usize)
            .expect("Number of batches is less than isize::MAX");
        unsafe { alloc::dealloc(ptr, layout) };
    }
}

/// Sender is both `Send` and `Sync`. It is also cloneable.
#[derive(Debug)]
pub struct Sender<T> {
    data: Arc<Data<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
        }
    }
}

/// Receiver is `Send` but not `Sync`. It also does not implement the `Clone` trait.
#[derive(Debug)]
pub struct Receiver<T> {
    data: Arc<Data<T>>,
}

impl<T> Receiver<T> {
    pub fn drain_with(&self, mut f: impl FnMut(T)) {
        // We have to pop the elements, and also reset the offset of the batches that we drain, back
        // to zero.
        let mut val = self.data.flag.load(Ordering::Relaxed);
        if val != 0 {
            // Acquire the writes to the slots that we just observed to be full.
            fence(Ordering::Acquire);

            let ptrs = self.data.slot_ptr.as_ptr();
            while val != 0 {
                let index = val.trailing_zeros();
                let slot = unsafe { &(*ptrs.add(index as usize)) };
                let ptr = slot.ptr.as_ptr();
                (0..slot.size).for_each(|j| {
                    let value = unsafe { (*ptr.add(j)).assume_init_read() };
                    f(value);
                });
                self.data.flag.fetch_and(!(1 << index), Ordering::Relaxed);
                slot.reset();
                val &= !(1 << index);
            }
        }
    }
}

impl<T> Sender<T> {
    pub fn push(&self, mut value: T) -> OpResult<T> {
        let ptrs = self.data.slot_ptr.as_ptr();
        let current = COUNTER.get();

        let mask = (self.data.size - 1) as usize;
        let mut new = (current + 1) & mask;
        COUNTER.set(new & mask);

        while new != current {
            let slot = unsafe { &(*ptrs.add(new)) };

            match slot.push(value) {
                OpResult::Success => return OpResult::Success,
                OpResult::BackPressure(v) => {
                    value = v;
                }
                OpResult::Completed => {
                    // We store with a Release flag because that ensures that when the executor
                    // loads and sees ready, it applies an Acquire fence which establishes a
                    // happens before relationship with this Release store and since the
                    // fetch_add previously in `Slot::push` cannot be reordered to happen after this store
                    // because of the Release Ordering, the executor are guaranteed to see all prior
                    // writes.
                    self.data.flag.fetch_or(1 << new, Ordering::Release);
                    return OpResult::Success;
                }
            }
            new = (new + 1) & mask;
        }
        OpResult::BackPressure(value)
    }
}

pub fn mpsc<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    assert!(size.is_power_of_two(), "Size must be a power of two.");
    assert!(
        size < isize::MAX as usize,
        "Size cannot be greater than isize::MAX."
    );
    let (batch, batch_size): (u8, usize) = {
        let val = size / 64;
        if val > 0 { (64, val) } else { (size as u8, 1) }
    };

    let layout = Layout::array::<Slot<T>>(size).expect("NUM_BATCH is less than isize::MAX");
    let ptr = unsafe { alloc::alloc(layout) };

    let ptr = ptr as *mut Slot<T>;
    (0..batch).for_each(|i| {
        let slot = Slot::<T>::new(batch_size);
        unsafe {
            let offset = ptr.add(i as usize);
            offset.write(slot);
        }
    });

    let ptr = match NonNull::new(ptr) {
        Some(ptr) => ptr,
        None => alloc::handle_alloc_error(layout),
    };

    let flag = AtomicU64::new(0);
    let data = Arc::new(Data {
        slot_ptr: ptr,
        flag,
        size: batch,
    });

    let sender = Sender {
        data: Arc::clone(&data),
    };
    let receiver = Receiver { data };
    (sender, receiver)
}

/// `SIZE` referes to the size of the allocation.
struct Slot<T> {
    /// Pointer to the start of the allocation.
    /// Not cachepadding this to reduce the amount of time taken by the executor to go through the
    /// elements when the slot is full as more elements can be on a cacheline.
    ptr: NonNull<MaybeUninit<T>>,

    /// The current offset into the allocation.
    offset: CachePadded<AtomicUsize>,

    /// The threads update this flag with an AcqRel ordering in order to ensure that we establish a
    /// transitive happens before relationship with all prior writes.
    counter: CachePadded<AtomicUsize>,
    size: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub enum OpResult<T> {
    Success,
    BackPressure(T),
    Completed,
}

impl<T> Slot<T> {
    fn new(size: usize) -> Self {
        assert!(size_of::<T>() > 0, "Zero sized types are not supported!");
        assert!(size < isize::MAX as usize);

        let layout = Layout::array::<MaybeUninit<T>>(size).expect("Size if less than `isize::MAX`");
        let ptr = unsafe { alloc::alloc(layout) };

        let ptr = match NonNull::new(ptr as *mut MaybeUninit<T>) {
            Some(ptr) => ptr,
            None => alloc::handle_alloc_error(layout),
        };

        Self {
            ptr,
            offset: CachePadded::new(AtomicUsize::new(0)),
            counter: CachePadded::new(AtomicUsize::new(0)),
            size,
        }
    }

    fn push(&self, value: T) -> OpResult<T> {
        // We must use `Acquire` here to make sure that we establish a happens before with the drainer
        // thread resetting the slot.
        let mut current = self.offset.load(Ordering::Acquire);
        // The offset can never go beyond SIZE.
        if current == self.size {
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
                    if current == (self.size - 1) {
                        break OpResult::Completed;
                    }
                    break OpResult::Success;
                }
                Err(n) => {
                    if n >= self.size {
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
    use super::*;

    #[test]
    fn check() {
        let (tx, rx) = mpsc(64);

        (0..64).for_each(|i| {
            let res = tx.push(i);
            assert_eq!(res, OpResult::Success);
        });

        let res = tx.push(42);
        assert_eq!(res, OpResult::BackPressure(42));

        let mut count = 0;
        rx.drain_with(|_| {
            count += 1;
        });
        assert_eq!(count, 64);
    }
}
