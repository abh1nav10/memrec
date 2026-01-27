#![allow(unused)]

use crate::markers::First;
use crate::{Holder, Provide, Provider, Registry};
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

struct Node<T> {
    value: MaybeUninit<T>,
    next: AtomicPtr<Provider<Node<T>, First>>,
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Node<T> {
    const fn new() -> Self {
        Self {
            value: MaybeUninit::uninit(),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }

    fn write(&mut self, value: T) {
        self.value.write(value);
    }
}

pub struct Queue<T> {
    head: AtomicPtr<Provider<Node<T>, First>>,
    tail: AtomicPtr<Provider<Node<T>, First>>,
    registry: Arc<Registry<First>>,
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        let registry = Arc::new(Registry::<First>::new());
        let node = Node::new();
        let sentinel = Provider::new(node, Arc::clone(&registry));
        let raw = Box::into_raw(Box::new(sentinel));
        Self {
            head: AtomicPtr::new(raw),
            tail: AtomicPtr::new(raw),
            registry,
        }
    }

    pub fn enqueue(&self, value: T) {
        let mut node = Node::new();
        node.write(value);
        let provider = Provider::new(node, Arc::clone(&self.registry));

        let boxed = Box::into_raw(Box::new(provider));
        loop {
            let mut tail_holder = Holder::with_registry(&self.registry);
            let guard =
                unsafe { tail_holder.load(&self.tail) }.expect("Sentinel node is always present");

            let mut tail_next_holder = Holder::with_registry(&self.registry);
            let next_ptr_of_tail = unsafe { tail_next_holder.load(&guard.next) };
            let ptr = guard.deref() as *const Provider<_, _> as *mut Provider<_, _>;
            if let Some(g) = next_ptr_of_tail {
                let next_ptr = g.deref() as *const Provider<_, _> as *mut Provider<_, _>;
                let _ = self.tail.compare_exchange_weak(
                    ptr,
                    next_ptr,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                continue;
            } else if guard
                .next
                .compare_exchange_weak(ptr::null_mut(), boxed, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let _ =
                    self.tail
                        .compare_exchange_weak(ptr, boxed, Ordering::SeqCst, Ordering::SeqCst);
                break;
            }
        }
    }

    pub fn dequeue(&self) -> Option<T> {
        loop {
            let mut head_holder = Holder::with_registry(&self.registry);
            let head_guard =
                unsafe { head_holder.load(&self.head) }.expect("Sentinel node is always there");
            let current_head_ptr =
                head_guard.deref() as *const Provider<_, _> as *mut Provider<_, _>;
            let mut head_next_holder = Holder::with_registry(&self.registry);
            let head_next_guard = unsafe { head_next_holder.load(&head_guard.next) };

            if let Some(guard) = head_next_guard {
                let new_tail_head_ptr =
                    guard.deref() as *const Provider<_, _> as *mut Provider<_, _>;
                let mut tail_guard = Holder::with_registry(&self.registry);
                let tail_guard =
                    unsafe { tail_guard.load(&self.tail) }.expect("Sentinel node is always there");
                let current_tail_ptr =
                    tail_guard.deref() as *const Provider<_, _> as *mut Provider<_, _>;

                // Shared references cannot be compares until the pointee implements PartialEq!
                if current_head_ptr == current_tail_ptr {
                    let _ = self.tail.compare_exchange_weak(
                        current_tail_ptr,
                        new_tail_head_ptr,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                    continue;
                } else {
                    if self
                        .head
                        .compare_exchange_weak(
                            current_head_ptr,
                            new_tail_head_ptr,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        // Cannot dereference and borrow as mutable since Provider has to implement DerefMut!
                        // MaybeUninit uses ManuallyDrop, therefore assume_init_read is fine even
                        // though it creates a bitiwise copy of the value. The caller can drop the
                        // value but, the danger of double drop is prevented due to MaybeUninit
                        // using Manuallydrop inside of it.
                        let read = unsafe { ptr::read(&guard.value) };
                        let ret = unsafe { read.assume_init_read() };
                        current_head_ptr.retire();
                        // forgetting is not required because MaybeUninit implements the Copy trait!
                        // So its a NOOP
                        // std::mem::forget(read);
                        break Some(ret);
                    }
                }
            } else {
                break None;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        // We load it into a hazard pointer because dereferencing it just like that might race with
        // another allocation happening at the same memory slot.
        let mut holder = Holder::with_registry(&self.registry);
        let head = unsafe { holder.load(&self.head) }.expect("Sentinel node is always there");
        head.next.load(Ordering::Relaxed).is_null()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_threaded() {
        let queue = Queue::<u32>::new();
        assert!(queue.is_empty());

        queue.enqueue(87);
        assert!(!queue.is_empty());

        let val = queue.dequeue();
        assert_eq!(val, Some(87));
        assert!(queue.is_empty());
    }

    #[test]
    fn multi_threaded() {
        let queue = Arc::new(Queue::<u32>::new());
        queue.enqueue(8);
        let handles = (0..100)
            .map(|e| {
                let cloned = Arc::clone(&queue);
                if e % 2 == 0 {
                    std::thread::spawn(move || {
                        cloned.enqueue(e);
                    })
                } else {
                    std::thread::spawn(move || {
                        let _ = cloned.dequeue();
                    })
                }
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(!queue.is_empty());
    }
}
