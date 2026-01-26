#![allow(unused)]

use crate::{First, Guard, Holder, Provider, Registry};
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
        todo!()
    }

    pub fn is_empty(&self) -> bool {
        // We load it into a hazard pointer because dereferencing it just like that might race with
        // another allocation happening at the same memory slot.
        let mut holder = Holder::with_registry(&self.registry);
        let head = unsafe { holder.load(&self.head) }.expect("Sentinel node is always there");
        head.next.load(Ordering::Relaxed).is_null()
    }
}
