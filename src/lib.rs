// Using Ordering::SeqCst everywhere for making it easy to proove the correctness
// of the algorithm under the Rust memory model gurantees

#![allow(unused)]
#![feature(arbitrary_self_types_pointers)]

// Dont allow the users to construct this type
#[non_exhaustive]
struct Global;

/// This module contains some types that which I have used to provide an
/// implementation of [`Provide`] trait for [`Provider`].
///
/// The user is free to create his own type, and then implement the [`Provide`] trait
/// for that type with as many types that he wants. That would allow the construction
/// of those many domains while at the same time guaranteeing the safety to some extent
/// as long as the caller does create many instances of the [`Registry`] for the same
/// type.
pub mod markers {
    #[non_exhaustive]
    pub struct First;
    #[non_exhaustive]
    pub struct Second;
    #[non_exhaustive]
    pub struct Third;
    #[non_exhaustive]
    pub struct Fourth;
    #[non_exhaustive]
    pub struct Fifth;
}
use markers::{Fifth, First, Fourth, Second, Third};

use std::cell::Cell;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

static GLOBAL_REGISTRY: Registry<Global> = Registry::new();

/// This type is generic over S simply for the purpose of providing compile-time checks
/// to prevent the caller from misusing the API and accidently retiring a pointer a different
/// registry from which it was loaded into.
/// The caller must ensure that no two instances of [`Registry`] are created for the same type.
/// Abiding by this rule guarantees that the aforementioned will be prevent at compile-time itself.
pub struct Registry<S> {
    hazptrs: HazardList,
    retired: RetiredList,
    marker: PhantomData<S>,
}

impl<S> Default for Registry<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Registry<S> {
    pub const fn new() -> Self {
        Self {
            hazptrs: HazardList::new(),
            retired: RetiredList::new(),
            marker: PhantomData,
        }
    }

    fn acquire_hazptr(&self) -> &HazPointer {
        self.hazptrs.acquire()
    }

    pub fn retire_ptr(&self, ptr: *mut (), deleter: fn(*mut ())) {
        let retired = Retired {
            next: Cell::new(std::ptr::null_mut()),
            ptr,
            deleter,
        };
        let boxed = Box::into_raw(Box::new(retired));
        loop {
            let mut head = self.retired.head.load(Ordering::SeqCst);
            unsafe {
                (*boxed).next.set(head);
            }
            if self
                .retired
                .head
                .compare_exchange_weak(head, boxed, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.reclaim_memory();
            }
        }
    }

    fn reclaim_memory(&self) -> usize {
        let mut set = HashSet::new();
        let mut haz_head = self.hazptrs.head.load(Ordering::SeqCst);

        while !haz_head.is_null() {
            let deref_head = unsafe { &(*haz_head) };
            set.insert(deref_head.ptr.load(Ordering::SeqCst));
            haz_head = deref_head.next.get();
        }

        let mut left_out = std::ptr::null_mut();
        let mut last: Option<*mut Retired> = None;
        let mut ret_head = self.retired.head.load(Ordering::SeqCst);
        let mut deleted = 0;

        while !ret_head.is_null() {
            let deref_ret_head = unsafe { &(*ret_head) };
            if set.contains(&deref_ret_head.ptr) {
                let temporary = ret_head;
                ret_head = deref_ret_head.next.get();
                unsafe { (*temporary).next.set(left_out) };
                if last.is_none() {
                    last = Some(temporary);
                }
                left_out = temporary;
            } else {
                deleted += 1;
                let owned_ret = unsafe { Box::from_raw(ret_head) };
                (owned_ret.deleter)(owned_ret.ptr);
                ret_head = owned_ret.next.get();
            }
        }

        if left_out.is_null() {
            return deleted;
        }

        assert!(!left_out.is_null());
        let last_ptr = last.expect("Remaining is not null");

        loop {
            let mut head = self.retired.head.load(Ordering::SeqCst);
            unsafe { (*last_ptr).next.set(head) };
            if self
                .retired
                .head
                .compare_exchange_weak(head, left_out, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        deleted
    }
}

struct HazardList {
    head: AtomicPtr<HazPointer>,
}

impl HazardList {
    const fn new() -> Self {
        Self {
            head: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    fn acquire(&self) -> &HazPointer {
        let mut head = self.head.load(Ordering::SeqCst);
        while !head.is_null() {
            let shared = unsafe { &(*head) };
            if shared.status.load(Ordering::SeqCst)
                && shared
                    .status
                    .compare_exchange_weak(true, false, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return shared;
            } else {
                head = shared.next.get();
            }
        }

        let hazptr = HazPointer {
            ptr: AtomicPtr::new(std::ptr::null_mut()),
            next: Cell::new(std::ptr::null_mut()),
            status: AtomicBool::new(false),
        };
        let boxed = Box::into_raw(Box::new(hazptr));
        loop {
            let haz = unsafe { &(*boxed) };
            let head = self.head.load(Ordering::SeqCst);
            haz.next.set(head);
            if self
                .head
                .compare_exchange_weak(head, boxed, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break haz;
            }
        }
    }
}

// Storing type erased pointers in order to have a common list for
// pointers that can be pointing to allocations of different types
struct HazPointer {
    ptr: AtomicPtr<()>,
    next: Cell<*mut HazPointer>,
    status: AtomicBool,
}

impl HazPointer {
    fn protect(&self, ptr: *mut ()) {
        self.ptr.store(ptr, Ordering::SeqCst);
    }

    fn reset(&self) {
        self.ptr.store(std::ptr::null_mut(), Ordering::SeqCst);
        self.status.store(true, Ordering::SeqCst);
    }
}

pub struct Holder<'registry, S> {
    hazptr: Option<&'registry HazPointer>,
    registry: &'registry Registry<S>,
}

impl Holder<'static, Global> {
    pub fn with_global_registry() -> Self {
        Self {
            hazptr: None,
            registry: &GLOBAL_REGISTRY,
        }
    }
}
impl<'registry, S> Holder<'registry, S> {
    pub fn with_registry(registry: &'registry Registry<S>) -> Self {
        Self {
            hazptr: None,
            registry,
        }
    }
}

pub struct Guard<'a, T> {
    ptr: &'a T,
    hazptr: &'a HazPointer,
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.ptr
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        self.hazptr.reset();
    }
}

impl<'a, 'registry, S> Holder<'registry, S>
where
    'registry: 'a,
{
    /// # SAFETY
    ///    
    ///   The caller must guarantee that the [`Holder`] being used for this
    ///   load is the same one that is used for writing to the atomic pointer.
    ///   This is because reading from one and retiring into another registry
    ///   immediately raises the possibility of getting into UB due to dangling
    ///   pointer dereference.
    ///
    ///   The caller must also ensure that the pointer inside of atomic is a valid
    ///   pointer
    ///
    ///   Tieing the mutable borrow to the lifetime of the [`Guard`] ensures that
    ///   the reference to `T` will no longer be used after a second use of `load`
    ///   as that would lead to two mutable borrows at the same time, something that
    ///   the borrow checker will reject immediately.
    pub unsafe fn load<T>(&'a mut self, atomic: &AtomicPtr<T>) -> Option<Guard<'a, T>>
    where
        T: Provide<'registry, S>,
    {
        let hazpointer = if let Some(haz) = self.hazptr {
            haz
        } else {
            self.registry.acquire_hazptr()
        };

        let mut first = atomic.load(Ordering::SeqCst);
        hazpointer.protect(first as *mut ());
        loop {
            let second = atomic.load(Ordering::SeqCst);
            if first == second {
                if first.is_null() {
                    break None;
                } else {
                    let data = unsafe { &(*first) };
                    break Some(Guard {
                        ptr: data,
                        hazptr: hazpointer,
                    });
                }
            } else {
                first = second;
            }
        }
    }

    pub fn reset(&mut self) {
        if let Some(haz) = self.hazptr {
            haz.reset();
        }
    }
}

// Do we provide a specific type that the caller can wrap the actual type
// into, because using that type we can access the registry and call
// methods to retire. Since the caller is allowed to create multiple domains
// providing a trait is the most feasible option

pub trait Provide<'registry, S> {
    fn access_registry(&self) -> &'registry Registry<S>;

    fn get_deleter(&self) -> fn(*mut ());
    // On swap, the caller may access the pointer given out
    // to access the registry and call the retire method
    fn retire(self: *mut Self);
}

pub struct Provider<'registry, T, S> {
    inner: T,
    registry: &'registry Registry<S>,
    deleter: fn(*mut ()),
}

impl<T, S> Deref for Provider<'_, T, S> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Provider<'static, T, Global> {
    pub fn for_global_registry(inner: T, registry: &'static Registry<Global>) -> Self {
        Self {
            inner,
            registry,
            deleter: Provider::<T, Global>::drop,
        }
    }
}

impl<'registry, T, S> Provider<'registry, T, S> {
    pub fn new(inner: T, registry: &'registry Registry<S>) -> Self {
        Provider {
            inner,
            registry,
            deleter: Provider::<T, S>::drop,
        }
    }

    fn drop(ptr: *mut ()) {
        let _ = unsafe { Box::from_raw(ptr as *mut T) };
    }
}

macro_rules! generate_impl {
    ($type: ty) => {
        impl<'registry, T> Provide<'registry, $type> for Provider<'registry, T, $type> {
            fn access_registry(&self) -> &'registry Registry<$type> {
                self.registry
            }

            fn get_deleter(&self) -> fn(*mut ()) {
                self.deleter
            }

            #[allow(clippy::not_unsafe_ptr_arg_deref)]
            fn retire(self: *mut Self) {
                let shared = unsafe { &(*self) };
                let registry = shared.access_registry();
                let deleter = shared.get_deleter();
                registry.retire_ptr(self as *mut (), deleter);
            }
        }
    };
}

generate_impl!(First);
generate_impl!(Second);
generate_impl!(Third);
generate_impl!(Fourth);
generate_impl!(Fifth);

impl<T> Provide<'static, Global> for Provider<'static, T, Global> {
    fn access_registry(&self) -> &'static Registry<Global> {
        self.registry
    }

    fn get_deleter(&self) -> fn(*mut ()) {
        self.deleter
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn retire(self: *mut Self) {
        let shared = unsafe { &(*self) };
        let registry = shared.access_registry();
        let deleter = shared.get_deleter();
        registry.retire_ptr(self as *mut (), deleter);
    }
}

struct RetiredList {
    head: AtomicPtr<Retired>,
}

impl RetiredList {
    const fn new() -> Self {
        Self {
            head: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

struct Retired {
    next: Cell<*mut Retired>,
    ptr: *mut (),
    deleter: fn(*mut ()),
}
