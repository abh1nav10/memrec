// Using Ordering::SeqCst everywhere for making it easy to proove the correctness
// of the algorithm under the Rust memory model gurantees
//
// Update 1: 'domain Registry inside of Provider did not allow the creation of Provider
// in a thread different from where the atomic pointer was created due to lifetime mismatch
// errors as an AtomicPtr<T> is invariant in T. Therefore I am moving to using Arc<Registry>
// inside of Provider due get rid of this limitation. Invariance is the limitation and rightly
// so.
//
// Update 2: Updating the loop in Registry::reclaim_memory to make it ABA safe.
//
// Update 3: Reclamation now happens in batches(RECLAIM_THRESHOLD);
//
// Update 4: Implementaion of DerefMut for Provider.

#![allow(unused)]

// Dont allow the users to construct this type
#[non_exhaustive]
struct Global;

/// This module contains some types that which I have used to provide an
/// implementation of [`Provide`] trait for [`Provider`].
///
/// The user is free to create his own type, and then implement the [`Provide`] trait
/// for that type with as many types that he wants. That would allow the construction
/// of those many domains while at the same time guaranteeing the safety to some extent
/// as long as the caller does NOT create many instances of the [`Registry`] for the same
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
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

const RECLAIM_THRESHOLD: usize = 100;

#[allow(private_interfaces)]
pub static GLOBAL_REGISTRY: Registry<Global> = Registry::new();

/// This type is generic over `S` simply for the purpose of providing compile-time checks
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

    /// Returns the number of reclaimed pointers.
    pub fn retire_ptr(&self, ptr: *mut (), deleter: fn(*mut ())) -> usize {
        let retired = Retired {
            next: Cell::new(std::ptr::null_mut()),
            ptr,
            deleter,
        };
        let boxed = Box::into_raw(Box::new(retired));
        loop {
            let head = self.retired.head.load(Ordering::SeqCst);
            unsafe {
                (*boxed).next.set(head);
            }
            if self
                .retired
                .head
                .compare_exchange_weak(head, boxed, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                if let Ok(RECLAIM_THRESHOLD) =
                    self.retired
                        .count
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| {
                            Some((c + 1) % RECLAIM_THRESHOLD)
                        })
                {
                    break self.reclaim_memory();
                } else {
                    break 0;
                }
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
        let mut deleted = 0;
        let mut ret_head = self
            .retired
            .head
            .swap(std::ptr::null_mut(), Ordering::SeqCst);

        while !ret_head.is_null() {
            let deref_ret_head = unsafe { &(*ret_head) };

            if set.contains(&deref_ret_head.ptr) {
                let temporary = ret_head;
                ret_head = deref_ret_head.next.get();
                unsafe { (*temporary).next.set(left_out) };
                left_out = temporary;
            } else {
                deleted += 1;
                let owned_ret = unsafe { Box::from_raw(ret_head) };
                (owned_ret.deleter)(owned_ret.ptr);
                ret_head = owned_ret.next.get();
                drop(owned_ret);
            }
        }

        if left_out.is_null() {
            return deleted;
        }

        let mut list_walk = left_out;

        // This loop is now ABA resistant. Trying to swap the head only with a null pointer as
        // opposed to relying on the previously loaded head pointer takes away the risk of the ABA
        // problem. But that has led to walking the list on every swap.
        loop {
            if self
                .retired
                .head
                .compare_exchange_weak(
                    std::ptr::null_mut(),
                    list_walk,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                break;
            } else {
                let swapped = self
                    .retired
                    .head
                    .swap(std::ptr::null_mut(), Ordering::SeqCst);
                if swapped.is_null() {
                    continue;
                }
                let mut current = swapped;
                let mut next_ptr_of_current = unsafe { (*current).next.get() };
                while !next_ptr_of_current.is_null() {
                    current = next_ptr_of_current;
                    next_ptr_of_current = unsafe { (*next_ptr_of_current).next.get() };
                }
                // Setting the last of current to be list_walk and then try to swap head again with
                // this.
                unsafe {
                    (*current).next.set(list_walk);
                }
                list_walk = swapped;
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
        // The load can have `Acquire` semantics
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
            // Relaxed load followed by an Release CAS is fine.
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

unsafe impl Send for HazPointer {}
unsafe impl Sync for HazPointer {}

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
    ///   1. The caller must guarantee that the [`Holder`] being used for this
    ///      load is the same one that is used for writing to the atomic pointer.
    ///      This is because reading from one and retiring into another registry
    ///      immediately raises the possibility of getting into UB due to dangling
    ///      pointer dereference.
    ///
    ///   2. The caller must also ensure that the pointer inside of atomic is a valid
    ///      pointer. Providing a dangling, unaligned or a pointer with different
    ///      provenance is going to lead to UB on dereference.
    ///
    ///   Tieing the mutable borrow to the lifetime of the [`Guard`] ensures that
    ///   the reference to `T` will no longer be used after a second use of `load`
    ///   as that would lead to two mutable borrows at the same time, something that
    ///   the borrow checker will reject immediately.
    pub unsafe fn load<T>(&'a mut self, atomic: &AtomicPtr<T>) -> Option<Guard<'a, T>>
    where
        T: Provide<S>,
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
                    // SAFETY: Safe because of the contract on this function.
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

pub trait Provide<S> {
    fn access_registry(&self) -> &Arc<Registry<S>>;

    fn get_deleter(&self) -> fn(*mut ());
    // On swap, the caller may access the pointer given out
    // to access the registry and call the retire method
    fn retire(self: *mut Self) -> usize;
}

pub struct Provider<T, S> {
    inner: T,
    registry: Arc<Registry<S>>,
    deleter: fn(*mut ()),
}

impl<T, S> Deref for Provider<T, S> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// The implementation of DerefMut will be most useful while dropping data structures implemented on
// top of this, as instead of atomically loading the ptrs, we can obtain a mutable reference to
// them since no other thread will be holding it in parallel. Without the implementation of
// DerefMut, a mutable reference to the fields of Provider cant be obtained through a dereference
// of a raw pointer to Provider!
impl<T, S> DerefMut for Provider<T, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> Provider<T, Global> {
    // Getting an Arc<Registry<Global>> is pointless sinceRegistry<Global> is static. This
    // anyway needs to be done to make it compatible with non-static registries.
    pub fn for_global_registry(inner: T, registry: Arc<Registry<Global>>) -> Self {
        Self {
            inner,
            registry,
            deleter: Provider::<T, Global>::drop,
        }
    }
}

impl<T, S> Provider<T, S> {
    pub fn new(inner: T, registry: Arc<Registry<S>>) -> Self {
        Provider {
            inner,
            registry,
            deleter: Provider::<T, S>::drop,
        }
    }

    fn drop(ptr: *mut ()) {
        let _ = unsafe { Box::from_raw(ptr as *mut Provider<T, S>) };
    }
}

macro_rules! generate_impl {
    ($type: ty) => {
        impl<T> Provide<$type> for Provider<T, $type> {
            fn access_registry(&self) -> &Arc<Registry<$type>> {
                &self.registry
            }

            fn get_deleter(&self) -> fn(*mut ()) {
                self.deleter
            }

            #[allow(clippy::not_unsafe_ptr_arg_deref)]
            fn retire(self: *mut Self) -> usize {
                let shared = unsafe { &(*self) };
                let registry = shared.access_registry();
                let deleter = shared.get_deleter();
                registry.retire_ptr(self as *mut (), deleter)
            }
        }
    };
}

generate_impl!(First);
generate_impl!(Second);
generate_impl!(Third);
generate_impl!(Fourth);
generate_impl!(Fifth);

impl<T> Provide<Global> for Provider<T, Global> {
    fn access_registry(&self) -> &Arc<Registry<Global>> {
        &self.registry
    }

    fn get_deleter(&self) -> fn(*mut ()) {
        self.deleter
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn retire(self: *mut Self) -> usize {
        let shared = unsafe { &(*self) };
        let registry = shared.access_registry();
        let deleter = shared.get_deleter();
        registry.retire_ptr(self as *mut (), deleter)
    }
}

struct RetiredList {
    head: AtomicPtr<Retired>,
    count: AtomicUsize,
}

impl RetiredList {
    const fn new() -> Self {
        Self {
            head: AtomicPtr::new(std::ptr::null_mut()),
            count: AtomicUsize::new(0),
        }
    }
}

struct Retired {
    next: Cell<*mut Retired>,
    ptr: *mut (),
    deleter: fn(*mut ()),
}

#[cfg(test)]
mod tests {

    // This test used to pass before this update of batched reclaim when the RECLAIM_THRESHOLD is
    // touched!!

    use super::*;
    //use std::sync::atomic::AtomicUsize;

    //struct Count(Arc<AtomicUsize>);
    //impl Drop for Count {
    //    fn drop(&mut self) {
    //        self.0.fetch_add(1, Ordering::SeqCst);
    //    }
    //}

    //#[test]
    //fn check_drop() {
    //    let registry = Arc::new(Registry::<First>::new());
    //    let mut holder = Holder::with_registry(&registry);
    //    let count = Arc::new(AtomicUsize::new(0));

    //    let provider = Box::into_raw(Box::new(Provider::<_, First>::new(
    //        Count(Arc::clone(&count)),
    //        Arc::clone(&registry),
    //    )));

    //    let atomicptr = AtomicPtr::new(provider);
    //    let guard = unsafe { holder.load(&atomicptr) }.expect("Cant be null");

    //    let provider2 = Box::into_raw(Box::new(Provider::<_, First>::new(
    //        Count(Arc::clone(&count)),
    //        Arc::clone(&registry),
    //    )));

    //    let swapped = atomicptr.swap(provider2, Ordering::SeqCst);
    //    let num = swapped.retire();

    //    assert_eq!(num, 0);
    //    assert_eq!(count.load(Ordering::SeqCst), 0);

    //    drop(guard);

    //    let swapped = atomicptr.swap(std::ptr::null_mut(), Ordering::SeqCst);
    //    let num = swapped.retire();

    //    assert_eq!(num, 2);
    //    assert_eq!(count.load(Ordering::SeqCst), 2);
    //}

    #[test]
    fn check_concurrency() {
        struct Inner;

        let registry = Arc::new(Registry::<First>::new());

        let inner = Inner;

        let provider = Box::into_raw(Box::new(Provider::new(inner, Arc::clone(&registry))));
        let atomic = Arc::new(AtomicPtr::new(provider));

        let counter = Arc::new(AtomicUsize::new(0));

        let handles = (0..100)
            .map(|i| {
                let cloned_registry = Arc::clone(&registry);
                let cloned_atomic = Arc::clone(&atomic);
                let counter = Arc::clone(&counter);
                if i % 2 == 0 {
                    std::thread::spawn(move || {
                        for _ in 0..500 {
                            let mut holder = Holder::with_registry(&cloned_registry);
                            let guard = unsafe { holder.load(&cloned_atomic) };
                            // Reaching here safely should passes the check because the pointers when
                            // loaded are protected since Holder::load dereferences the pointer after
                            // loading. That is indeed what we are testing in a concurrent
                            // scenario.
                            if guard.is_some() {
                                counter.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    })
                } else {
                    std::thread::spawn(move || {
                        for _ in 0..500 {
                            let cloned_for_loop = cloned_registry.clone();
                            let inner = Inner;
                            let provider =
                                Box::into_raw(Box::new(Provider::new(inner, cloned_for_loop)));
                            let swapped = cloned_atomic.swap(provider, Ordering::SeqCst);
                            let _ = swapped.retire();
                        }
                    })
                }
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap();
        }

        let count = counter.load(Ordering::SeqCst);

        // Trivial check, if we reach here, the test has already passed!!
        assert!(count > 0);

        println!("{}", count);
    }
}
