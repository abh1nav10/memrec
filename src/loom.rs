#![allow(unexpected_cfgs)]

#[cfg(not(loom))]
pub(crate) mod atomic {
    pub(crate) use std::sync::Arc;
    pub(crate) use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
}

#[cfg(loom)]
pub(crate) mod atomic {
    pub(crate) use loom::sync::Arc;
    pub(crate) use loom::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize};
    pub(crate) use std::sync::atomic::Ordering;
}
