#![feature(arbitrary_self_types_pointers)]
pub mod hazard;
pub mod msqueue;

pub use hazard::markers;
pub use hazard::{GLOBAL_REGISTRY, Holder, Provide, Provider, Registry};
