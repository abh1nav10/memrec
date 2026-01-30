#![feature(arbitrary_self_types_pointers)]
pub mod backoff;
pub mod hazard;
mod loom;
pub mod msqueue;

pub use backoff::Backoff;
pub use hazard::markers;
pub use hazard::{GLOBAL_REGISTRY, Holder, Provide, Provider, Registry};
pub use msqueue::Queue;
