#![feature(arbitrary_self_types_pointers)]
pub mod hazard;
pub mod msqueue;

pub use hazard::markers::{Fifth, First, Fourth, Second, Third};
pub use hazard::{GLOBAL_REGISTRY, Guard, Holder, Provide, Provider, Registry};
