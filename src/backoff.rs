#![allow(unused)]

use std::cell::Cell;

const SPIN_LIMIT: u8 = 7;

// Cannot be shared across threads!
pub struct Backoff {
    status: Cell<u8>,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

impl Backoff {
    pub fn new() -> Self {
        Self {
            status: Cell::new(5),
        }
    }

    pub fn initiate(&self) {
        let current = self.status.get();
        if current <= SPIN_LIMIT {
            self.status.set(current + 1);
            for _ in 0..1 << current {
                std::hint::spin_loop();
            }
        } else {
            self.status.set(0);
            std::thread::yield_now();
        }
    }
}
