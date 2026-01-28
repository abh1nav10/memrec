// Tested with LOOM_MAX_PREMPTIONS=3(recommended by author of loom), LOOM_MAX_PREMPTIONS=4 and LOOM_MAX_PREMPTIONS=5, passes all
// of them

#![allow(unexpected_cfgs)]

#[cfg(loom)]
#[cfg(test)]
mod tests {
    use loom::sync::Arc;
    use recmem::Queue;

    #[test]
    fn loom_test() {
        loom::model(|| {
            let queue = Arc::new(Queue::<u32>::new());
            let cloned = Arc::clone(&queue);
            queue.enqueue(9);
            let thread = loom::thread::spawn(move || {
                let _ = cloned.dequeue();
            });
            queue.enqueue(88);
            let deq = queue.dequeue();
            thread.join().unwrap();
            matches!(deq, Some(88) | Some(9));
        });
    }
}
