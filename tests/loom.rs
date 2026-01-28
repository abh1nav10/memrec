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
            thread.join().unwrap();
            let deq = queue.dequeue();
            matches!(deq, Some(88) | Some(9));
        });
    }
}
