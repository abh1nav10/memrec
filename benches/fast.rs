use crossbeam::queue::ArrayQueue;
use flume::bounded;
use recmem::{FastSync, OpResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

// Not a good way to benchmark as it ends up measuring the overhead of spawning threads as well.
// TODO: Implement a better benchmark.

fn fast_sync(num_threads: usize, enqueue: usize, num: usize) {
    let fastsync = Arc::new(FastSync::<4, 16, usize>::new());

    let mut handles = (0..(num_threads - 1))
        .map(|_| {
            let cloned = Arc::clone(&fastsync);
            std::thread::spawn(move || {
                let mut count = 1;
                for i in 0..num {
                    for j in 0..enqueue {
                        if let OpResult::BackPressure(_) = cloned.push(i * j) {
                            count += 1;
                        }
                    }
                }
                println!("{count}");
            })
        })
        .collect::<Vec<_>>();

    let flag = Arc::new(AtomicBool::new(false));
    let cloned = Arc::clone(&flag);
    let cloned_queue = Arc::clone(&fastsync);

    handles.push(std::thread::spawn(move || {
        let mut count = 0;
        for i in 0..num {
            for j in 0..enqueue {
                if let OpResult::BackPressure(_) = cloned_queue.push(i * j) {
                    count += 1;
                }
            }
        }
        println!("{count}");
        cloned.store(true, Ordering::Relaxed);
    }));

    let mut count = 0;
    while !flag.load(Ordering::Relaxed) {
        fastsync.drain_with(|_| count += 1);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn array_queue(num_threads: usize, enqueue: usize, num: usize) {
    let queue = Arc::new(ArrayQueue::new(64));

    let mut handles = (0..(num_threads - 1))
        .map(|_| {
            let cloned = Arc::clone(&queue);
            std::thread::spawn(move || {
                let mut count = 0;
                for i in 0..num {
                    for j in 0..enqueue {
                        if cloned.push(i * j).is_err() {
                            count += 1;
                        }
                    }
                }
                println!("{count}");
            })
        })
        .collect::<Vec<_>>();

    let flag = Arc::new(AtomicBool::new(false));
    let cloned = Arc::clone(&flag);
    let cloned_queue = Arc::clone(&queue);

    handles.push(std::thread::spawn(move || {
        let mut count = 0;
        for i in 0..num {
            for j in 0..enqueue {
                if cloned_queue.push(i * j).is_err() {
                    count += 1;
                }
            }
        }
        println!("{count}");
        cloned.store(true, Ordering::Relaxed);
    }));

    while !flag.load(Ordering::Relaxed) {
        while queue.pop().is_some() {}
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn flume(num_threads: usize, enqueue: usize, num: usize) {
    let (tx, rx) = bounded(64);
    let tx = Arc::new(tx);

    let mut handles = (0..(num_threads - 1))
        .map(|_| {
            let cloned = Arc::clone(&tx);
            std::thread::spawn(move || {
                let mut count = 0;
                for i in 0..num {
                    for j in 0..enqueue {
                        if cloned.try_send(i * j).is_err() {
                            count += 1;
                        }
                    }
                }
                println!("{count}");
            })
        })
        .collect::<Vec<_>>();

    let flag = Arc::new(AtomicBool::new(false));
    let cloned = Arc::clone(&flag);
    let cloned_queue = Arc::clone(&tx);

    handles.push(std::thread::spawn(move || {
        let mut count = 0;
        for i in 0..num {
            for j in 0..enqueue {
                if cloned_queue.send(i * j).is_err() {
                    count += 1;
                }
            }
        }
        println!("{count}");
        cloned.store(true, Ordering::Relaxed);
    }));

    while !flag.load(Ordering::Relaxed) {
        while rx.try_recv().is_ok() {}
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn bench_queues(c: &mut Criterion) {
    c.bench_function("Flume", |b| {
        b.iter(|| flume(black_box(3), black_box(100000), black_box(3)))
    });
    c.bench_function("ArrayQueue", |b| {
        b.iter(|| array_queue(black_box(3), black_box(100000), black_box(3)))
    });
    c.bench_function("FastSync", |b| {
        b.iter(|| fast_sync(black_box(3), black_box(100000), black_box(3)))
    });
}

criterion_group!(benches, bench_queues);
criterion_main!(benches);
