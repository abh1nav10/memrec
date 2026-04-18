#![allow(dead_code, unused_variables, unused_assignments)]

use crossbeam::queue::ArrayQueue;
use recmem::{OpResult, mpsc};
use std::sync::{Arc, Barrier};

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

struct Pool<const N: usize, M, O> {
    /// Task senders and completion signal receivers for the threads pushing data into the queues.
    senders: Box<[flume::Sender<M>]>,
    receivers: Box<[flume::Receiver<()>]>,
    /// Task sender and completion signal receiver for the drainer thread.
    sender: flume::Sender<O>,
    receiver: flume::Receiver<()>,
    barrier: Arc<Barrier>,
}

impl<const N: usize, M, O> Pool<N, M, O>
where
    M: FnMut() + Send + 'static,
    O: FnMut() + Send + 'static,
{
    fn spawn() -> Pool<N, M, O> {
        let barrier = Arc::new(Barrier::new(N + 1));
        let mut senders = Vec::with_capacity(N - 1);
        let mut receivers = Vec::with_capacity(N - 1);

        std::iter::repeat_with(|| {
            let barrier = Arc::clone(&barrier);

            let (tx_main, rx_main) = flume::bounded(N - 1);
            let (tx, rx) = flume::bounded(N - 1);

            std::thread::spawn(move || {
                // We only take the task through the channel once and then store it for the entire
                // benchmark. That makes the impact of sending on the main task channel
                // non-existent. Further, threads only send on the other channel when their work is
                // done. This is compulsory to actually measure the time taken for a specific run of
                // the benchmark.
                let mut task: Option<M> = None;
                loop {
                    barrier.wait();
                    // This branch is free because we will have the task in here only except the
                    // first time. That must make the job of the branch predictor very very easy and
                    // it will be accurate.
                    if let Some(ref mut task) = task {
                        task();
                    } else {
                        let t: M = rx_main.recv().unwrap();
                        task = Some(t);
                        task.as_mut().unwrap()();
                    }
                    let _ = tx.send(());
                }
            });
            (tx_main, rx)
        })
        .take(N - 1)
        .for_each(|(tx_main, rx)| {
            senders.push(tx_main);
            receivers.push(rx);
        });

        // Support the sending of one drainer task.
        let barrier_cloned = Arc::clone(&barrier);

        let (tx_main, rx_main) = flume::unbounded();
        let (tx, rx) = flume::unbounded();

        std::thread::spawn(move || {
            let mut task: Option<O> = None;
            loop {
                barrier_cloned.wait();
                if let Some(ref mut task) = task {
                    task();
                } else {
                    let t: O = rx_main.recv().unwrap();
                    task = Some(t);
                    task.as_mut().unwrap()();
                }
                let _ = tx.send(());
            }
        });

        Pool {
            senders: senders.into_boxed_slice(),
            receivers: receivers.into_boxed_slice(),
            sender: tx_main,
            receiver: rx,
            barrier,
        }
    }

    fn send(&self, task: M, task2: O)
    where
        M: Clone,
    {
        self.senders.iter().take(N - 1).for_each(|s| {
            let _ = s.send(task.clone());
        });
        let _ = self.sender.send(task2);
    }

    fn initiate(&self) {
        self.barrier.wait();
    }

    fn finish(&self) {
        self.receivers.iter().for_each(|r| {
            let _ = r.recv();
        });
        let _ = self.receiver.recv();
    }
}

// The function that Criterion runs repeatedly.
fn measure<const N: usize, M, O>(pool: &Pool<N, M, O>)
where
    M: FnMut() + Send + 'static,
    O: FnMut() + Send + 'static,
{
    pool.initiate();
    pool.finish();
}

fn bench_queues(c: &mut Criterion) {
    // SETUP for benchmarking ArrayQueue
    let arrayqueue = Arc::new(ArrayQueue::new(64));
    let arrayqueue_cloned = Arc::clone(&arrayqueue);

    let closure = move || {
        let mut count = 0;
        for i in 0..1000 {
            if arrayqueue.push(i).is_ok() {
                count += 1;
            }
        }
        //println!("{count}");
    };

    let closure2 = move || {
        for _ in 0..1000 {
            while arrayqueue_cloned.pop().is_some() {}
        }
    };
    let pool = Pool::<3, _, _>::spawn();
    pool.send(black_box(closure), black_box(closure2));

    c.bench_function("ArrayQueue", |b| b.iter(|| measure(black_box(&pool))));

    // ----------------------------------------------------------------------------
    // SETUP for benchmarking FastSync
    let (tx, rx) = mpsc::<8, 8, usize>();
    let tx_cloned = tx.clone();

    let closure = move || {
        let mut count = 0;
        for i in 0..1000 {
            if let OpResult::Success = tx_cloned.push(i) {
                count += 1;
            }
        }
        //println!("{count}");
    };

    let closure2 = move || {
        for _ in 0..1000 {
            rx.drain_with(|_| {});
        }
    };
    let pool = Pool::<3, _, _>::spawn();
    pool.send(black_box(closure), black_box(closure2));

    c.bench_function("FastSync", |b| b.iter(|| measure(black_box(&pool))));
}

// Bench the draining of queue when there are no pushers.
fn bench_empty(c: &mut Criterion) {
    let arrayqueue = Arc::new(ArrayQueue::<usize>::new(64));
    c.bench_function("ArrayQueue", |b| b.iter(|| arrayqueue.pop()));

    let (_, rx) = mpsc::<64, 1, usize>();
    c.bench_function("FastSync", |b| b.iter(|| rx.drain_with(|_| {})));
}

criterion_group!(benches, bench_empty, bench_queues);
criterion_main!(benches);
