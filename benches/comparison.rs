#![allow(unused)]

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use crossbeam::queue::SegQueue;
use recmem::Queue;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

struct Pool<const N: usize> {
    joinhandles: [JoinHandle<()>; N],
    status: [Arc<AtomicBool>; N],
    flag: Arc<AtomicBool>,
}

// TODO: Use a declarative macro
impl<const N: usize> Pool<N> {
    fn lock_free_backoff(queue: Arc<Queue<u32>>) -> Self {
        let flag = Arc::new(AtomicBool::new(false));
        let status: [Arc<AtomicBool>; N] =
            std::array::from_fn(|_| Arc::new(AtomicBool::new(false)));
        let mut index = 0;
        let handles: [JoinHandle<()>; N] = std::array::from_fn(|_| {
            let cloned = Arc::clone(&status[index]);
            let flag = Arc::clone(&flag);
            let que = Arc::clone(&queue);
            index += 1;
            // Mod operation is kept outside the thread spawn to avoid its overhead inside the real
            // measurement!
            if index % 2 == 0 {
                thread::spawn(move || {
                    loop {
                        if flag.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    for i in 0..200 {
                        que.enqueue_with_backoff(i);
                    }
                    for _ in 0..200 {
                        // This newly added method takes away memory reclamation overhead from this
                        // benchmark.
                        let _ = que.dequeue_without_reclaim_with_backoff();
                    }
                    cloned.store(true, Ordering::Relaxed);
                })
            } else {
                thread::spawn(move || {
                    loop {
                        if flag.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    for _ in 0..200 {
                        let _ = que.dequeue_without_reclaim_with_backoff();
                    }
                    for i in 0..200 {
                        que.enqueue_with_backoff(i);
                    }
                    cloned.store(true, Ordering::Relaxed);
                })
            }
        });
        Self {
            joinhandles: handles,
            status,
            flag,
        }
    }

    fn crossbeam_segqueue(queue: Arc<SegQueue<u32>>) -> Self {
        let flag = Arc::new(AtomicBool::new(false));
        let status: [Arc<AtomicBool>; N] =
            std::array::from_fn(|_| Arc::new(AtomicBool::new(false)));
        let mut index = 0;
        let handles: [JoinHandle<()>; N] = std::array::from_fn(|_| {
            let cloned = Arc::clone(&status[index]);
            let flag = Arc::clone(&flag);
            let que = Arc::clone(&queue);
            index += 1;
            if index % 2 == 0 {
                thread::spawn(move || {
                    loop {
                        if flag.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    for i in 0..200 {
                        que.push(i);
                    }
                    for _ in 0..200 {
                        let _ = que.pop();
                    }
                    cloned.store(true, Ordering::Relaxed);
                })
            } else {
                thread::spawn(move || {
                    loop {
                        if flag.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    for _ in 0..200 {
                        let _ = que.pop();
                    }
                    for i in 0..200 {
                        que.push(i);
                    }
                    cloned.store(true, Ordering::Relaxed);
                })
            }
        });
        Self {
            joinhandles: handles,
            status,
            flag,
        }
    }

    fn locking(queue: Arc<Mutex<Vec<u32>>>) -> Self {
        let flag = Arc::new(AtomicBool::new(false));
        let status: [Arc<AtomicBool>; N] =
            std::array::from_fn(|_| Arc::new(AtomicBool::new(false)));
        let mut index = 0;
        let handles: [JoinHandle<()>; N] = std::array::from_fn(|_| {
            let cloned = Arc::clone(&status[index]);
            let flag = Arc::clone(&flag);
            let que = Arc::clone(&queue);
            index += 1;
            if index % 2 == 0 {
                thread::spawn(move || {
                    loop {
                        if flag.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    for i in 0..200 {
                        let mut lock = que.lock().unwrap();
                        lock.push(i);
                    }

                    for _ in 0..200 {
                        let mut lock = que.lock().unwrap();
                        let _ = lock.pop();
                    }

                    cloned.store(true, Ordering::Relaxed);
                })
            } else {
                thread::spawn(move || {
                    loop {
                        if flag.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    for _ in 0..200 {
                        let mut lock = que.lock().unwrap();
                        let _ = lock.pop();
                    }
                    for i in 0..200 {
                        let mut lock = que.lock().unwrap();
                        lock.push(i);
                    }
                    cloned.store(true, Ordering::Relaxed);
                })
            }
        });
        Self {
            joinhandles: handles,
            status,
            flag,
        }
    }
}

fn measure(pool: Pool<8>) {
    pool.flag.store(true, Ordering::Relaxed);
    let mut completed = true;
    loop {
        for element in pool.status.iter() {
            if !element.load(Ordering::Relaxed) {
                completed = false;
                break;
            }
        }
        if completed {
            break;
        } else {
            completed = true;
        }
    }
}

fn bench_locking(c: &mut Criterion) {
    let mut group = c.benchmark_group("Mutex<Vec>");
    group.bench_function("lock_fn", |b| {
        b.iter_batched(
            || {
                let queue = Arc::new(Mutex::new(Vec::new()));
                Pool::locking(queue)
            },
            |mut pool| measure(black_box(pool)),
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

fn bench_lockfree_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("Queue");
    group.bench_function("lockfree", |b| {
        b.iter_batched(
            || {
                let queue = Arc::new(Queue::new());
                Pool::lock_free_backoff(queue)
            },
            |mut pool| measure(black_box(pool)),
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

fn bench_crossbeam_segqueue(c: &mut Criterion) {
    let mut group = c.benchmark_group("SegQueue");
    group.bench_function("lockfree", |b| {
        b.iter_batched(
            || {
                let queue = Arc::new(SegQueue::new());
                Pool::crossbeam_segqueue(queue)
            },
            |mut pool| measure(black_box(pool)),
            BatchSize::PerIteration,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_locking,
    bench_crossbeam_segqueue,
    bench_lockfree_queue,
);
criterion_main!(benches);

// Learnings:
//
//// The commented benchmark produces false results because the noise for the bench functions goes down
// progressively as in case of `Mutex`, the number of threads is too many as it includes the ones
// spawned for the `SegQueue` and `Queue` benchmark. Thus each one of them has to be measured separately.
// Further the flag is flipped once and work is done just once. So the benchmark is essentially measuring nothing.
// Maybe use a monotonic counter and make the threads loop and wait for the counter to be updated
// to start doing their work again. Counter will be updated my each measure call. At the end of the
// not the iter but the benchmark function that encapsulated all the iterations set the shutdown
// flag to true for all threads to exit. This too did not work.
//fn bench_ops(c: &mut Criterion) {
//    let flag1 = Arc::new(AtomicBool::new(false));
//    let queue1 = Arc::new(Mutex::new(Vec::new()));
//    let pool1 = Pool::locking(Arc::clone(&flag1), queue1);
//
//    let flag2 = Arc::new(AtomicBool::new(false));
//    let queue2 = Arc::new(Queue::new());
//    let pool2 = Pool::lock_free_backoff(Arc::clone(&flag2), queue2);
//
//    let flag3 = Arc::new(AtomicBool::new(false));
//    let queue3 = Arc::new(SegQueue::new());
//    let pool3 = Pool::crossbeam_segqueue(Arc::clone(&flag3), queue3);
//
//    let mut group = c.benchmark_group("locking_lockfree");
//    group.bench_function("Mutex", |b| {
//        b.iter(|| measure(black_box(flag1.clone()), black_box(&pool1)))
//    });
//    group.bench_function("SegQueue", |b| {
//        b.iter(|| measure(black_box(flag3.clone()), black_box(&pool3)))
//    });
//    group.bench_function("LockFree", |b| {
//        b.iter(|| measure(black_box(flag2.clone()), black_box(&pool2)))
//    });
//    group.finish();
//}
//
//MARK: Following approach did not succeed
// monotonic counter (AtomicU64 -> I do not want an overflow, Criterion
// sample size is limited by usize(size is of the size of a CPU word on the machine),
// 8 bytes is the largest size (general machines, exception - those used in research),
// hence u64 won't overflow), incremented once by the `measure`
// function on every iteration of the benchmark. This solves the problem of performing the
// enqueuing and dequeuing work only once which used to occur because all
// threads used to exit after performing all there operations and all the
// subsequent iterations measured by Criterion would effectively be doing
// nothing because the measure function would return immediately as the
// work was already done. It also solves the problem of incurring
// measurement overhead due to successive thread spawning. Spawning is done
// only once at the beginning. It is for the same reason that I did not use
// the `iter_batched` or `iter_batched_ref` as they would both cause
// measurement overhead because `BatchSize` would have to be set to
// `PerIteration` as not doing so would spawn lots of threads at the
// beginning itself and make all kinds of wierd things happen, possibly out
// of memory errors and a very very noisy environment due to soo many
// context switches of all those threads. So overhead occurs both ways.
// `PerIteration` causes its own overhead and `SmallInput` and `LargeInput`
// comes with the aforementioned problem of too many thread spawns in the
// beginning itself.
//
// non-monotonic updates(like using an AtomicBool to flip is backforth between true and false)
// present the lost-update problem
//
// Tried the above approach, failed due to weird blocking issues when performing state reset
// at the end of the measure function.
//
// Currently using iter_batched with BatchSize::PerIteration, but that comes with its own problems related to measurememt
// overhead. This benchmark cannot be carried out without providing inputs using `iter_batched`
// as trying otherwise got me into those wierd issues possibly related to memory orderings that
// blocked the entire system. The same state reset problem will occur if we use a Barrier as in
// once the state is updated by all threads we need to reset it back from the measure function for
// the next iteration of the benchmark. There might be a better approach.
