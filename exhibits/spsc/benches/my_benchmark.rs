use concurrency_zoo::{SPSC_CG, SPSCBox};
use criterion::{Criterion, criterion_group, criterion_main};
use std::{
    sync::{Arc, Barrier},
    thread::{self},
    time::Instant,
};

fn spsc_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc");

    group.throughput(criterion::Throughput::Elements(1));

    group.bench_function("spsc const generic", |b| {
        b.iter_custom(|iters| {
            let mut spsc: SPSC_CG<usize, 128> = SPSC_CG::new().unwrap();
            let (mut writer, mut reader) = spsc.split();

            let barrier = Arc::new(Barrier::new(2));
            let barrier2 = barrier.clone();

            thread::scope(|s| {
                let consumer = s.spawn(move || {
                    core_affinity::set_for_current(core_affinity::CoreId { id: 3 });
                    barrier2.wait();

                    for _ in 0..iters {
                        while reader.recv().is_none() {
                            std::hint::spin_loop();
                        }
                    }
                });

                core_affinity::set_for_current(core_affinity::CoreId { id: 1 });
                barrier.wait();
                let start = Instant::now();

                for i in 0..iters {
                    while writer.send(i as usize).is_some() {
                        std::hint::spin_loop();
                    }
                }
                let elapsed = start.elapsed();

                consumer.join().unwrap();
                elapsed
            })
        });
    });

    group.bench_function("spsc box", |b| {
        b.iter_custom(|iters| {
            let mut spsc: SPSCBox<usize> = SPSCBox::new(128).unwrap();
            let (mut writer, mut reader) = spsc.split();

            let barrier = Arc::new(Barrier::new(2));
            let barrier2 = barrier.clone();

            thread::scope(|s| {
                let consumer = s.spawn(move || {
                    core_affinity::set_for_current(core_affinity::CoreId { id: 3 });
                    barrier2.wait();

                    for _ in 0..iters {
                        while reader.recv().is_none() {
                            std::hint::spin_loop();
                        }
                    }
                });

                core_affinity::set_for_current(core_affinity::CoreId { id: 1 });
                barrier.wait();
                let start = Instant::now();

                for i in 0..iters {
                    while writer.send(i as usize).is_some() {
                        std::hint::spin_loop();
                    }
                }
                let elapsed = start.elapsed();

                consumer.join().unwrap();
                elapsed
            })
        });
    });
}

criterion_group!(benches, spsc_benchmark);
criterion_main!(benches);
