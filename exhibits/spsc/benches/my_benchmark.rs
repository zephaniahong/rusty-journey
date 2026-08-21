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
                    barrier2.wait();

                    for _ in 0..iters {
                        while reader.recv().is_none() {
                            std::hint::spin_loop();
                        }
                    }
                });

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
            let mut spsc: SPSCBox<u64> = SPSCBox::new(128).unwrap();
            let (mut writer, mut reader) = spsc.split();

            let barrier = Arc::new(Barrier::new(2));
            let barrier2 = barrier.clone();

            thread::scope(|s| {
                let consumer = s.spawn(move || {
                    barrier2.wait();

                    for _ in 0..iters {
                        while reader.recv().is_none() {
                            std::hint::spin_loop();
                        }
                    }
                });

                barrier.wait();
                let start = Instant::now();

                for i in 0..iters {
                    while writer.send(i).is_some() {
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

// fn spsc_bench() {
//     const N: usize = 100000;
//     let mut spsc: SPSC_CG<usize, 128> = SPSC_CG::new().unwrap();
//     let (mut writer, mut reader) = spsc.split();
//
//     thread::scope(|s| {
//         s.spawn(|| {
//             for i in 0..N {
//                 while writer.send(i).is_some() {
//                     std::hint::spin_loop();
//                 }
//             }
//         });
//
//         for _ in 0..N {
//             while reader.recv().is_none() {
//                 std::hint::spin_loop();
//             }
//         }
//     });
// }

// fn main() {
//     let mut total = Duration::new(0, 0);
//     const N: usize = 100000;
//     for _ in 0..1000 {
//         let mut spsc = SPSC::new(100);
//         let (mut writer, mut reader) = spsc.split();
//
//         let start = Instant::now();
//
//         thread::scope(|s| {
//             s.spawn(|| {
//                 for i in 0..N {
//                     while writer.send(i).is_some() {
//                         std::hint::spin_loop();
//                     }
//                 }
//             });
//
//             for _ in 0..N {
//                 while reader.recv().is_none() {
//                     std::hint::spin_loop();
//                 }
//             }
//         });
//
//         let elapsed = start.elapsed();
//         total += elapsed;
//     }
//
//     let avg = total / 1000;
//
//     println!("Average: {:?}", avg);
// }
