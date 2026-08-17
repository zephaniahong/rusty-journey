use concurrency_zoo::SPSC;
use criterion::{Criterion, criterion_group, criterion_main};
use std::thread;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("spsc 100-100000", |b| b.iter(|| test()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn test() {
    const N: usize = 100000;
    let mut spsc: SPSC<usize, 128> = SPSC::new().unwrap();
    let (mut writer, mut reader) = spsc.split();

    thread::scope(|s| {
        s.spawn(|| {
            for i in 0..N {
                while writer.send(i).is_some() {
                    std::hint::spin_loop();
                }
            }
        });

        for _ in 0..N {
            while reader.recv().is_none() {
                std::hint::spin_loop();
            }
        }
    });
}

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
