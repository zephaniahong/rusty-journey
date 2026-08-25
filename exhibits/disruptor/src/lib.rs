#![allow(dead_code, unused_variables)]

use std::{
    cell::UnsafeCell,
    cmp::min,
    marker::PhantomData,
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
};

struct Producer<T> {
    data: PhantomData<T>,
}

struct Executor<T> {
    data: PhantomData<T>,
}

impl<T> Executor<T> {
    pub fn spawn(&self) -> JoinHandle<T> {
        todo!()
    }
}

struct Sequence {
    seq: AtomicUsize,
}

impl Sequence {
    fn new() -> Self {
        Sequence {
            seq: AtomicUsize::new(0),
        }
    }

    pub fn get(&self) -> usize {
        self.seq.load(Ordering::Acquire)
    }
}

enum Handler<T> {
    Read(Box<dyn FnMut() + Send>),
    Write(Box<dyn FnMut() + Send>),
    _Marker(PhantomData<T>),
}

struct Barrier<T> {
    handlers: Vec<Handler<T>>,
    seq: Sequence,
}

impl<T> Barrier<T> {
    fn new() -> Self {
        Barrier {
            handlers: Vec::new(),
            seq: Sequence::new(),
        }
    }

    pub fn handle_event<F>(&mut self, handler: F) -> &mut Self
    where
        F: FnMut() + Send + 'static,
    {
        self.handlers.push(Handler::Read(Box::new(handler)));
        self
    }

    pub fn handle_event_mut<F>(&mut self, handler: F) -> &mut Self
    where
        F: FnMut() + Send + 'static,
    {
        self.handlers.push(Handler::Write(Box::new(handler)));
        self
    }

    fn is_valid(&self) -> bool {
        match self.handlers.len() {
            1 => true,
            _ => self.handlers.iter().all(|h| matches!(h, Handler::Read(_))),
        }
    }
}

struct RingBuffer<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        let slots = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();

        Self { slots }
    }

    pub fn index(&self, idx: usize) -> T {
        todo!()
    }
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send + Sync> Sync for RingBuffer<T> {}

struct DisruptorBuilder<T> {
    data: Arc<RingBuffer<T>>,
    producer_seq: Sequence,
    barriers: Vec<Barrier<T>>,
}

impl<T: Send + Sync + 'static> DisruptorBuilder<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity.is_power_of_two(),
            "capacity must be a power of two, got {capacity}"
        );

        DisruptorBuilder {
            data: Arc::new(RingBuffer::new(capacity)),
            producer_seq: Sequence::new(),
            barriers: Vec::new(),
        }
    }

    pub fn with_barrier<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Barrier<T>),
    {
        let mut barrier = Barrier::new();
        f(&mut barrier);

        self.barriers.push(barrier);
        self
    }

    pub fn build<'a>(self) -> (Executor<T>, Producer<T>) {
        // spawn a thread for each of the handlers

        let barriers = self.barriers;
        for window in self.barriers.windows(2) {
            let prev = window[0];
            let curr = window[1];
            // For each barrier, find out whats the maximum index it can read
            // Need to make sure previous barrier (i - 1) is done and producer is ahead
            for handler in curr.handlers {
                let idx = barriers[min(0, i - 1)].seq.get();
                let data = self.data.clone();
                // Find out whats the highest index this thread can read

                match handler {
                    Handler::Read(mut f) => {
                        let t = thread::spawn(move || {
                            loop {
                                let x = data.index(5);
                                f(); // TODO: take the data as an argument
                            }
                        });
                    }
                    Handler::Write(mut f) => {
                        thread::spawn(move || {
                            f();
                        });
                    }
                    Handler::_Marker(phantom_data) => todo!(),
                }
            }
        }

        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let (executor, producer) = DisruptorBuilder::<u64>::new(128)
            .with_barrier(|b| {
                b.handle_event(|| {
                    println!("Logging ");
                });
                b.handle_event(|| {
                    println!("Sending to backup");
                });
            })
            .with_barrier(|b| {
                b.handle_event_mut(|| println!("mutable event"));
            })
            .build();

        let handle = executor.spawn();

        handle.join().unwrap();

        // assert_eq!(disruptor.barriers.len(), 2);
        // assert_eq!(disruptor.barriers[0].handlers.len(), 2);
        // assert_eq!(disruptor.barriers[1].handlers.len(), 1);
    }

    #[test]
    #[should_panic(expected = "must contain only that handler")]
    fn rejects_a_mutator_sharing_a_barrier() {
        // DisruptorBuilder::<u64>::new(8).with_barrier(|b| {
        //     b.handle_event(|d| println!("{d}"));
        //     b.handle_event_mut(|d| *d += 1);
        // });
    }
}
