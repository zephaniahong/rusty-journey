#![allow(dead_code, unused_variables)]

use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
    thread::JoinHandle,
};

struct Producer<'a, T> {
    rb: &'a RingBuffer<T>,
    barriers: &'a Vec<Barrier<T>>,
    seq: Sequence,
}

impl<'a, T> Producer<'a, T> {
    pub fn new(rb: &'a RingBuffer<T>, barriers: &'a Vec<Barrier<T>>) -> Self {
        Self {
            rb,
            barriers,
            seq: Sequence::new(),
        }
    }

    pub fn send(&self, data: T) -> Option<T> {
        let tail = self.seq.get(Ordering::Acquire);
        let max_consumer = self
            .barriers
            .iter()
            .map(|b| &b.seq)
            .map(|s| s.get(Ordering::Relaxed))
            .max()
            .unwrap();

        if tail - max_consumer == self.rb.size() {
            // FULL
            Some(data)
        } else {
            let idx = tail & (self.rb.size() - 1);
            self.rb.set(idx, data);
            None
        }
    }
}

struct Executor<'a, T> {
    rb: &'a RingBuffer<T>,
    barriers: &'a Vec<Barrier<T>>,
}

impl<'a, T> Executor<'a, T> {
    pub fn new(rb: &'a RingBuffer<T>, barriers: &'a Vec<Barrier<T>>) -> Self {
        Self { rb, barriers }
    }
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

    pub fn get(&self, ordering: Ordering) -> usize {
        self.seq.load(ordering)
    }

    pub fn set(&self, val: usize) {
        self.seq.store(val, Ordering::Release);
    }
}

enum Handler<T> {
    Read(Box<dyn Fn(&T) + Send>),
    Write(Box<dyn FnMut(&mut T) + Send>),
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
        F: Fn(&T) + Send + 'static,
    {
        self.handlers.push(Handler::Read(Box::new(handler)));
        self
    }

    pub fn handle_event_mut<F>(&mut self, handler: F) -> &mut Self
    where
        F: FnMut(&mut T) + Send + 'static,
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
    capacity: usize,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        let slots = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();

        Self { slots, capacity }
    }

    pub fn size(&self) -> usize {
        self.capacity
    }

    pub fn get(&self, idx: usize) -> T {
        todo!()
    }

    pub fn set(&self, idx: usize, data: T) {
        unsafe { self.slots[idx].get().write(MaybeUninit::new(data)) };
    }
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send + Sync> Sync for RingBuffer<T> {}

struct DisruptorBuilder<T> {
    data: RingBuffer<T>,
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
            data: RingBuffer::new(capacity),
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

    pub fn build<'a>(&'a mut self) -> (Executor<'a, T>, Producer<'a, T>) {
        let executor = Executor::new(&self.data, &self.barriers);
        let producer = Producer::new(&self.data, &self.barriers);

        (executor, producer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut builder = DisruptorBuilder::<u64>::new(128)
            .with_barrier(|b| {
                b.handle_event(|d| {
                    println!("Logging ");
                });
                b.handle_event(|d| {
                    println!("Sending to backup");
                });
            })
            .with_barrier(|b| {
                b.handle_event_mut(|d| println!("mutable event"));
            });

        let (executor, producer) = builder.build();

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
