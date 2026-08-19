use std::{cell::UnsafeCell, mem::MaybeUninit};

struct Producer<T> {}

struct Consumer<T> {}

struct DisruptorBuilder<T> {
    data: Box<[UnsafeCell<MaybeUninit<T>>]>,
    producer_seq: Sequence,
    consumers: Vec<Consumer<T>>,
}

impl<T> DisruptorBuilder<T> {
    pub fn new(capacity: usize) -> Self {
        todo!()
    }

    pub fn add_consumer(&self) {
        todo!()
    }

    pub fn build(&self) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let disruptor = DisruptorBuilder::new(128);

        disruptor.add_consumer();
        disruptor.add_consumer();

        let (handle, producer) = disruptor.build();

        // let journaler = Consumer::new();
        // let replicator = Consumer::new();
        // let bz = Consumer::new();
    }
}
