use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam_utils::CachePadded;

pub struct SPSC<T> {
    data: Box<[UnsafeCell<MaybeUninit<T>>]>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    capacity: usize,
}

impl<T> SPSC<T> {
    pub fn new(capacity: usize) -> Result<Self, ()> {
        if !capacity.is_power_of_two() {
            return Err(());
        }
        let data = std::iter::repeat_with(|| {
            let u: MaybeUninit<T> = MaybeUninit::uninit();
            UnsafeCell::new(u)
        })
        .take(capacity)
        .collect::<Vec<_>>()
        .into_boxed_slice();

        Ok(Self {
            data,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            capacity,
        })
    }

    pub fn split<'a>(&'a mut self) -> (Writer<'a, T>, Reader<'a, T>) {
        (Writer::new(self), Reader::new(self))
    }
}

unsafe impl<'a, T: Send> Send for Writer<'a, T> {}
unsafe impl<'a, T: Send> Send for Reader<'a, T> {}

pub struct Writer<'a, T> {
    channel: &'a SPSC<T>,
}

impl<'a, T> Writer<'a, T> {
    fn new(channel: &'a SPSC<T>) -> Self {
        Self { channel }
    }
    pub fn send(&mut self, data: T) -> Option<T> {
        let tail = self.channel.tail.load(Ordering::Relaxed);
        let head = self.channel.head.load(Ordering::Acquire);
        let idx = tail & (self.channel.capacity - 1);

        if tail - head == self.channel.capacity {
            Some(data) // Full
        } else {
            unsafe {
                (*self.channel.data)[idx]
                    .get()
                    .write(MaybeUninit::new(data))
            };
            self.channel.tail.store(tail + 1, Ordering::Release);
            None
        }
    }
}

pub struct Reader<'a, T> {
    channel: &'a SPSC<T>,
}

impl<'a, T> Reader<'a, T> {
    fn new(channel: &'a SPSC<T>) -> Self {
        Self { channel: channel }
    }

    pub fn recv(&mut self) -> Option<T> {
        let head = self.channel.head.load(Ordering::Relaxed);
        let tail = self.channel.tail.load(Ordering::Acquire);
        let idx = head & (self.channel.capacity - 1);

        if tail - head == 0 {
            None // Empty
        } else {
            let cell = (*self.channel.data)
                .get(idx)
                .expect("Index should always be within bounds")
                .get();

            let data = unsafe { Some(cell.read().assume_init()) };

            self.channel.head.store(head + 1, Ordering::Release);

            data
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn it_works() {
        let mut spsc = SPSC::new(8).unwrap();
        let mut store = Vec::new();

        const N: usize = 100;
        thread::scope(|s| {
            let (mut writer, mut reader) = spsc.split();

            s.spawn(move || {
                for i in 0..N {
                    while writer.send(i).is_some() {
                        std::thread::yield_now();
                    }
                }
            });

            while store.len() < N {
                match reader.recv() {
                    Some(data) => {
                        store.push(data);
                    }
                    None => std::thread::yield_now(),
                }
            }
        });

        let mut res = Vec::new();

        for i in 0..100 {
            res.push(i);
        }

        assert_eq!(res, store);
    }
}
