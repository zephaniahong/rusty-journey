use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
};

struct SPSC<T> {
    data: Box<[UnsafeCell<MaybeUninit<T>>]>,
    head: AtomicUsize,
    tail: AtomicUsize,
    capacity: usize,
}

impl<T> SPSC<T> {
    fn new(capacity: usize) -> Self {
        let data = std::iter::repeat_with(|| {
            let u: MaybeUninit<T> = MaybeUninit::uninit();
            UnsafeCell::new(u)
        })
        .take(capacity)
        .collect::<Vec<_>>()
        .into_boxed_slice();

        Self {
            data,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            capacity,
        }
    }

    pub fn split<'a>(&'a mut self) -> (Writer<'a, T>, Reader<'a, T>) {
        (Writer::new(self), Reader::new(self))
    }
}

unsafe impl<'a, T: Send> Send for Writer<'a, T> {}
unsafe impl<'a, T: Send> Send for Reader<'a, T> {}

struct Writer<'a, T> {
    channel: &'a SPSC<T>,
}

impl<'a, T> Writer<'a, T> {
    fn new(channel: &'a SPSC<T>) -> Self {
        Self { channel }
    }
    pub fn send(&mut self, data: T) -> Option<T> {
        let head = self.channel.head.load(Ordering::Acquire);
        let tail = self.channel.tail.load(Ordering::Acquire);
        if (tail + 1) % self.channel.capacity == head {
            Some(data)
        } else {
            unsafe {
                (*self.channel.data)[tail]
                    .get()
                    .write(MaybeUninit::new(data))
            };
            self.channel
                .tail
                .store((tail + 1) % self.channel.capacity, Ordering::Release);
            None
        }
    }
}

struct Reader<'a, T> {
    channel: &'a SPSC<T>,
}

impl<'a, T> Reader<'a, T> {
    fn new(channel: &'a SPSC<T>) -> Self {
        Self { channel: channel }
    }

    pub fn recv(&mut self) -> Option<T> {
        let head = self.channel.head.load(Ordering::Acquire);
        let tail = self.channel.tail.load(Ordering::Acquire);

        if head == tail {
            None
        } else {
            let cell = (*self.channel.data)
                .get(head)
                .expect("Index should always be within bounds")
                .get();

            let data = unsafe { Some(cell.read().assume_init()) };

            self.channel
                .head
                .store((head + 1) % self.channel.capacity, Ordering::Release);

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
        let mut spsc = SPSC::new(5);

        const N: usize = 100;
        thread::scope(|s| {
            let (mut writer, mut reader) = spsc.split();

            s.spawn(move || {
                for i in 0..N {
                    while writer.send(i).is_some() {
                        println!("Unable to write: {i}");
                        std::thread::yield_now();
                    }
                    println!("Wrote data: {i}");
                }
            });

            let mut store = Vec::new();

            while store.len() < N {
                match reader.recv() {
                    Some(data) => {
                        println!("Read data: {data}");
                        store.push(data);
                    }
                    None => std::thread::yield_now(),
                }
            }

            println!("{:?}", store);
        });
    }
}
