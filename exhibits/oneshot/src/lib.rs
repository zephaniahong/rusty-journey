use std::sync::atomic::{AtomicBool, Ordering};
use std::{cell::UnsafeCell, mem::MaybeUninit, sync::Arc};

struct Sender<T> {
    inner: Arc<OneShot<T>>,
}

impl<T> Sender<T> {
    pub fn send(self, data: T) {
        unsafe { (*self.inner.data.get()).write(data) };

        self.inner.state.store(true, Ordering::Release);
    }
}
struct Receiver<T> {
    inner: Arc<OneShot<T>>,
}

impl<T> Receiver<T> {
    pub fn recv(self) -> T {
        if !self.inner.state.swap(false, Ordering::Acquire) {
            panic!("No message available");
        }
        unsafe { (*self.inner.data.get()).assume_init_read() }
    }
}

struct OneShot<T> {
    data: UnsafeCell<MaybeUninit<T>>,
    state: AtomicBool,
}

unsafe impl<T: Send> Sync for OneShot<T> {}

impl<T> OneShot<T> {
    fn new() -> Self {
        OneShot {
            data: UnsafeCell::new(MaybeUninit::uninit()),
            state: AtomicBool::new(false),
        }
    }
    pub fn channel() -> (Sender<T>, Receiver<T>) {
        let oneshot = Arc::new(OneShot::new());

        (
            Sender {
                inner: oneshot.clone(),
            },
            Receiver { inner: oneshot },
        )
    }
}

impl<T> Drop for OneShot<T> {
    fn drop(&mut self) {
        if *self.state.get_mut() {
            unsafe { self.data.get_mut().assume_init_drop() };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn it_works() {
        let (tx, rx) = OneShot::channel();

        let t1 = thread::spawn(move || {
            tx.send(78);
        });

        let t2 = thread::spawn(move || {
            t1.join().unwrap();
            let data: i32 = rx.recv();

            assert_eq!(data, 78);
        });

        t2.join().unwrap();
    }
}
