use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicI8, Ordering},
    },
};

struct Sender<T> {
    inner: Arc<OneShot<T>>,
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let state = self.inner.state.load(Ordering::Acquire);
        if state == 0 {
            // send() was never called
            self.inner.state.store(4, Ordering::Release);
        }
    }
}

impl<T> Sender<T> {
    pub fn send(&self, data: T) {
        if self
            .inner
            .state
            .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            panic!("Message has already been sent or channel is closed")
        }

        // SAFETY: we managed to set state to 1 == we got the exclusive right to write
        unsafe { (*self.inner.data.get()).write(data) };

        self.inner.state.store(READY, Ordering::Release);
    }
}
struct Receiver<T> {
    inner: Arc<OneShot<T>>,
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Result<T, ()> {
        loop {
            match self.inner.state.load(Ordering::Acquire) {
                NOT_READY | WRITING => std::hint::spin_loop(),
                READY => {
                    match self.inner.state.compare_exchange(
                        2,
                        3,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            let data = unsafe { (*self.inner.data.get()).assume_init_read() };
                            return Ok(data);
                        }
                        Err(_) => continue, // lost the race - loop back to see new state
                    }
                }
                READING | SENDER_DROPPED => return Err(()),
                _ => unreachable!(),
            }
        }
    }
}

const NOT_READY: i8 = 0;
const WRITING: i8 = 1;
const READY: i8 = 2;
const READING: i8 = 3;
const SENDER_DROPPED: i8 = 4;

struct OneShot<T> {
    data: UnsafeCell<MaybeUninit<T>>,
    state: AtomicI8,
}

impl<T> Drop for OneShot<T> {
    fn drop(&mut self) {
        let state = self.state.load(Ordering::Acquire);
        if state == 2 {
            unsafe { (*self.data.get()).assume_init_drop() };
        }
    }
}

unsafe impl<T: Send> Sync for OneShot<T> {}

impl<T> OneShot<T> {
    fn new() -> Self {
        OneShot {
            data: UnsafeCell::new(MaybeUninit::uninit()),
            state: AtomicI8::new(0),
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

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::*;

    #[test]
    fn it_works() {
        let (tx, rx) = OneShot::channel();

        let t1 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(1000));
            tx.send(78);
            tx.send(76);
        });

        let t2 = thread::spawn(move || {
            let data = rx.recv().unwrap();

            assert_eq!(data, 78);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn dropping_sender_before_send_should_not_spin_forever() {
        let (tx, rx) = OneShot::channel();

        let t1 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(1000));
            drop(tx);
        });

        let t2 = thread::spawn(move || {
            let res: &Result<i32, ()> = &rx.recv();
            assert!(res.is_err());
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }
}
