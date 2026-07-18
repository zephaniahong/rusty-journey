use std::{marker::PhantomData, sync::Arc};

struct Inner<T> {
    store: Box<[T]>,
}

impl<T> Inner<T> {
    fn new(capacity: usize) -> Self {
        let s = Vec::with_capacity(capacity);
        Self {
            store: s.into_boxed_slice(),
        }
    }
}

struct Producer<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Producer<T> {
    pub fn new(inner: Arc<Inner<T>>) -> Self {
        Producer { inner }
    }
}

struct Consumer<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Consumer<T> {
    pub fn new(inner: Arc<Inner<T>>) -> Self {
        Consumer { inner }
    }

    fn pop(&mut self) -> Option<T> {
        todo!()
    }
}

fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let inner = Arc::new(Inner::new(capacity));
    return (Producer::new(inner.clone()), Consumer::new(inner));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        // assert_eq!(result, 4);
    }
}
