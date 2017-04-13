/*
SPMC queue.

AGPL3.
*/
use std::sync::{Arc, Mutex};

/// Single producer, multiconsumer queue.
///
/// # intent
///
/// this is designed for a multithreaded producer-consumer system with
/// a high velocity of incoming items and a low velocity of consumers.
/// T should be clonable.
pub struct Queue<T> {
    elements : Arc<Mutex<Vec<T>>>
}

/// Example usage.
/// let (mut tx,mut rx) = queue::new();
pub fn new<T>() -> (Queue<T>, Queue<T>) {
    let sender = Queue { elements :  Arc::new(Mutex::new(Vec::new())) };
    let receiver = sender.clone();
    return (sender, receiver);
}


/// Tricky clone here. Queue's aren't actually clonable; a clone of a
/// queue is a *shallow* clone.
impl <T> Clone for Queue<T> {
    fn clone(&self) -> Queue<T> {

        //  https://doc.rust-lang.org/std/sync/struct.Arc.html
        // When you clone an Arc<T>, it will create another pointer to
        // the data and increase the reference counter.
        return Queue { elements : self.elements.clone() }

    }
}

impl <T> Queue<T> {
    /// Pushes value onto the queue.
    pub fn push(&mut self, value : T) {
        let guard = self.elements.lock();
        guard.unwrap().push(value);

    }

    /// Pops value from queue
    pub fn pop(&mut self) -> Option<T> {
        let guard = self.elements.lock();
        return guard.unwrap().pop();
    }

    /// Returns length of queue.
    pub fn len(&self) -> usize {
        let guard = self.elements.lock();
        return guard.unwrap().len();
    }

}

// Allow cloning, providing the underlying parameterized type allows
// cloning.
impl <T> Queue<T>
    where T: Clone {
/*
    fn backing_data(&self) -> Vec<T> {
        let foo = self.elements.lock();
        let bar = foo.unwrap();
        return bar.clone();
    }
*/
}
