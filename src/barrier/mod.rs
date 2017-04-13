/*
a classic barrier
AGPL3

*/
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub struct Barrier {
    completed : Arc<Mutex<usize>>,
    top : usize
}

impl Barrier {
    pub fn new(top : usize) -> Barrier{
        Barrier { top : top,
                  completed : Arc::new(Mutex::new(0)) }
    }

    pub fn reach(&mut self) {
        {
            let locked = self.completed.lock();
            *(locked).unwrap() += 1;
        }
        debug!("finished a task -  reached state: {}",
               *(self.completed.lock()).unwrap());
    }

    pub fn reached_p(&mut self) -> bool {
        let result =
        {
            let locked = self.completed.lock();
            *(locked).unwrap() >= self.top
        };
        debug!("checking for finish-up -  reached state {} out of {}",
               *(self.completed.lock()).unwrap(),
               self.top);
        return result;
    }

    pub fn wait_for_everyone(&mut self) {
        loop {
            let keep_waiting = {
                let locked = self.completed.lock();
                 *(locked).unwrap() < self.top
            };
            if keep_waiting {
                debug!("Waiting... - state {}", *(self.completed.lock()).unwrap());
                thread::sleep(Duration::from_millis(100));
            } else {
                break
            }
        }
    }
}

impl Clone for Barrier{
    fn clone(&self) -> Barrier{
        Barrier {
            completed : self.completed.clone(),
            top: self.top.clone()
        }
    }
}
