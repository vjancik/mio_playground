use num_cpus;
use std::thread;
use std::sync::Arc;
use std::time::{self, Duration};
// use std::collections::HashMap;
use mio;
use anyhow::Result;
use parking_lot::RwLock;
use smallvec::SmallVec;
// use std::convert::TryInto;

use runtime::*;

mod runtime {
    use super::*;

    type Thunk = dyn Fn() -> () + Send + Sync;

    pub struct Runtime {
        pub polls: SmallVec<[Arc<RwLock<mio::Poll>>; 8]>,
        pub wakers: SmallVec<[Arc<mio::Waker>; 8]>,
        pub join_handles: SmallVec<[Option<thread::JoinHandle<Result<()>>>; 8]>,
        pub nthreads: usize,
        callbacks: SmallVec<[Box<Thunk>; 5]>,
        timer_events: SmallVec<[TimerEvent; 10]>,
        next_token: usize,
        next_event_ix: usize,
    }

    #[derive(Clone, Copy, PartialEq)]
    struct EventIndex(usize);

    /// Can trigger multiple times in rapid succession if multiple timer periods were missed
    struct TimerEvent {
        //TODO: cancelling
        #[allow(dead_code)]
        event_ix: EventIndex,
        handler: Box<Thunk>,
        timer: Duration,
        next_trigger: time::Instant,
        last_triggered: Option<time::Instant>,
        repeat: bool,
    }

    /// if repeat is true, the timer becomes periodic until cancelled
    pub fn register_timer_event(runtime: &Arc<RwLock<Runtime>>, timer: time::Duration, repeat: bool, handler: Box<Thunk>) {
        let mut rwrite = runtime.write();
        let rt_cloned = runtime.clone();

        let event_ix = rwrite.get_next_event_ix();
        let next_trigger = time::Instant::now() + timer;

        rwrite.timer_events.push(TimerEvent { event_ix, handler, repeat, last_triggered: None, timer, next_trigger });

        let handle = thread::spawn(move || -> Result<()> {
            loop { 
                thread::sleep(timer);
                // println!("Waking");
                let wakers: SmallVec<[Arc<mio::Waker>; 8]> = rt_cloned.read().wakers.iter().cloned().collect();
                for waker in wakers {
                    waker.wake().ok();
                }
                if !repeat {
                    break;
                }
                // for event in runtime.write().timer_events.iter_mut().filter(
                //     |item| { item.event_ix == event_ix }) {
                //     event.handled = false;
                // }
            }
            Ok(())
        });
        rwrite.join_handles.push(Some(handle));
    }

    pub fn block_until_finished(runtime: Arc<RwLock<Runtime>>) -> Result<()> {
        let join_handles: SmallVec<[thread::JoinHandle<Result<()>>; 8]> = runtime.write().join_handles.iter_mut()
            .map(|item| { item.take().unwrap() }).collect();
        for handle in join_handles {
            handle.join().unwrap()?;
        }
        Ok(())
    }

    impl Runtime {
        pub fn new(nthreads: usize) -> Result<Self> {
            let mut runtime = Runtime {
                polls: SmallVec::new(),
                wakers: SmallVec::new(),
                join_handles: SmallVec::new(),
                callbacks: SmallVec::new(),
                timer_events: SmallVec::new(),
                nthreads,
                next_token: 0,
                next_event_ix: 0,
            };

            for _ in 0..nthreads {
                let poll = Arc::new(RwLock::new(mio::Poll::new()?));
                runtime.polls.push(poll.clone());

                let waker = mio::Waker::new(poll.read().registry(), runtime.get_next_token())?;
                runtime.wakers.push(Arc::new(waker));
            }

            Ok(runtime)
        }

        #[allow(dead_code)]
        pub fn register_callback(&mut self, cb: Box<Thunk>) {
            self.callbacks.push(Box::new(cb));
        }


        #[inline]
        fn trigger_timer_event(event: &mut TimerEvent, delay: time::Duration) -> bool {
            event.last_triggered = Some(event.next_trigger + delay);
            event.next_trigger += event.timer;
            (*event.handler)();
            event.repeat
        }

        fn handle_timer_events(&mut self) {
            self.timer_events.retain(|event| {
                match time::Instant::now().checked_duration_since(event.next_trigger) {
                    None if event.last_triggered == None => true,
                    // probably redundant
                    None => event.repeat,
                    Some(delay) => Self::trigger_timer_event(event, delay)
                }
            });
        }

        pub fn start(self) -> Arc<RwLock<Self>> {
            let runtime = Arc::new(RwLock::new(self));
            let mut rwrite = runtime.write();

            for i in 0..rwrite.nthreads {
                let runtime_clone = runtime.clone();

                let handle = thread::spawn(move || -> Result<()> {
                    let poll = runtime_clone.read().polls[i].clone();
                    let mut events = mio::Events::with_capacity(5);

                    for cb in &runtime_clone.read().callbacks {
                        cb();
                    }
                    
                    #[allow(unused_labels)]
                    'event_loop: loop {
                        poll.write().poll(&mut events, None)?;
                        for event in events.iter() {
                            if event.token() == mio::Token(i) {
                                // println!("Thread {} awoken.", i);
                                runtime_clone.write().handle_timer_events();
                                if runtime_clone.read().timer_events.len() == 0
                                {
                                    break 'event_loop;
                                }
                            }
                        }
                    }
                    #[allow(unreachable_code)]
                    Ok(())
                });
                rwrite.join_handles.push(Some(handle));
            }
            drop(rwrite);
            runtime
        }

        fn get_next_token(&mut self) -> mio::Token {
            let next_token = self.next_token;
            self.next_token += 1;
            mio::Token(next_token)
        }

        fn get_next_event_ix(&mut self) -> EventIndex {
            let next_ix = self.next_event_ix;
            self.next_event_ix += 1;
            EventIndex(next_ix)   
        }
    }
}

// fn any_func(ix: usize) {
//     println!("Function: Thread {}: Hello world", ix);
// }

fn main() -> Result<()> {
    let nthreads = num_cpus::get();
    let runtime = Runtime::new(nthreads)?;
    // runtime.register_callback(Box::new(|i| {println!("Closure: Thread {}: Hello world", i)}));
    // runtime.register_callback(Box::new(any_func));
    let runtime = runtime.start();

    let now = time::Instant::now();
    let timer = Duration::from_millis(100);
    runtime::register_timer_event(&runtime, timer, false, Box::new(move || {
        println!("Printing every {:?}, elapsed: {}", timer, now.elapsed().as_millis());
    }));

    runtime::block_until_finished(runtime)?;
    Ok(())
}
