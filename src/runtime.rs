use std::thread;
use std::sync::Arc;
use std::time::{self, Duration};
use mio;
use anyhow::Result;
use parking_lot::RwLock;
use smallvec::SmallVec;

const AVG_CORES: usize = 8;

pub trait ThreadSafe: Send + Sync + 'static {}

type Thunk = dyn Fn() -> Result<()> + Send + Sync;
// type Callback<TD> = dyn Fn(TD) -> Result<()> + Send + Sync;
pub type Handler<TD> = dyn Fn(&mut Arc<RwLock<Runtime<TD>>>, &mut TD, &mio::event::Event) -> Result<()> + Send + Sync;

pub struct Runtime<TD: ThreadSafe> {
    nthreads: usize,
    wakers: SmallVec<[Arc<mio::Waker>; AVG_CORES]>,
    polls: SmallVec<[Option<mio::Poll>; AVG_CORES]>,
    join_handles: SmallVec<[Option<thread::JoinHandle<Result<()>>>; AVG_CORES]>,
    thread_data: SmallVec<[Option<TD>; AVG_CORES]>,
    // callbacks: SmallVec<[Box<Thunk>; 5]>,
    timer_events: SmallVec<[TimerEvent; 10]>,
    source_handlers: SmallVec<[Arc<SourceHandler<TD>>; AVG_CORES * 2]>,
    next_token: usize,
    next_event_ix: usize,
    started: bool,
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

struct SourceHandler<TD: ThreadSafe> {
    source_id: mio::Token,
    handler: Box<Handler<TD>>
}

/// if repeat is true, the timer becomes periodic until cancelled
pub fn register_timer_event<TD: ThreadSafe>(runtime: &Arc<RwLock<Runtime<TD>>>, timer: time::Duration, repeat: bool, handler: Box<Thunk>) {
    let mut rwrite = runtime.write();
    let rt_cloned = runtime.clone();

    let event_ix = rwrite.get_next_event_ix();
    let next_trigger = time::Instant::now() + timer;

    rwrite.timer_events.push(TimerEvent { event_ix, handler, repeat, last_triggered: None, timer, next_trigger });

    let handle = thread::spawn(move || -> Result<()> {
        loop { 
            thread::sleep(timer);
            // println!("Waking");
            let wakers: SmallVec<[Arc<mio::Waker>; AVG_CORES]> = rt_cloned.read().wakers.iter().cloned().collect();
            for waker in wakers {
                waker.wake().ok();
            }
            if !repeat {
                break;
            }
            // TODO: cancellation check here
            // for event in runtime.write().timer_events.iter_mut().filter(
            //     |item| { item.event_ix == event_ix }) {
            //     event.handled = false;
            // }
        }
        Ok(())
    });
    rwrite.join_handles.push(Some(handle));
}

pub fn block_until_finished<TD: ThreadSafe>(runtime: Arc<RwLock<Runtime<TD>>>) -> Result<()> {
    let join_handles: SmallVec<[thread::JoinHandle<Result<()>>; AVG_CORES]> = runtime.write().join_handles.iter_mut()
        .map(|item| { item.take().unwrap() }).collect();
    for handle in join_handles {
        handle.join().unwrap()?;
    }
    Ok(())
}

impl<TD: ThreadSafe> Runtime<TD> {
    pub fn new(nthreads: usize) -> Result<Self> {
        let mut runtime = Runtime {
            polls: SmallVec::new(),
            wakers: SmallVec::new(),
            join_handles: SmallVec::new(),
            // callbacks: SmallVec::new(),
            timer_events: SmallVec::new(),
            source_handlers: SmallVec::new(),
            thread_data: SmallVec::new(),
            nthreads,
            next_token: 0,
            next_event_ix: 0,
            started: false,
        };

        for _ in 0..nthreads {
            let poll = mio::Poll::new()?;
            let waker = mio::Waker::new(poll.registry(), runtime.get_next_token())?;

            runtime.polls.push(Some(poll));
            runtime.wakers.push(Arc::new(waker));

            runtime.thread_data.push(None);
        }

        Ok(runtime)
    }

    // #[allow(dead_code)]
    // pub fn register_callback(&mut self, cb: Box<Thunk>) {
    //     self.callbacks.push(Box::new(cb));
    // }

    /// if interests: None, defaults to READABLE + WRITABLE
    pub fn register_event_source<S>(&mut self, source: &mut S, interests: Option<mio::Interest>, 
        thread_id: usize) -> Result<mio::Token>
    where S: mio::event::Source 
    {
        if thread_id >= self.nthreads {
            panic!("Can't register source with an unitialized thread");
        }
        let source_token = self.get_next_token();
        let interests = interests.unwrap_or(mio::Interest::READABLE.add(mio::Interest::WRITABLE));

        if let Some(poll) = &self.polls[thread_id] {
            poll.registry().register(source, source_token, interests)?;
        }
        Ok(source_token)
    }

    // TODO: partition per poll / thread
    pub fn register_source_event_handler(&mut self, token: mio::Token, handler: Box<Handler<TD>>) {
        self.source_handlers.push(Arc::new(SourceHandler {
            source_id: token,
            handler
        }));
    }

    pub fn set_thread_data(&mut self, thread_ix: usize, data: TD) {
        if self.started {
            panic!("Can't set thread data on a running runtime");
        }
        self.thread_data[thread_ix] = Some(data);
    }


    #[inline]
    fn trigger_timer_event(event: &mut TimerEvent, delay: time::Duration) -> bool {
        event.last_triggered = Some(event.next_trigger + delay);
        event.next_trigger += event.timer;
        // TODO: where to send error?
        (*event.handler)().ok();
        event.repeat
    }

    // TODO: Arc<RwLock<>> the TimerEvent and split into two methods, one which calls the handlers
    //       and modifies the events (runtime read lock), the second which prunes the finished events
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

    fn executor_loop_factory(mut runtime: Arc<RwLock<Self>>, poll: mio::Poll, thread_data: TD, thread_ix: usize)
        -> impl FnOnce() -> Result<()>
    { move || { 
        let mut poll = poll;
        let mut thread_data = thread_data;
        let mut events = mio::Events::with_capacity(1000);

        // for cb in &runtime.read().callbacks {
        //     cb();
        // }
        
        #[allow(unused_labels)]
        'event_loop: loop {
            poll.poll(&mut events, None)?;
            for event in events.iter() {
                let handlers: SmallVec<[Arc<SourceHandler<TD>>; 1]> = runtime.read().source_handlers.iter()
                    .filter(|handler| { handler.source_id == event.token() })
                    .cloned().collect();
                for handler in handlers {
                    (*handler.handler)(&mut runtime, &mut thread_data, event)?;
                }
                // Token(thread_ix) is Waker
                if event.token() == mio::Token(thread_ix) {
                    // println!("Thread {} awoken.", thread_ix);
                    runtime.write().handle_timer_events();
                }
            }
        }
        #[allow(unreachable_code)]
        Ok(())
    }}

    pub fn start(self) -> Arc<RwLock<Self>> {
        let runtime = Arc::new(RwLock::new(self));
        let mut rwrite = runtime.write();

        for i in 0..rwrite.nthreads {
            let handle = thread::spawn(Self::executor_loop_factory(runtime.clone(), 
                rwrite.polls[i].take().unwrap(), rwrite.thread_data[i].take().unwrap(), i));

            rwrite.join_handles.push(Some(handle));
        }

        rwrite.started = true;
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