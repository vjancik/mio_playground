use num_cpus;
use std::thread;
use std::sync::Arc;
use std::time::{self, Duration};
use mio::{self, net::UdpSocket};
use anyhow::Result;
use parking_lot::RwLock;
use smallvec::SmallVec;
use cfg_if;

use runtime::*;

mod runtime {
    use super::*;

    pub trait SharedData: Send + Sync + 'static {}

    type Thunk = dyn Fn() -> () + Send + Sync;
    // type Callback<T> = dyn Fn(T) -> Result<()> + Send + Sync;
    pub type Handler<T> = dyn Fn(&mut Arc<RwLock<Runtime<T>>>, &mut Arc<RwLock<Option<T>>>, &mio::event::Event) -> Result<()> + Send + Sync;

    pub struct Runtime<T: SharedData> {
        pub polls: SmallVec<[Arc<RwLock<mio::Poll>>; 8]>,
        pub wakers: SmallVec<[Arc<mio::Waker>; 8]>,
        pub join_handles: SmallVec<[Option<thread::JoinHandle<Result<()>>>; 8]>,
        pub nthreads: usize,
        pub shared_data: SmallVec<[Arc<RwLock<Option<T>>>; 8]>,
        callbacks: SmallVec<[Box<Thunk>; 5]>,
        timer_events: SmallVec<[TimerEvent; 10]>,
        source_handlers: Arc<RwLock<SmallVec<[SourceHandler<T>; 8 * 2]>>>,
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

    struct SourceHandler<T: SharedData> {
        source_id: mio::Token,
        handler: Box<Handler<T>>
    }

    /// if repeat is true, the timer becomes periodic until cancelled
    pub fn register_timer_event<T: SharedData>(runtime: &Arc<RwLock<Runtime<T>>>, timer: time::Duration, repeat: bool, handler: Box<Thunk>) {
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

    pub fn block_until_finished<T: SharedData>(runtime: Arc<RwLock<Runtime<T>>>) -> Result<()> {
        let join_handles: SmallVec<[thread::JoinHandle<Result<()>>; 8]> = runtime.write().join_handles.iter_mut()
            .map(|item| { item.take().unwrap() }).collect();
        for handle in join_handles {
            handle.join().unwrap()?;
        }
        Ok(())
    }

    impl<T: SharedData> Runtime<T> {
        pub fn new(nthreads: usize) -> Result<Self> {
            let mut runtime = Runtime {
                polls: SmallVec::new(),
                wakers: SmallVec::new(),
                join_handles: SmallVec::new(),
                callbacks: SmallVec::new(),
                timer_events: SmallVec::new(),
                source_handlers: Arc::new(RwLock::new(SmallVec::new())),
                shared_data: SmallVec::new(),
                nthreads,
                next_token: 0,
                next_event_ix: 0,
            };

            for _ in 0..nthreads {
                let poll = Arc::new(RwLock::new(mio::Poll::new()?));
                runtime.polls.push(poll.clone());

                let waker = mio::Waker::new(poll.read().registry(), runtime.get_next_token())?;
                runtime.wakers.push(Arc::new(waker));

                runtime.shared_data.push(Arc::new(RwLock::new(None)));
            }

            Ok(runtime)
        }

        #[allow(dead_code)]
        pub fn register_callback(&mut self, cb: Box<Thunk>) {
            self.callbacks.push(Box::new(cb));
        }

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

            self.polls[thread_id].read().registry().register(source, source_token, interests)?;
            Ok(source_token)
        }

        // TODO: partition per poll / thread
        pub fn register_source_event_handler(&mut self, token: mio::Token, handler: Box<Handler<T>>) {
            self.source_handlers.write().push(SourceHandler {
                source_id: token,
                handler
            });
        }

        pub fn set_thread_shared_data(&mut self, thread_ix: usize, data: T) -> Arc<RwLock<Option<T>>> {
            *self.shared_data[thread_ix].write() = Some(data);
            self.shared_data[thread_ix].clone()
        }


        #[inline]
        fn trigger_timer_event(event: &mut TimerEvent, delay: time::Duration) -> bool {
            event.last_triggered = Some(event.next_trigger + delay);
            event.next_trigger += event.timer;
            (*event.handler)();
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

        fn executor_loop_factory(mut runtime: Arc<RwLock<Self>>, thread_ix: usize)
            -> impl FnOnce() -> Result<()>
        { move || { 
            let poll = runtime.read().polls[thread_ix].clone();
            let handlers = runtime.read().source_handlers.clone();
            let mut shared_data = runtime.read().shared_data[thread_ix].clone();
            let mut events = mio::Events::with_capacity(1000);

            for cb in &runtime.read().callbacks {
                cb();
            }
            
            #[allow(unused_labels)]
            'event_loop: loop {
                poll.write().poll(&mut events, None)?;
                for event in events.iter() {
                    for handler in handlers.read().iter().filter(|handler| { handler.source_id == event.token() }) {
                        (*handler.handler)(&mut runtime, &mut shared_data, event)?;
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
                let handle = thread::spawn(Self::executor_loop_factory(runtime.clone(), i));
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

fn udp_handler_factory() -> Box<runtime::Handler<SharedData>> 
{ 
    // TODO: Must exhaust socket
    Box::new(move |_runtime, shared_data, event| {
        let mut buf = [0u8; 1500];
        let udp_socket = shared_data.write().take().unwrap().udp_socket;
        
        if event.is_readable() {
            let (nread, from) = udp_socket.recv_from(&mut buf)?;
            println!("Received message: \"{:?}\" from: {}", &buf[..nread], from)
        }
        Ok(())
    })
}

struct SharedData {
    udp_socket: mio::net::UdpSocket
}

impl runtime::SharedData for SharedData {}

fn main() -> Result<()> {
    let nthreads = num_cpus::get();
    let port = 44444;
    let mut runtime = Runtime::<SharedData>::new(nthreads)?;
    // runtime.register_callback(Box::new(|i| {println!("Closure: Thread {}: Hello world", i)}));
    // runtime.register_callback(Box::new(any_func));

    for i in 0..nthreads {
        use socket2::{Socket, Domain, Protocol, Type};
        let sock = Socket::new(Domain::ipv6(), Type::dgram().non_blocking(), Some(Protocol::udp()))?;
        cfg_if::cfg_if! {
            if #[cfg(all(unix, 
                not(any(target_os = "solaris", target_os = "illumos"))))] {
                sock.set_reuse_port(true)?;
            } else {
            sock.set_reuse_address(true);
            }
        }
        sock.bind(&format!("[::0]:{}", port).parse::<std::net::SocketAddr>()?.into())?;
        let mut mio_sock = UdpSocket::from_std(sock.into_udp_socket());
        let event_source_id = runtime.register_event_source(&mut mio_sock, None, i)?;
        runtime.register_source_event_handler(event_source_id, udp_handler_factory());
        runtime.set_thread_shared_data(i, SharedData { udp_socket: mio_sock });
    }

    let runtime = runtime.start();

    let now = time::Instant::now();
    let timer = Duration::from_millis(100);
    runtime::register_timer_event(&runtime, timer, false, Box::new(move || {
        println!("Printing every {:?}, elapsed: {}", timer, now.elapsed().as_millis());
    }));

    // temporary
    let send_sock = std::net::UdpSocket::bind("[::0]:0")?;
    let msg = "Hello world".as_bytes();
    send_sock.send_to(msg, format!("[::0]:{}", port))?;

    runtime::block_until_finished(runtime)?;
    Ok(())
}
