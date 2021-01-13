use num_cpus;
use std::time::{self, Duration};
use mio::{self, net::UdpSocket};
use anyhow::Result;
use cfg_if;

mod runtime;
use runtime::*;

// fn any_func(ix: usize) {
//     println!("Function: Thread {}: Hello world", ix);
// }

fn udp_handler_factory() -> Box<runtime::SourceEvHandler<ThreadData>> 
{ 
    Box::new(move |_runtime, thread_data, _event| {
        let mut buf = [0u8; 1500];

        loop {
            let (nread, from) = match thread_data.udp_socket.recv_from(&mut buf) {
                Err(err @ std::io::Error { .. }) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                pass => pass
            }?;
            println!("Thread {}: Received message: \"{}\" from: {}", thread_data.thread_ix,
                String::from_utf8_lossy(&buf[..nread]), from);
        }
        Ok(())
    })
}

struct ThreadData {
    thread_ix: usize,
    udp_socket: mio::net::UdpSocket
}

fn main() -> Result<()> {
    let nthreads = num_cpus::get();
    let port = 44444;
    let mut runtime = Runtime::<ThreadData>::new(nthreads)?;
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
        runtime.set_thread_data(i, ThreadData { thread_ix: i, udp_socket: mio_sock });
    }

    let runtime = runtime.start();

    let now = time::Instant::now();
    let timer = Duration::from_millis(1000);
    runtime::register_timer_event(&runtime, timer, true, Box::new(move |_runtime| {
        println!("Printing every {:?}, elapsed: {}", timer, now.elapsed().as_millis());
        Ok(())
    }));

    // temporary
    let send_sock = std::net::UdpSocket::bind("[::0]:0")?;
    let msg = "Hello world".as_bytes();

    let now = time::Instant::now();
    runtime::register_timer_event(&runtime, Duration::from_millis(500), true, Box::new(move |_runtime| {
        // println!("Sending message, elapsed: {}", now.elapsed().as_millis());
        send_sock.send_to(msg, format!("[::0]:{}", port))?;
        Ok(())
    }));

    runtime::block_until_finished(runtime)?;
    Ok(())
}
