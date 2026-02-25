mod named_pipe_server;
mod splice;

use clap::Parser;
use splice::splice;
use tokio::{
    io,
    net::{TcpListener, TcpStream, windows::named_pipe::{NamedPipeClient, NamedPipeServer}},
};
use tracing::*;
use std::thread;
use std::time::Duration;

/// Implement the `TryRead` trait for our structures  
/// Remember that this is just needed because Tokio
/// does not provide traits for the below functions
impl splice::Readable for NamedPipeServer {
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        Self::try_read(self, buf)
    }
    async fn readable(&self) -> io::Result<()> {
        Self::readable(self).await
    }
}
impl splice::Readable for NamedPipeClient {
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        Self::try_read(self, buf)
    }
    async fn readable(&self) -> io::Result<()> {
        Self::readable(self).await
    }
}

/// Implement the `TryRead` trait for our structures  
/// Remember that this is just needed because Tokio
/// does not provide traits for the below functions
impl splice::Readable for TcpStream {
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        return Self::try_read(self, buf);
    }
    async fn readable(&self) -> io::Result<()> {
        Self::readable(self).await
    }
}

/// Named Pipe to TCP Server Proxy.  
/// The primary use case is for example a Docker Engine Socket
/// Which on Windows might be listening to the Windows File Socket  
/// But behind the scenes, could actually be proxied to an indepedent
/// TCP client without setting DOCKER_HOST on terminals etc.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the Windows Named Pipe to listen on
    #[arg(long, required = true)]
    pipe: String,

    /// Name of the TCP Server to connect to
    #[arg(long, required = true)]
    tcp: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Args::parse();
        tracing_subscriber::fmt::fmt()
        // uses RUST_LOG env for filtering log levels and namespaces
        //.with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    use named_pipe_server::NamedPipeServerManager;
    info!("Using pipe: {}", &args.pipe);
    info!("Binding {}", &args.tcp);
    let tcp_listener = TcpListener::bind(&args.tcp).await.unwrap();
    loop {
        info!("Beginning check...");

        match tcp_listener.accept().await {
            Ok((mut _socket, addr)) => {
                println!("new client: {:?}", addr);
                let mut server = NamedPipeServerManager::new(&args.pipe);
                let mut pipe_stream = server.accept().await.unwrap();
                let mut buf = vec![0; 1024 * 8];
                // Note that even if this returns an error we do not really care
                // The only reason the let exists here is so we do not get warnings
                let _ = splice(&mut pipe_stream, &mut _socket, &mut buf).await;
        },
            Err(e) => println!("couldn't get client: {:?}", e),
        }
        info!("sleeping 1 second before accepting a new tcp conn");
        thread::sleep(Duration::new(1, 0))
    }
}
