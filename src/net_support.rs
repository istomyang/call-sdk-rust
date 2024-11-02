use std::future::Future;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, UdpSocket},
    sync::{broadcast, mpsc},
};

use crate::Error;

pub(crate) trait NetSupport {
    fn run(
        &mut self,
        closed: broadcast::Receiver<()>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn get_commander(&mut self) -> (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>);
}

pub(crate) struct TcpNetSupport {
    remote_addr: String,

    send_tx: Option<mpsc::Sender<Vec<u8>>>,
    send_rx: mpsc::Receiver<Vec<u8>>,
    recv_tx: mpsc::Sender<Vec<u8>>,
    recv_rx: Option<mpsc::Receiver<Vec<u8>>>,
}

impl TcpNetSupport {
    pub(crate) fn new(remote_addr: String) -> Self {
        let (send_tx, send_rx) = mpsc::channel(1024);
        let (recv_tx, recv_rx) = mpsc::channel(1024);
        TcpNetSupport {
            remote_addr,
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
        }
    }
}

impl NetSupport for TcpNetSupport {
    async fn run(&mut self, mut closed: broadcast::Receiver<()>) -> Result<(), Error> {
        let mut stream = TcpStream::connect(self.remote_addr.as_str()).await?;
        let mut buf = [0_u8; 8192];

        loop {
            // from remote.
            match stream.try_read(buf.as_mut()) {
                Ok(n) => {
                    self.recv_tx.send(buf[..n].to_vec()).await?;
                }
                _ => { /* go on */ }
            }

            // from upper
            match self.send_rx.try_recv() {
                Ok(data) => stream.write_all(&data).await?,
                _ => { /* go on */ }
            }

            // closed
            match closed.try_recv() {
                Ok(_) => {
                    stream.shutdown().await?;
                    break;
                }
                _ => { /* go on */ }
            }
        }

        Ok(())
    }

    fn get_commander(&mut self) -> (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }
}

pub(crate) struct UdpNetSupport {
    remote_addr: String,

    send_tx: Option<mpsc::Sender<Vec<u8>>>,
    send_rx: mpsc::Receiver<Vec<u8>>,
    recv_tx: mpsc::Sender<Vec<u8>>,
    recv_rx: Option<mpsc::Receiver<Vec<u8>>>,
}

impl UdpNetSupport {
    pub(crate) fn new(remote_addr: String) -> Self {
        let (send_tx, send_rx) = mpsc::channel(1024);
        let (recv_tx, recv_rx) = mpsc::channel(1024);
        UdpNetSupport {
            remote_addr,
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
        }
    }
}

impl NetSupport for UdpNetSupport {
    async fn run(&mut self, mut closed: broadcast::Receiver<()>) -> Result<(), Error> {
        let sock = UdpSocket::bind("0.0.0.0:0").await.map_err(Error::from)?;
        sock.connect(self.remote_addr.as_str()).await?;
        let mut buf = [0_u8; 8192];

        loop {
            // from remote.
            match sock.try_recv(buf.as_mut()) {
                Ok(n) => {
                    self.recv_tx.send(buf[..n].to_vec()).await?;
                }
                _ => { /* go on */ }
            }

            // from upper
            match self.send_rx.try_recv() {
                Ok(data) => {
                    sock.send(&data).await?;
                }
                _ => { /* go on */ }
            }

            // closed
            match closed.try_recv() {
                Ok(_) => {
                    break;
                }
                _ => { /* go on */ }
            }
        }

        Ok(())
    }

    fn get_commander(&mut self) -> (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }
}
