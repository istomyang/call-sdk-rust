use net_support::{NetSupport, TcpNetSupport, UdpNetSupport};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Formatter,
    future::Future,
    net::{IpAddr, SocketAddr},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
    vec,
};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time::{Duration, Instant},
};

mod net_support;

extern crate serde;

// ==== Static Variables =====

static mut SENDER: Option<&mut mpsc::Sender<RequestFuture>> = None;
static mut SERVICE_NAME: Option<&mut str> = None;
static mut SERVICE_NUMBER: Option<&mut str> = None;
static mut COUNT: AtomicU64 = AtomicU64::new(0);

// ==== Init ====

pub async fn run<S>(
    service_name: &str,
    service_number: &str,
    weight: u8,
    remote_addr: &str,
    timeout: Duration,
    server_handler: S,
    mut closed: broadcast::Receiver<()>,
) -> Result<(), Error>
where
    S: ServerSupport + 'static,
{
    unsafe {
        let name = service_name.to_owned();
        let number = service_number.to_owned();
        SERVICE_NAME = Some(name.leak());
        SERVICE_NUMBER = Some(number.leak());
    }

    let sender;
    let reg = RegisterRequest {
        service_name: service_name.to_owned(),
        service_number: service_number.to_owned(),
        weight,
    };
    let (mut svr_rx, svr_tx);

    if is_private_addr(remote_addr) {
        let core = UdpNetSupport::new(remote_addr.to_owned());
        let mut ins = Runner::new(core, reg, timeout);
        sender = ins.get_client_sender();
        (svr_rx, svr_tx) = ins.get_server_commander();
        let closed_clone: broadcast::Receiver<()> = closed.resubscribe();
        tokio::spawn(async move {
            ins.run(closed_clone).await.unwrap();
        });
    } else {
        let core = TcpNetSupport::new(remote_addr.to_owned());
        let mut ins = Runner::new(core, reg, timeout);
        sender = ins.get_client_sender();
        (svr_rx, svr_tx) = ins.get_server_commander();
        let closed_clone = closed.resubscribe();
        tokio::spawn(async move {
            ins.run(closed_clone).await.unwrap();
        });
    }

    let (send_tx, mut send_rx) = mpsc::channel(32);
    unsafe {
        let tx_box = Box::new(send_tx);
        SENDER = Some(Box::leak(tx_box));
    }

    // handle server
    {
        let mut closed_clone = closed.resubscribe();
        tokio::spawn(async move {
            loop {
                if let Ok(msg) = svr_rx.try_recv() {
                    let tx = svr_tx.clone();
                    let mut s = server_handler.clone();
                    // spawn a new task for each handle.
                    tokio::spawn(async move {
                        let mut send_msg = Message {
                            id: get_msg_id(),
                            request_id: msg.request_id,
                            from_service_name: msg.to_service_name,
                            from_service_no: msg.to_service_no.unwrap(),
                            to_service_name: msg.from_service_name,
                            to_service_no: Some(msg.from_service_no),
                            lb_algo: msg.lb_algo,
                            is_request: false,
                            write_op: msg.write_op,
                            data: vec![],
                            ..msg
                        };
                        match s.handle(msg.data).await {
                            Ok(data) => {
                                send_msg.success = Some(true);
                                send_msg.description = None;
                                send_msg.data = data;
                            }
                            Err(e) => {
                                send_msg.success = Some(false);
                                send_msg.description = Some(e.to_string());
                            }
                        }
                        let _ = tx.send(send_msg);
                    });
                }

                // close
                match closed_clone.try_recv() {
                    Ok(_) => {
                        break;
                    }
                    _ => { /* go on */ }
                }
            }
        });
    }

    loop {
        // client send request.
        match send_rx.try_recv() {
            Ok(data) => {
                sender.send(data).await?;
            }
            _ => { /* go on */ }
        }

        // close
        match closed.try_recv() {
            Ok(_) => {
                break;
            }
            _ => { /* go on */ }
        }
    }
    Ok(())
}

// ==== Client =====

pub async fn send(req: Request) -> Result<Response, Error> {
    let msg = Message::from(req);
    let (tx, rx) = oneshot::channel();
    let fu = RequestFuture {
        msg_id: msg.id.clone(),
        req_msg: Some(msg),
        result_tx: tx,
        created_at: Instant::now(),
    };
    unsafe {
        SENDER.as_ref().unwrap().send(fu).await?;
    }
    match rx.await {
        Ok(resp) => match resp {
            Ok(r) => Ok(r),
            Err(e) => Err(e),
        },
        Err(err) => Err(Error::new(err.to_string())),
    }
}

// ==== Server =====

pub type YourTypeBytes = Vec<u8>;

pub trait ServerSupport: Send + Clone {
    fn handle(
        &mut self,
        req: YourTypeBytes,
    ) -> impl Future<Output = Result<YourTypeBytes, Error>> + Send;
}

// ==== Runner =====

trait RunnerTrait {
    fn run(
        &mut self,
        closed: broadcast::Receiver<()>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn get_client_sender(&mut self) -> mpsc::Sender<RequestFuture>;

    fn get_server_commander(&mut self) -> (oneshot::Receiver<Message>, mpsc::Sender<Message>);
}

struct RequestFuture {
    msg_id: String,
    req_msg: Option<Message>,
    result_tx: oneshot::Sender<Result<Response, Error>>,
    created_at: Instant,
}

struct Runner<T>
where
    T: NetSupport,
{
    core: Option<T>,

    futures: Vec<RequestFuture>,

    send_tx: Option<mpsc::Sender<RequestFuture>>,
    send_rx: mpsc::Receiver<RequestFuture>,

    svr_send_tx: Option<mpsc::Sender<Message>>,
    svr_send_rx: Option<mpsc::Receiver<Message>>,
    svr_recv_tx: Option<oneshot::Sender<Message>>,
    svr_recv_rx: Option<oneshot::Receiver<Message>>,

    reg: RegisterRequest,

    timeout: Duration,
}

impl<T> Runner<T>
where
    T: NetSupport,
{
    fn new(core: T, reg: RegisterRequest, timeout: Duration) -> Self {
        let (send_tx, send_rx) = mpsc::channel(32);
        let (svr_send_tx, svr_send_rx) = mpsc::channel(1024);
        let (svr_recv_tx, svr_recv_rx) = oneshot::channel();
        Self {
            core: Some(core),
            futures: vec![],
            send_tx: Some(send_tx),
            send_rx,
            svr_send_tx: Some(svr_send_tx),
            svr_send_rx: Some(svr_send_rx),
            svr_recv_tx: Some(svr_recv_tx),
            svr_recv_rx: Some(svr_recv_rx),
            reg,
            timeout,
        }
    }
}

impl<T> RunnerTrait for Runner<T>
where
    T: NetSupport + Send + 'static,
{
    async fn run(&mut self, mut closed: broadcast::Receiver<()>) -> Result<(), Error> {
        let mut core = self.core.take().unwrap();
        let (tx, mut rx) = core.get_commander();
        let close_clone = closed.resubscribe();
        tokio::spawn(async move {
            core.run(close_clone).await.unwrap();
        });

        // register
        {
            tx.send((&self.reg).into()).await?;
            if let Some(data) = rx.blocking_recv() {
                let res = RegisterResponse::from(data);
                if !res.success {
                    return Err(Error::new(res.description));
                }
            }
        }

        let mut t0 = Instant::now();

        loop {
            // from client to send request.
            match self.send_rx.try_recv() {
                Ok(mut data) => {
                    // send and save the future.
                    let req = data.req_msg.take().unwrap();
                    tx.send(req.into()).await?;
                    self.futures.push(data);
                }
                _ => { /* go on */ }
            }

            // from core msg.
            match rx.try_recv() {
                Ok(raw) => {
                    let msg = Message::from(raw);

                    // maybe remote request.
                    if msg.is_request {
                        let _ = self.svr_recv_tx.take().unwrap().send(msg);
                    }
                    // maybe response.
                    else {
                        let idx = self
                            .futures
                            .iter()
                            .position(|f| f.msg_id == msg.id)
                            .unwrap();
                        let a = self.futures.remove(idx);
                        let _ = a.result_tx.send(Ok(msg.into()));
                    }
                }
                _ => { /* go on */ }
            }

            // response send to remote.
            match self.svr_send_rx.take().unwrap().try_recv() {
                Ok(a) => {
                    tx.send(a.into()).await?;
                }
                _ => { /* go on */ }
            }

            // handle timeout
            if t0 + self.timeout < Instant::now() {
                let mut idx = vec![];
                for (i, f) in self.futures.iter().enumerate() {
                    if t0 + self.timeout < f.created_at + self.timeout {
                        idx.push(i);
                    }
                }
                for i in idx {
                    let a = self.futures.remove(i);
                    let _ = a.result_tx.send(Err(Error::new("timeout".to_string())));
                }
                t0 = Instant::now();
            }

            // handle close.
            match closed.try_recv() {
                Ok(_) => {
                    break;
                }
                _ => { /* go on */ }
            }
        }

        Ok(())
    }

    fn get_client_sender(&mut self) -> mpsc::Sender<RequestFuture> {
        self.send_tx.take().unwrap()
    }

    fn get_server_commander(&mut self) -> (oneshot::Receiver<Message>, mpsc::Sender<Message>) {
        (
            self.svr_recv_rx.take().unwrap(),
            self.svr_send_tx.take().unwrap(),
        )
    }
}

// ==== Utils ====

fn is_private_addr(addr: &str) -> bool {
    match addr.parse::<SocketAddr>().unwrap().ip() {
        IpAddr::V4(ipv4) => {
            // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
            ipv4.octets()[0] == 10
                || (ipv4.octets()[0] == 172 && ipv4.octets()[1] >= 16 && ipv4.octets()[1] <= 31)
                || (ipv4.octets()[0] == 192 && ipv4.octets()[1] == 168)
        }
        IpAddr::V6(ipv6) => {
            // fc00::/7
            ipv6.segments()[0] >= 0xfc00 && ipv6.segments()[0] <= 0xfdff
        }
    }
}

fn get_msg_id() -> String {
    let name = unsafe { SERVICE_NAME.as_ref().unwrap() };
    let no = unsafe { SERVICE_NUMBER.as_ref().unwrap() };
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let count = unsafe { COUNT.fetch_add(1, Ordering::Relaxed).wrapping_add(1) };
    format!("{}:{}_m_{}_{}", name, no, now, count)
}

fn get_request_id() -> String {
    let name = unsafe { SERVICE_NAME.as_ref().unwrap() };
    let no = unsafe { SERVICE_NUMBER.as_ref().unwrap() };
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let count = unsafe { COUNT.fetch_add(1, Ordering::Relaxed).wrapping_add(1) };
    format!("{}:{}_r_{}_{}", name, no, now, count)
}

// ==== Types ====

pub struct Request {
    pub service_name: String,
    pub service_number: Option<String>,
    pub load_balance_algo: LBAlgo,
    pub write_op: bool,
    pub data: Vec<u8>,
}

impl From<Message> for Request {
    fn from(value: Message) -> Self {
        Self {
            service_name: value.from_service_name,
            service_number: Some(value.from_service_no),
            load_balance_algo: value.lb_algo,
            write_op: value.write_op,
            data: value.data,
        }
    }
}

pub struct Response {
    pub service_name: String,
    pub service_number: String,
    pub success: bool,
    pub description: Option<String>,
    pub data: Vec<u8>,
}

impl From<Message> for Response {
    fn from(value: Message) -> Self {
        Self {
            service_name: value.to_service_name,
            service_number: value.to_service_no.unwrap(),
            success: value.success.unwrap(),
            description: value.description,
            data: value.data,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum LBAlgo {
    #[serde(rename = "RR")]
    RoundRobin,
    #[serde(rename = "WRR")]
    WeightedRoundRobin,
    #[serde(rename = "LC")]
    LeastConnection,
    #[serde(rename = "WLC")]
    WeightedLeastConnection,
    #[serde(rename = "SIPH")]
    SourceIpHashing,
    #[serde(rename = "Random")]
    Random,
    #[serde(rename = "RT")]
    ResponseTime,
    #[serde(rename = "P2C")]
    P2C,
}

#[derive(Serialize, Deserialize, Clone)]
struct Message {
    /// id must be unique across lifetime of this system.
    id: String,
    /// request_id must be unique across lifetime of this system.
    request_id: String,

    created_at: u64,

    from_service_name: String,
    from_service_no: String,
    to_service_name: String,
    to_service_no: Option<String>,

    /// load balance algorithm.
    /// support: "RR", "WRR", "LC", "WLC", "SIPH", "Random", "RT", "P2C"
    lb_algo: LBAlgo,

    is_request: bool,
    write_op: bool,
    success: Option<bool>,
    description: Option<String>,

    data: Vec<u8>,
}

impl Into<Vec<u8>> for Message {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl From<Vec<u8>> for Message {
    fn from(value: Vec<u8>) -> Self {
        serde_json::from_slice(&value).unwrap()
    }
}

impl From<Request> for Message {
    fn from(value: Request) -> Self {
        let from_name = unsafe { SERVICE_NAME.as_ref().unwrap() };
        let from_no = unsafe { SERVICE_NUMBER.as_ref().unwrap() };
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Message {
            id: get_msg_id(),
            request_id: get_request_id(),
            from_service_name: from_name.to_string(),
            from_service_no: from_no.to_string(),
            to_service_name: value.service_name,
            to_service_no: value.service_number,
            lb_algo: value.load_balance_algo,
            is_request: true,
            write_op: value.write_op,
            success: None,
            description: None,
            data: value.data,
            created_at: t,
        }
    }
}

#[derive(Debug)]
pub struct Error {
    pub msg: String,
}

impl Error {
    pub fn new(msg: String) -> Self {
        Error { msg }
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "error(call-rust-sdk): {}", self.msg)
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Error { msg: e.to_string() }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error { msg: e.to_string() }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct RegisterRequest {
    service_name: String,
    service_number: String,
    weight: u8,
}

impl Into<Vec<u8>> for &RegisterRequest {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct RegisterResponse {
    success: bool,
    description: String,
}

impl From<Vec<u8>> for RegisterResponse {
    fn from(value: Vec<u8>) -> Self {
        serde_json::from_slice(&value).unwrap()
    }
}

#[cfg(test)]
mod tests {
    mod example;

    use std::sync::atomic::{AtomicU64, Ordering};

    use serde::{Deserialize, Serialize};

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn my_test() {
        let atomic = AtomicU64::new(u64::MAX);
        let new = atomic.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        println!("111111: {}", new);
    }

    #[derive(Serialize, Deserialize)]
    struct A {
        a: Option<i32>,
    }

    #[test]
    fn encode() {
        let a = A { a: None };
        let v = serde_json::to_vec(&a).unwrap();
        println!("{:?}", String::from_utf8_lossy(v.as_slice()));
    }
}
