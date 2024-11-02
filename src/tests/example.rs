use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::{ServerSupport, YourTypeBytes};

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn example_main() {
    let (_closed_tx, closed_rx) = broadcast::channel(1);

    // client
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            let _res = crate::send(crate::Request {
                service_name: "service_name".to_string(),
                service_number: None,
                load_balance_algo: crate::LBAlgo::RoundRobin,
                write_op: false,
                data: Data {
                    method: "get".to_string(),
                    url: "a/b".to_string(),
                    params: HashMap::new(),
                    headers: HashMap::new(),
                    body: "hello".as_bytes().to_vec(),
                }
                .into(),
            })
            .await;
        }
    });

    // server
    let s = Server;

    let _ = crate::run(
        "service_name",
        "service_number",
        2,
        "127.0.0.1:12345",
        Duration::from_secs(10),
        s,
        closed_rx,
    )
    .await;
}

#[derive(Clone)]
struct Server;

impl ServerSupport for Server {
    async fn handle(&mut self, req: YourTypeBytes) -> Result<YourTypeBytes, crate::Error> {
        let data: Data = req.into();

        println!("body: {}", String::from_utf8_lossy(data.body.as_slice()));

        Ok("hello!".into())
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct Data {
    method: String,
    url: String,
    params: HashMap<String, String>,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl From<YourTypeBytes> for Data {
    fn from(value: YourTypeBytes) -> Self {
        serde_json::from_slice(&value).unwrap()
    }
}

impl Into<YourTypeBytes> for Data {
    fn into(self) -> YourTypeBytes {
        serde_json::to_vec(&self).unwrap()
    }
}
