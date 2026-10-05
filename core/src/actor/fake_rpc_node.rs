// A JSON-RPC node for actor tests. A blocking std server on its own thread, so the fake needs
// no async IO features; one request per connection keeps the HTTP handling to a single read
// and write. `answer` maps a method and its params to a result, or to a JSON-RPC error code.
use std::io::{BufRead, BufReader, Read, Write};

use serde_json::Value;

pub(crate) fn spawn_fake_rpc_node(
    mut answer: impl FnMut(&str, &Value) -> Result<Value, i64> + Send + 'static,
) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds");
    let url = format!("http://{}", listener.local_addr().expect("address"));
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.expect("accepts");
            let mut reader = BufReader::new(stream.try_clone().expect("clones"));
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).expect("reads header");
                let line = line.trim_end().to_ascii_lowercase();
                if line.is_empty() {
                    break;
                }
                if let Some(value) = line.strip_prefix("content-length:") {
                    content_length = value.trim().parse().expect("length");
                }
            }
            let mut body = vec![0; content_length];
            reader.read_exact(&mut body).expect("reads body");
            let request: Value = serde_json::from_slice(&body).expect("json");
            let method = request["method"].as_str().expect("method");
            let response = match answer(method, &request["params"]) {
                Ok(result) => serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": result }),
                Err(code) => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": { "code": code, "message": "fake node error" },
                }),
            }
            .to_string();
            // A client that dropped its request (a try_join whose other call failed) has
            // closed the socket; the node must outlive that and answer the next one.
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                response.len()
            );
        }
    });
    url
}
