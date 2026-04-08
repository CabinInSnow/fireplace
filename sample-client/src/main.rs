use cabin_fireplace::network::protocol::{ActionRequest, AuthRequest, WsRequest, WsResponse};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// WebSocket endpoint URL
    #[arg(short, long, default_value = "ws://127.0.0.1:80/ws")]
    url: String,

    /// Login ID for Interactive Mode
    #[arg(short, long)]
    id: Option<String>,

    /// Number of concurrent connections for Stress Mode
    #[arg(short, long)]
    stress: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    cabin_fireplace::utils::init_logger(tracing::Level::INFO);

    let args = Args::parse();

    if let Some(count) = args.stress {
        tracing::info!("Starting Stress Mode with {} connections to {}", count, args.url);
        run_stress_mode(args.url.clone(), count).await;
    } else if let Some(id) = args.id {
        tracing::info!("Starting Interactive Mode as user '{}' to {}", id, args.url);
        run_interactive_mode(args.url.clone(), id).await;
    } else {
        tracing::error!("You must provide either --id <login_id> or --stress <count>");
    }

    Ok(())
}

enum InputState {
    ActionName,
    Payload { action_name: String },
}

async fn run_interactive_mode(url: String, id: String) {
    let (ws_stream, _) = connect_async(&url).await.expect("Failed to connect to server");
    tracing::info!("WebSocket connected");

    let (mut write, mut read) = ws_stream.split();

    // 1. Send Auth Request
    let auth_req = AuthRequest {
        login_type: 1,
        login_id: id.clone(),
        device_id: format!("device_{}", id),
    };
    let ws_req = WsRequest::Auth(auth_req);
    write
        .send(Message::Text(serde_json::to_string(&ws_req).unwrap().into()))
        .await
        .expect("Failed to send auth request");

    let client_seqnum = Arc::new(AtomicU64::new(0));
    
    // 2. Channel for stdin
    let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<String>(1);
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = tokio::io::BufReader::new(stdin);
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).await.is_err() { break; }
            if stdin_tx.send(line).await.is_err() { break; }
        }
    });

    println!("============================================================");
    println!("Interactive Mode Ready.");
    println!("1. Type action name (e.g. give_item)");
    println!("2. Type raw JSON payload (e.g. {{\"target_uid\": 1002, ...}})");
    println!("============================================================");

    let mut input_state = InputState::ActionName;
    
    fn print_prompt(state: &InputState) {
        use std::io::Write;
        match state {
            InputState::ActionName => print!("Action: "),
            InputState::Payload { .. } => print!("Payload (raw JSON): "),
        }
        std::io::stdout().flush().unwrap();
    }

    print_prompt(&input_state);

    loop {
        tokio::select! {
            // Handle WebSocket messages
            ws_msg = read.next() => {
                match ws_msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(res) = serde_json::from_str::<WsResponse>(&text) {
                            match res {
                                WsResponse::Auth(a) => {
                                    let uid_str = cabin_fireplace::utils::snowflake_to_string(a.uid);
                                    tracing::info!("Auth Success: uid={}, display_id={}, nickname={}, sid={}", uid_str, a.display_id, a.nickname, a.sid);
                                }
                                WsResponse::Init(i) => {
                                    client_seqnum.store(i.seqnum, Ordering::SeqCst);
                                    tracing::info!("Init Snapshot:\n{}", serde_json::to_string_pretty(&i.data).unwrap());
                                }
                                WsResponse::Action(a) => {
                                    client_seqnum.store(a.seqnum, Ordering::SeqCst);
                                    tracing::info!("Action Response (seq={}):\n{}", a.seqnum, serde_json::to_string_pretty(&a.data).unwrap());
                                }
                                WsResponse::Sync(s) => {
                                    client_seqnum.store(s.seqnum, Ordering::SeqCst);
                                    tracing::info!(">>> SYNC EVENT (seq={}) <<<\n{}", s.seqnum, serde_json::to_string_pretty(&s.sync_events).unwrap());
                                }
                                WsResponse::Error { error } => tracing::error!("Error {}: {}", error.code, error.message),
                            }
                        }
                        // After any message, reprint the current prompt
                        print_prompt(&input_state);
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        tracing::info!("Server closed connection");
                        break;
                    }
                    Some(Err(e)) => {
                        tracing::error!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
            // Handle Stdin input
            Some(line) = stdin_rx.recv() => {
                let trimmed = line.trim();
                match input_state {
                    InputState::ActionName => {
                        if trimmed.is_empty() {
                            print_prompt(&input_state);
                            continue;
                        }
                        input_state = InputState::Payload { action_name: trimmed.to_string() };
                        print_prompt(&input_state);
                    }
                    InputState::Payload { action_name } => {
                        let payload: Value = if trimmed.is_empty() {
                            serde_json::json!({})
                        } else {
                            match serde_json::from_str(trimmed) {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::error!("Invalid JSON: {}", e);
                                    input_state = InputState::ActionName;
                                    print_prompt(&input_state);
                                    continue;
                                }
                            }
                        };
                        
                        let action_req = ActionRequest {
                            seqnum: client_seqnum.load(Ordering::SeqCst),
                            action: action_name,
                            params: payload,
                        };
                        
                        let ws_req = WsRequest::Action(action_req);
                        if let Err(e) = write.send(Message::Text(serde_json::to_string(&ws_req).unwrap().into())).await {
                            tracing::error!("Failed to send: {}", e);
                            break;
                        }
                        
                        input_state = InputState::ActionName;
                        print_prompt(&input_state);
                    }
                }
            }
        }
    }
}

async fn run_stress_mode(url: String, count: usize) {
    let active_connections = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 1..=count {
        let u = url.clone();
        let active = active_connections.clone();
        
        let handle = tokio::spawn(async move {
            let id = format!("stress_test_{:04}", i);
            let ws_stream = match connect_async(&u).await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    tracing::error!("Client {} failed to connect: {}", id, e);
                    return;
                }
            };
            
            active.fetch_add(1, Ordering::SeqCst);
            let (mut write, mut read) = ws_stream.split();

            // Auth
            let auth_req = AuthRequest {
                login_type: 1,
                login_id: id.clone(),
                device_id: format!("device_{}", id),
            };
            let _ = write.send(Message::Text(serde_json::to_string(&WsRequest::Auth(auth_req)).unwrap().into())).await;

            let mut seq = 0;
            loop {
                tokio::select! {
                    msg = read.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                if let Ok(res) = serde_json::from_str::<WsResponse>(&text) {
                                    match res {
                                        WsResponse::Init(i) => seq = i.seqnum,
                                        WsResponse::Action(a) => seq = a.seqnum,
                                        WsResponse::Sync(s) => seq = s.seqnum,
                                        _ => {}
                                    }
                                }
                            }
                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                active.fetch_sub(1, Ordering::SeqCst);
                                break;
                            }
                            _ => {} // Ignore other messages for now
                        }
                    }
                    _ = sleep(Duration::from_millis(1000)) => {
                        let action = ActionRequest {
                            seqnum: seq,
                            action: "ping".to_string(),
                            params: serde_json::json!({}),
                        };
                        if write.send(Message::Text(serde_json::to_string(&WsRequest::Action(action)).unwrap().into())).await.is_err() {
                            active.fetch_sub(1, Ordering::SeqCst);
                            break;
                        }
                    }
                }
            }
        });
        handles.push(handle);
        sleep(Duration::from_millis(10)).await;
    }

    loop {
        tracing::info!("Active stress connections: {}", active_connections.load(Ordering::SeqCst));
        sleep(Duration::from_secs(5)).await;
    }
}
