mod codec;

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::{
    io::split,
    net::TcpStream,
    sync::{
        mpsc::{self, Sender},
        oneshot, RwLock,
    },
    time::timeout,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, error, info, warn};

use crate::{db_handler::PgHandler, manager::codec::DMessage, SharedState};

use self::codec::{DMessageCodec, DRequest};

#[derive(Debug, Clone)]
pub struct ServerWorker {
    pub router: Sender<DRequest>,
    // 1: Idle; 2: Busy
    pub status: u8,
}

impl ServerWorker {
    pub fn new(router: Sender<DRequest>) -> Self {
        Self { router, status: 1 }
    }
}

#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub timestampt: u64,
    pub hash: String,
}

#[derive(Debug, Clone)]
pub struct Manager {
    pub workers: Arc<RwLock<HashMap<String, ServerWorker>>>,
    pub task_queue: Arc<RwLock<Vec<DRequest>>>,
    pub task_mapping: Arc<RwLock<HashMap<String, TaskInfo>>>,
    pub shared: Arc<SharedState<PgHandler>>,
}

impl Manager {
    pub fn new(shared: Arc<SharedState<PgHandler>>) -> Arc<Self> {
        Arc::new(Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            task_queue: Arc::new(RwLock::new(vec![])),
            task_mapping: Arc::new(RwLock::new(HashMap::new())),
            shared,
        })
    }

    pub async fn handle_stream(stream: TcpStream, server: Arc<Self>) -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<DRequest>(1024);
        let mut worker_name = stream.peer_addr().unwrap().clone().to_string();
        let (r, w) = split(stream);
        let mut outbound_w = FramedWrite::new(w, DMessageCodec::default());
        let mut outbound_r = FramedRead::new(r, DMessageCodec::default());
        let (router, handler) = oneshot::channel();
        let mut timer = tokio::time::interval(Duration::from_secs(300));
        let _ = timer.tick().await;

        let worker_server = server.clone();
        tokio::spawn(async move {
            let _ = router.send(());
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        debug!("no message from worker {} for 5 mins, exit", worker_name);
                        let _ = worker_server.workers.write().await.remove(&worker_name).unwrap();
                        break;
                    },
                    Some(request) = rx.recv() => {
                        debug!("new message from rx for worker {}", worker_name);
                        let _ = worker_server.workers.write().await.get_mut(&worker_name).unwrap().status = 2;
                        let send_future = outbound_w.send(DMessage::DRequest(request));
                        if let Err(error) = timeout(Duration::from_millis(200), send_future).await {
                            debug!("send message to worker timeout: {}", error);
                        }

                    },
                    result = outbound_r.next() => {
                        debug!("new message from outboud_reader {:?} of worker {}", result, worker_name);
                        match result {
                            Some(Ok(message)) => {
                                timer.reset();
                                match message {
                                    DMessage::RegisterWorker(register) => {
                                        debug!("heart beat info {:?}", register);
                                        match worker_name == register.name {
                                            true => {},
                                            false => {
                                                let mut worker = ServerWorker::new(tx.clone());
                                                worker_name = register.name;
                                                info!("new worker: {}", worker_name.clone());
                                                match worker_server.task_queue.write().await.pop() {
                                                    Some(task) => {
                                                        let _ = tx.send(task).await.unwrap();
                                                        worker.status = 2;
                                                    },
                                                    None => {},
                                                }
                                                let _ = worker_server.workers.write().await.insert(worker_name.clone(), worker);
                                            }
                                        }
                                    },
                                    DMessage::DRequest(_) => error!("invalid message from worker, should never happen"),
                                    DMessage::DResponse(response) => {
                                        let task_id = response.id.clone();
                                        info!("task {} response from worker {}", task_id, worker_name);
                                        // handle decryption result
                                        match worker_server.task_queue.write().await.pop() {
                                            Some(task) => {
                                                let _ = tx.send(task).await.unwrap();
                                            },
                                            None => worker_server.workers.write().await.get_mut(&worker_name).unwrap().status = 1,
                                        }
                                    },
                                }
                            },
                            _ => {
                                warn!("unknown message");
                                let _ = worker_server.workers.write().await.remove(&worker_name).unwrap();
                                break;
                            },
                        }
                    }
                }
            }
            error!("worker {} main loop exit", worker_name);
        });
        let _ = handler.await;
        Ok(())
    }
}
