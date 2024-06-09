use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    error_handling::HandleErrorLayer,
    http::StatusCode,
    routing::{get, post},
    BoxError, Router,
};
use db_handler::{DBHandler, PgHandler};
use networking::rpc_handler::RpcHandler;
use tokio::net::TcpListener;
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::handlers::{
    account_status_handler, broadcast_transaction_handler, create_transaction_handler,
    get_balances_handler, get_ores_handler, get_transaction_handler, get_transactions_handler,
    health_check_handler, import_account_handler, latest_block_handler, remove_account_handler,
    rescan_account_handler, update_scan_status_handler,
};

mod handlers;

#[derive(Debug, Clone)]
pub struct SharedState<T: DBHandler> {
    pub db_handler: T,
    pub rpc_handler: RpcHandler,
}

impl<T> SharedState<T>
where
    T: DBHandler,
{
    pub fn new(db_handler: T, endpoint: &str) -> Self {
        Self {
            db_handler: db_handler,
            rpc_handler: RpcHandler::new(endpoint.into()),
        }
    }
}

pub async fn run_server(
    listen: SocketAddr,
    rpc_server: String,
    db_handler: PgHandler,
) -> Result<()> {
    let shared_resource = Arc::new(SharedState::new(db_handler, &rpc_server));

    let router = Router::new()
        .route("/import", post(import_account_handler))
        .route("/remove", post(remove_account_handler))
        .route("/getBalances", post(get_balances_handler))
        .route("/getTransaction", post(get_transaction_handler))
        .route("/getTransactions", post(get_transactions_handler))
        .route("/createTx", post(create_transaction_handler))
        .route("/broadcastTx", post(broadcast_transaction_handler))
        .route("/accountStatus", post(account_status_handler))
        .route("/latestBlock", get(latest_block_handler))
        .route("/ores", post(get_ores_handler))
        .route("/rescan", post(rescan_account_handler))
        .route("/healthCheck", get(health_check_handler))
        .route("/updateScan", post(update_scan_status_handler))
        .with_state(shared_resource.clone())
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|_: BoxError| async {
                    StatusCode::REQUEST_TIMEOUT
                }))
                .layer(TimeoutLayer::new(Duration::from_secs(30))),
        )
        .layer(
            CorsLayer::new()
                .allow_methods(Any)
                .allow_origin(Any)
                .allow_headers(Any),
        );

    let listener = TcpListener::bind(&listen).await?;
    info!("Server listening on {}", listen);
    axum::serve(listener, router).await?;
    Ok(())
}
