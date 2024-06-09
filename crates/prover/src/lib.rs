use std::{net::SocketAddr, time::Duration};

use axum::{
    error_handling::HandleErrorLayer,
    http::StatusCode,
    routing::{get, post},
    BoxError, Router,
};
use tokio::net::TcpListener;
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::handlers::{generate_proof_handler, health_check_handler};

pub mod handlers;

pub async fn run_prover(listen: SocketAddr) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/generateProofs", post(generate_proof_handler))
        .route("/healthCheck", get(health_check_handler))
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
    info!("Prover listening on {}", listen);
    axum::serve(listener, router).await?;
    Ok(())
}
