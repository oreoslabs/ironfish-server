use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum OreoError {
    #[error("The account `{0}` is already imported")]
    Duplicate(String),
    #[error("The account `{0}` is not imported yet")]
    NoImported(String),
    #[error("The account `{0}` is not scanned yet")]
    Scanning(String),
    #[error("The node is not synced yet")]
    Syncing,
    #[error("Internal db error")]
    DBError,
    #[error("Internal Ironfish rpc error")]
    InternalRpcError,
    #[error("The `{0}` spend circuit can not generate proof")]
    GenerateSpendProofFailed(u32),
    #[error("The `{0}` output circuit can not generate proof")]
    GenerateOutputProofFailed(u32),
    #[error("The `{0}` mint asset circuit can not generate proof")]
    GenerateMintAssetProofFailed(u32),
    #[error("Balance not enough")]
    BalanceNotEnough,
}

impl IntoResponse for OreoError {
    fn into_response(self) -> Response {
        let (status_code, err_msg) = match self {
            OreoError::DBError => (StatusCode::from_u16(600).unwrap(), self.to_string()),
            OreoError::Duplicate(_) => (StatusCode::from_u16(601).unwrap(), self.to_string()),
            OreoError::NoImported(_) => (StatusCode::from_u16(602).unwrap(), self.to_string()),
            OreoError::Scanning(_) => (StatusCode::from_u16(603).unwrap(), self.to_string()),
            OreoError::Syncing => (StatusCode::from_u16(604).unwrap(), self.to_string()),
            OreoError::InternalRpcError => (StatusCode::from_u16(605).unwrap(), self.to_string()),
            OreoError::GenerateSpendProofFailed(_) => {
                (StatusCode::from_u16(606).unwrap(), self.to_string())
            }
            OreoError::GenerateOutputProofFailed(_) => {
                (StatusCode::from_u16(607).unwrap(), self.to_string())
            }
            OreoError::GenerateMintAssetProofFailed(_) => {
                (StatusCode::from_u16(608).unwrap(), self.to_string())
            }
            OreoError::BalanceNotEnough => (StatusCode::from_u16(609).unwrap(), self.to_string()),
        };
        Json(json!({"code": status_code.as_u16(), "error": err_msg})).into_response()
    }
}
