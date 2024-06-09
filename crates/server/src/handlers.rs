use std::sync::Arc;

use axum::{
    extract::{self, State},
    response::IntoResponse,
    Json,
};
use constants::ACCOUNT_VERSION;
use db_handler::DBHandler;
use networking::{
    rpc_abi::{
        OutPut, RpcBroadcastTxRequest, RpcCreateTxRequest, RpcGetAccountStatusRequest,
        RpcGetAccountTransactionRequest, RpcGetBalancesRequest, RpcGetBalancesResponse,
        RpcGetTransactionsRequest, RpcImportAccountRequest, RpcRemoveAccountRequest,
        RpcResetAccountRequest, RpcResponse, RpcSetScanningRequest,
    },
    web_abi::{GetTransactionDetailResponse, ImportAccountRequest, RescanAccountResponse},
};
use oreo_errors::OreoError;
use serde_json::json;

use crate::SharedState;

pub async fn import_account_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(import): extract::Json<ImportAccountRequest>,
) -> impl IntoResponse {
    let account_name = shared
        .db_handler
        .save_account(import.clone().to_account(), 0)
        .await;
    if let Err(e) = account_name {
        return e.into_response();
    }
    let ImportAccountRequest {
        view_key,
        incoming_view_key,
        outgoing_view_key,
        public_address,
        created_at,
    } = import;
    let rpc_data = RpcImportAccountRequest {
        view_key,
        incoming_view_key,
        outgoing_view_key,
        public_address,
        version: ACCOUNT_VERSION,
        name: account_name.unwrap(),
        created_at,
    };
    shared.rpc_handler.import_account(rpc_data).into_response()
}

pub async fn remove_account_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(remove_account): extract::Json<RpcRemoveAccountRequest>,
) -> impl IntoResponse {
    let db_account = shared
        .db_handler
        .get_account(remove_account.account.clone())
        .await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let result = shared.rpc_handler.remove_account(RpcRemoveAccountRequest {
        account: db_account.unwrap().name,
        confirm: Some(true),
        wait: Some(true),
    });
    match result {
        Ok(response) => {
            if let Err(e) = shared
                .db_handler
                .remove_account(remove_account.account.clone())
                .await
            {
                return e.into_response();
            }
            response.into_response()
        }
        Err(e) => e.into_response(),
    }
}

pub async fn account_status_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(account): extract::Json<RpcGetAccountStatusRequest>,
) -> impl IntoResponse {
    let db_account = shared.db_handler.get_account(account.account.clone()).await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    shared
        .rpc_handler
        .get_account_status(RpcGetAccountStatusRequest {
            account: db_account.unwrap().name,
        })
        .into_response()
}

pub async fn rescan_account_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(account): extract::Json<RpcGetAccountStatusRequest>,
) -> impl IntoResponse {
    let db_account = shared.db_handler.get_account(account.account.clone()).await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let account = db_account.unwrap();
    let _ = shared.rpc_handler.set_scanning(RpcSetScanningRequest {
        account: account.name.clone(),
        enabled: false,
    });
    let _ = shared.rpc_handler.reset_account(RpcResetAccountRequest {
        account: account.name.clone(),
        reset_scanning_enabled: Some(false),
        reset_created_at: Some(false),
    });
    let _ = shared
        .db_handler
        .update_scan_status(account.address.clone(), true)
        .await;
    RpcResponse {
        status: 200,
        data: RescanAccountResponse { success: true },
    }
    .into_response()
}

pub async fn update_scan_status_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(account): extract::Json<RpcGetAccountStatusRequest>,
) -> impl IntoResponse {
    let db_account = shared.db_handler.get_account(account.account.clone()).await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let account = db_account.unwrap();
    let _ = shared.rpc_handler.set_scanning(RpcSetScanningRequest {
        account: account.name.clone(),
        enabled: true,
    });
    let _ = shared
        .db_handler
        .update_scan_status(account.address, false)
        .await;
    RpcResponse {
        status: 200,
        data: RescanAccountResponse { success: true },
    }
    .into_response()
}

pub async fn get_balances_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(get_balance): extract::Json<RpcGetBalancesRequest>,
) -> impl IntoResponse {
    let db_account = shared
        .db_handler
        .get_account(get_balance.account.clone())
        .await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let resp = shared.rpc_handler.get_balances(RpcGetBalancesRequest {
        account: db_account.unwrap().name,
        confirmations: Some(get_balance.confirmations.unwrap_or(10)),
    });
    match resp {
        Ok(res) => {
            let response = RpcResponse {
                status: 200,
                data: RpcGetBalancesResponse::verified_asset(res.data),
            };
            response.into_response()
        }
        Err(e) => e.into_response(),
    }
}

pub async fn get_ores_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(get_balance): extract::Json<RpcGetBalancesRequest>,
) -> impl IntoResponse {
    let db_account = shared
        .db_handler
        .get_account(get_balance.account.clone())
        .await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let resp = shared.rpc_handler.get_balances(RpcGetBalancesRequest {
        account: db_account.unwrap().name,
        confirmations: Some(get_balance.confirmations.unwrap_or(10)),
    });
    match resp {
        Ok(res) => {
            let response = RpcResponse {
                status: 200,
                data: RpcGetBalancesResponse::ores(res.data).await,
            };
            response.into_response()
        }
        Err(e) => e.into_response(),
    }
}

pub async fn get_transaction_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(account): extract::Json<RpcGetAccountTransactionRequest>,
) -> impl IntoResponse {
    let db_account = shared.db_handler.get_account(account.account.clone()).await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let rpc_transaction =
        shared
            .rpc_handler
            .get_account_transaction(RpcGetAccountTransactionRequest {
                account: db_account.unwrap().name,
                hash: account.hash,
                notes: Some(true),
            });
    match rpc_transaction {
        Ok(RpcResponse { data, status: _ }) => {
            let transaction_detail = GetTransactionDetailResponse::from_rpc_data(data);
            match transaction_detail {
                Ok(detail) => RpcResponse {
                    status: 200,
                    data: detail,
                }
                .into_response(),
                Err(e) => e.into_response(),
            }
        }
        Err(e) => e.into_response(),
    }
}

pub async fn get_transactions_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(get_transactions): extract::Json<RpcGetTransactionsRequest>,
) -> impl IntoResponse {
    let db_account = shared
        .db_handler
        .get_account(get_transactions.account.clone())
        .await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    shared
        .rpc_handler
        .get_transactions(RpcGetTransactionsRequest {
            account: db_account.unwrap().name,
            limit: Some(get_transactions.limit.unwrap_or(6)),
            reverse: Some(true),
        })
        .into_response()
}

pub async fn create_transaction_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(create_transaction): extract::Json<RpcCreateTxRequest>,
) -> impl IntoResponse {
    let db_account = shared
        .db_handler
        .get_account(create_transaction.account.clone())
        .await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let outputs: Vec<OutPut> = create_transaction
        .outputs
        .unwrap_or(vec![])
        .iter()
        .map(|output| OutPut::from(output.clone()))
        .collect();
    let mut mints = vec![];
    for item in create_transaction.mints.unwrap_or(vec![]).into_iter() {
        if item.asset_id.is_none() && item.name.is_none() {
            return OreoError::BadMintRequest.into_response();
        } else {
            mints.push(item);
        }
    }
    let burns = create_transaction.burns.unwrap_or(vec![]);
    shared
        .rpc_handler
        .create_transaction(RpcCreateTxRequest {
            account: db_account.unwrap().name,
            outputs: Some(outputs),
            fee: Some(create_transaction.fee.unwrap_or("1".into())),
            expiration_delta: Some(create_transaction.expiration_delta.unwrap_or(30)),
            mints: Some(mints),
            burns: Some(burns),
        })
        .into_response()
}

pub async fn broadcast_transaction_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
    extract::Json(broadcast_transaction): extract::Json<RpcBroadcastTxRequest>,
) -> impl IntoResponse {
    shared
        .rpc_handler
        .broadcast_transaction(broadcast_transaction)
        .into_response()
}

pub async fn latest_block_handler<T: DBHandler>(
    State(shared): State<Arc<SharedState<T>>>,
) -> impl IntoResponse {
    shared.rpc_handler.get_latest_block().into_response()
}

pub async fn health_check_handler() -> impl IntoResponse {
    Json(json!({"code": 200, "data": "Hello prover!"})).into_response()
}
