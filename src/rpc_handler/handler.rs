use std::{fmt::Debug, time::Duration};

use serde::Deserialize;
use tracing::debug;
use ureq::{Agent, AgentBuilder, Error, Response};

use crate::{
    error::OreoError,
    web_handlers::abi::{GetAccountStatusRep, GetAccountStatusReq, GetLatestBlockRep},
};

use super::{abi::*, RpcError};

#[derive(Debug, Clone)]
pub struct RpcHandler {
    pub endpoint: String,
    pub agent: Agent,
}

impl RpcHandler {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            agent: AgentBuilder::new()
                .timeout_read(Duration::from_secs(5))
                .timeout_write(Duration::from_secs(5))
                .build(),
        }
    }

    pub async fn import_view_only(
        &self,
        req: ImportAccountReq,
    ) -> Result<RpcResponse<ImportAccountRep>, OreoError> {
        let path = format!("http://{}/wallet/importAccount", self.endpoint);
        let resp = self
            .agent
            .clone()
            .post(&path)
            .send_json(ureq::json!({"account": req}));
        handle_response(resp)
    }

    pub async fn get_balance(
        &self,
        req: GetBalancesReq,
    ) -> Result<RpcResponse<GetBalancesRep>, OreoError> {
        let path = format!("http://{}/wallet/getBalances", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&req);
        handle_response(resp)
    }

    pub async fn get_transactions(
        &self,
        req: GetTransactionsReq,
    ) -> Result<RpcResponse<GetTransactionsRep>, OreoError> {
        let path = format!("http://{}/wallet/getAccountTransactions", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&req);
        handle_response(resp)
    }

    pub async fn create_transaction(
        &self,
        req: CreateTxReq,
    ) -> Result<RpcResponse<CreateTxRep>, OreoError> {
        let path = format!("http://{}/wallet/createTransaction", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&req);
        handle_response(resp)
    }

    pub async fn broadcast_transaction(
        &self,
        req: BroadcastTxReq,
    ) -> Result<RpcResponse<BroadcastTxRep>, OreoError> {
        let path = format!("http://{}/chain/broadcastTransaction", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&req);
        handle_response(resp)
    }

    pub async fn get_account_status(
        &self,
        req: GetAccountStatusReq,
    ) -> Result<RpcResponse<GetAccountStatusRep>, OreoError> {
        let path = format!("http://{}/wallet/getAccountStatus", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&req);
        handle_response(resp)
    }

    pub async fn get_latest_block(&self) -> Result<RpcResponse<GetLatestBlockRep>, OreoError> {
        let path = format!("http://{}/chain/getChainInfo", self.endpoint);
        let resp = self.agent.clone().get(&path).call();
        handle_response(resp)
    }

    pub async fn get_account_transaction(
        &self,
        req: GetAccountTransactionReq,
    ) -> Result<RpcResponse<GetAccountTransactionRep>, OreoError> {
        let path = format!("http://{}/account/getAccountTransaction", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&req);
        handle_response(resp)
    }
}

pub fn handle_response<S: Debug + for<'a> Deserialize<'a>>(
    resp: Result<Response, Error>,
) -> Result<RpcResponse<S>, OreoError> {
    let res = match resp {
        Ok(response) => match response.into_json::<RpcResponse<S>>() {
            Ok(data) => Ok(data),
            Err(e) => Err(RpcError {
                code: "Unknown".into(),
                status: 606,
                message: e.to_string(),
            }),
        },
        Err(ureq::Error::Status(_code, response)) => match response.into_json::<RpcError>() {
            Ok(data) => Err(data),
            Err(e) => Err(RpcError {
                code: "Unknown".into(),
                status: 606,
                message: e.to_string(),
            }),
        },
        Err(e) => Err(RpcError {
            code: "Unknown".into(),
            status: 606,
            message: e.to_string(),
        }),
    };
    debug!("Handle rpc response: {:?}", res);
    match res {
        Ok(data) => Ok(data),
        Err(e) => Err(OreoError::try_from(e).unwrap()),
    }
}
