use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportAccountReq {
    pub view_key: String,
    pub incoming_view_key: String,
    pub outgoing_view_key: String,
    pub public_address: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ImportAccountRep {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateProofReq {
    pub spend_circuits: Vec<Vec<u8>>,
    pub output_circuits: Vec<Vec<u8>>,
    pub mint_asset_circuits: Vec<Vec<u8>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GenerateProofRep {
    pub spend_proofs: Vec<Vec<u8>>,
    pub output_proofs: Vec<Vec<u8>>,
    pub mint_asset_proofs: Vec<Vec<u8>>,
}