use std::sync::{Arc, Mutex};

use axum::{extract, response::IntoResponse, Json};
use bellperson::groth16;
use ironfish_rust::sapling_bls12::SAPLING;
use ironfish_zkp::proofs::{MintAsset, Output, Spend};
use networking::web_abi::{GenerateProofRequest, GenerateProofResponse};
use oreo_errors::OreoError;
use rand::thread_rng;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde_json::json;
use tracing::{error, info};

pub async fn generate_proof_handler(
    extract::Json(request): extract::Json<GenerateProofRequest>,
) -> impl IntoResponse {
    info!("calling generate_proof_handler");
    let failed_index = Arc::new(Mutex::new(0u32));
    let spend_proofs = request
        .spend_circuits
        .par_iter()
        .enumerate()
        .map(|(idx, bytes)| {
            let proof_bytes = if let Ok(spend) = Spend::read(&bytes[..]) {
                let proof =
                    groth16::create_random_proof(spend, &SAPLING.spend_params, &mut thread_rng());
                if let Ok(proof) = proof {
                    let mut writer = vec![];
                    proof.write(&mut writer).unwrap();
                    Some(writer)
                } else {
                    None
                }
            } else {
                None
            };
            if proof_bytes.is_none() {
                *failed_index.lock().unwrap() = idx as u32 + 1;
                error!("generate spend proof failed");
                return vec![];
            }
            proof_bytes.unwrap()
        })
        .collect();

    let idx = *failed_index.lock().unwrap();
    if idx > 0 {
        return OreoError::GenerateSpendProofFailed(idx - 1).into_response();
    }

    let output_proofs = request
        .output_circuits
        .par_iter()
        .enumerate()
        .map(|(idx, bytes)| {
            let proof_bytes = if let Ok(output) = Output::read(&bytes[..]) {
                let proof =
                    groth16::create_random_proof(output, &SAPLING.output_params, &mut thread_rng());
                if let Ok(proof) = proof {
                    let mut writer = vec![];
                    proof.write(&mut writer).unwrap();
                    Some(writer)
                } else {
                    None
                }
            } else {
                None
            };
            if proof_bytes.is_none() {
                *failed_index.lock().unwrap() = idx as u32 + 1;
                error!("generate output proof failed");
                return vec![];
            }
            proof_bytes.unwrap()
        })
        .collect();

    let idx = *failed_index.lock().unwrap();
    if idx > 0 {
        return OreoError::GenerateSpendProofFailed(idx - 1).into_response();
    }

    let mint_asset_proofs = request
        .mint_asset_circuits
        .par_iter()
        .enumerate()
        .map(|(idx, bytes)| {
            let proof_bytes = if let Ok(mint_asset) = MintAsset::read(&bytes[..]) {
                let proof = groth16::create_random_proof(
                    mint_asset,
                    &SAPLING.mint_params,
                    &mut thread_rng(),
                );
                if let Ok(proof) = proof {
                    let mut writer = vec![];
                    proof.write(&mut writer).unwrap();
                    Some(writer)
                } else {
                    None
                }
            } else {
                None
            };
            if proof_bytes.is_none() {
                *failed_index.lock().unwrap() = idx as u32 + 1;
                error!("generate mint asset proof failed");
                return vec![];
            }
            proof_bytes.unwrap()
        })
        .collect();

    let idx = *failed_index.lock().unwrap();
    if idx > 0 {
        return OreoError::GenerateMintAssetProofFailed(idx - 1).into_response();
    }

    let proof = GenerateProofResponse {
        spend_proofs,
        output_proofs,
        mint_asset_proofs,
    };

    Json(json!({"code": 200, "data": proof})).into_response()
}

pub async fn health_check_handler() -> impl IntoResponse {
    Json(json!({"code": 200, "data": "Hello prover!"})).into_response()
}
