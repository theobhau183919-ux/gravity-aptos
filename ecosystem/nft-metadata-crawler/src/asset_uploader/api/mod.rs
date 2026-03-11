// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{asset_uploader::api::get_status::get_status, config::Server};
use ahash::AHashMap;
use axum::{
    extract::Path,
    http::header,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Extension, Json,
};
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;
use upload_batch::upload_batch;
use url::Url;

mod get_status;
mod upload_batch;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssetUploaderApiConfig {
    pub api_key: String,
}

#[derive(Clone)]
pub struct AssetUploaderApiContext {
    pool: Pool<ConnectionManager<PgConnection>>,
    config: AssetUploaderApiConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IdempotencyTuple {
    pub idempotency_key: String,
    pub application_id: String,
}

#[derive(Debug, Deserialize)]
struct BatchUploadRequest {
    #[serde(flatten)]
    idempotency_tuple: IdempotencyTuple,
    urls: Vec<Url>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum BatchUploadResponse {
    Success {
        #[serde(flatten)]
        idempotency_tuple: IdempotencyTuple,
    },
    Error {
        error: String,
    },
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum GetStatusResponseSuccess {
    Success {
        status_code: u16,
        cdn_image_uri: String,
    },
    Error {
        status_code: u16,
        error_message: Option<Vec<Option<String>>>,
    },
}

#[derive(Serialize)]
#[serde(untagged)]
enum GetStatusResponse {
    Success {
        #[serde(flatten)]
        idempotency_tuple: IdempotencyTuple,
        urls: AHashMap<String, GetStatusResponseSuccess>,
    },
    Error {
        error: String,
    },
}

impl AssetUploaderApiContext {
    pub fn new(
        pool: Pool<ConnectionManager<PgConnection>>,
        config: AssetUploaderApiConfig,
    ) -> Self {
        Self { pool, config }
    }

    fn is_authorized(headers: &axum::http::HeaderMap, expected_api_key: &str) -> bool {
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == format!("Bearer {expected_api_key}"))
    }

    async fn handle_upload_batch(
        Extension(context): Extension<Arc<AssetUploaderApiContext>>,
        headers: axum::http::HeaderMap,
        Json(request): Json<BatchUploadRequest>,
    ) -> impl IntoResponse {
        if !Self::is_authorized(&headers, &context.config.api_key) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(BatchUploadResponse::Error {
                    error: "Unauthorized".to_string(),
                }),
            );
        }

        match upload_batch(context.pool.clone(), &request) {
            Ok(idempotency_tuple) => (
                StatusCode::OK,
                Json(BatchUploadResponse::Success { idempotency_tuple }),
            ),
            Err(e) => {
                error!(error = ?e, "Error uploading asset");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(BatchUploadResponse::Error {
                        error: format!("Error uploading asset: {}", e),
                    }),
                )
            },
        }
    }

    async fn handle_get_status(
        Extension(context): Extension<Arc<AssetUploaderApiContext>>,
        headers: axum::http::HeaderMap,
        Path((application_id, idempotency_key)): Path<(String, String)>, // Extracts application_id and idempotency_key from the URL
    ) -> impl IntoResponse {
        if !Self::is_authorized(&headers, &context.config.api_key) {
            return (
                StatusCode::UNAUTHORIZED,
                Json(GetStatusResponse::Error {
                    error: "Unauthorized".to_string(),
                }),
            );
        }

        let idempotency_tuple = IdempotencyTuple {
            idempotency_key,
            application_id,
        };

        match get_status(context.pool.clone(), &idempotency_tuple) {
            Ok(statuses) => (
                StatusCode::OK,
                Json(GetStatusResponse::Success {
                    idempotency_tuple,
                    urls: statuses,
                }),
            ),
            Err(e) => {
                error!(error = ?e, "Error getting status");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(GetStatusResponse::Error {
                        error: format!("Error getting status: {}", e),
                    }),
                )
            },
        }
    }
}

impl Server for AssetUploaderApiContext {
    fn build_router(&self) -> axum::Router {
        let self_arc = Arc::new(self.clone());
        axum::Router::new()
            .route("/upload", post(Self::handle_upload_batch))
            .route(
                "/status/:application_id/:idempotency_key",
                get(Self::handle_get_status),
            )
            .layer(Extension(self_arc.clone()))
    }
}
