/********************************************************************************
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/
use crate::UPClientZenoh;
use async_trait::async_trait;
use std::{string::ToString, time::Duration};
use up_rust::{RpcClient, RpcClientResult, UAttributesError, UMessage, UMessageError, UUri};
use zenoh::{prelude::*, query::QueryTarget, sample::ValueBuilderTrait};

#[async_trait]
impl RpcClient for UPClientZenoh {
    // CY_TODO: Should remove method in the future
    async fn invoke_method(&self, _method: UUri, request: UMessage) -> RpcClientResult {
        // Get Zenoh key
        let source = *request.attributes.source.0.clone().ok_or_else(|| {
            let msg = "attributes.source should not be empty".to_string();
            log::error!("{msg}");
            UMessageError::PayloadError(msg)
        })?;
        let zenoh_key = if let Some(sink) = request.attributes.sink.0.clone() {
            self.to_zenoh_key_string(&source, Some(&sink))
        } else {
            self.to_zenoh_key_string(&source, None)
        };

        // Create UAttributes and put into Zenoh user attachment
        let attributes = *request.attributes.0.clone().ok_or_else(|| {
            let msg = "Invalid UAttributes".to_string();
            log::error!("{msg}");
            UMessageError::AttributesValidationError(UAttributesError::ParsingError(msg))
        })?;
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            let msg = "Unable to transform UAttributes to user attachment in Zenoh".to_string();
            log::error!("{msg}");
            return Err(UMessageError::AttributesValidationError(
                UAttributesError::ParsingError(msg),
            ));
        };

        // Get the data from UPayload
        let value = if let Some(payload) = request.payload {
            payload.to_vec()
        } else {
            vec![]
        };

        // Send the query
        let mut getbuilder = self
            .session
            .get(&zenoh_key)
            .value(value)
            .attachment(attachment)
            .target(QueryTarget::BestMatching);
        if let Some(ttl) = request.attributes.ttl {
            getbuilder = getbuilder.timeout(Duration::from_millis(u64::from(ttl)));
        }
        let Ok(replies) = getbuilder.await else {
            let msg = "Error while sending Zenoh query".to_string();
            log::error!("{msg}");
            return Err(UMessageError::PayloadError(msg));
        };

        // Receive the reply
        let Ok(reply) = replies.recv_async().await else {
            let msg = "Error while receiving Zenoh reply".to_string();
            log::error!("{msg}");
            return Err(UMessageError::PayloadError(msg));
        };
        match reply.result() {
            Ok(sample) => Ok(UMessage {
                attributes: Some(attributes).into(),
                payload: Some(sample.payload().into::<Vec<u8>>().into()),
                ..Default::default()
            }),
            Err(e) => {
                let msg = format!("Error while parsing Zenoh reply: {e:?}");
                log::error!("{msg}");
                Err(UMessageError::PayloadError(msg))
            }
        }
    }
}
