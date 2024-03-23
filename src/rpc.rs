//
// Copyright (c) 2024 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use crate::UPClientZenoh;
use async_trait::async_trait;
use std::{string::ToString, time::Duration};
use up_rust::{
    CallOptions, Data, RpcClient, RpcClientResult, RpcMapperError, UMessage, UMessageBuilder,
    UPayload, UResourceBuilder, UUri, UriValidator,
};
use zenoh::prelude::r#async::*;

#[async_trait]
impl RpcClient for UPClientZenoh {
    async fn invoke_method(
        &self,
        topic: UUri,
        payload: UPayload,
        options: CallOptions,
    ) -> RpcClientResult {
        // Validate UUri
        UriValidator::validate(&topic)
            .map_err(|_| RpcMapperError::UnexpectedError(String::from("Wrong UUri")))?;

        // Get Zenoh key
        let Ok(zenoh_key) = UPClientZenoh::to_zenoh_key_string(&topic) else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Unable to transform to Zenoh key",
            )));
        };

        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // Assume we only have Value here, no reference for shared memory
            return Err(RpcMapperError::InvalidPayload(String::from(
                "The data in UPayload should be Data::Value",
            )));
        };

        // Generate UAttributes
        // Create response address
        let mut source = UUri {
            entity: Some(self.entity.clone()).into(),
            ..Default::default()
        };
        // if we are sending to a remote authority, we attach our authority
        if topic.authority.is_some() {
            source.authority = Some(self.authority.clone()).into();
        }
        source.resource = Some(UResourceBuilder::for_rpc_response()).into();
        // Create UMessage
        let umessage = if let Some(token) = options.token() {
            UMessageBuilder::request(topic, source, options.timeout())
                .with_message_id(self.uuid_builder.build())
                .with_token(token.to_string())
                .build()
        } else {
            UMessageBuilder::request(topic, source, options.timeout())
                .with_message_id(self.uuid_builder.build())
                .build()
        };
        // Extract uAttributes
        let Ok(UMessage {
            attributes: uattributes,
            ..
        }) = umessage
        else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Unable to create uAttributes",
            )));
        };
        // Put into attachment
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&uattributes) else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Invalid uAttributes",
            )));
        };

        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        // TODO: Query should support .encoding
        let getbuilder = self
            .session
            .get(&zenoh_key)
            .with_value(value)
            .with_attachment(attachment.build())
            .target(QueryTarget::BestMatching)
            .timeout(Duration::from_millis(u64::from(options.timeout())));

        // Send the query
        let Ok(replies) = getbuilder.res().await else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Error while sending Zenoh query",
            )));
        };

        let Ok(reply) = replies.recv_async().await else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Error while receiving Zenoh reply",
            )));
        };
        match reply.sample {
            Ok(sample) => {
                let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
                    return Err(RpcMapperError::UnexpectedError(String::from(
                        "Error while parsing Zenoh encoding",
                    )));
                };
                // TODO: Need to check attributes is correct or not
                Ok(UMessage {
                    attributes: uattributes,
                    payload: Some(UPayload {
                        length: Some(0),
                        format: encoding.into(),
                        data: Some(Data::Value(sample.payload.contiguous().to_vec())),
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                })
            }
            Err(e) => Err(RpcMapperError::UnexpectedError(format!(
                "Error while parsing Zenoh reply: {e:?}"
            ))),
        }
    }
}
