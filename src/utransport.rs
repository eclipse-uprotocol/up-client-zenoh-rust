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
use crate::{UPClientZenoh, UtransportListener};
use async_trait::async_trait;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use up_rust::{
    Data, UAttributes, UAttributesValidators, UCode, UMessage, UMessageType, UPayload,
    UPayloadFormat, UStatus, UTransport, UUri, UriValidator,
};
use zenoh::{
    prelude::{r#async::*, Sample},
    query::Reply,
    queryable::Query,
};

impl UPClientZenoh {
    async fn send_publish(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // Assume we only have Value here, no reference for shared memory
            let msg = "The data in UPayload should be Data::Value".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Transform UAttributes to user attachment in Zenoh
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            let msg = "Unable to transform UAttributes to attachment".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Map the priority to Zenoh
        let priority =
            UPClientZenoh::map_zenoh_priority(attributes.priority.enum_value().map_err(|_| {
                let msg = "Unable to map to Zenoh priority".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?);

        // Send data
        let putbuilder = self
            .session
            .put(zenoh_key, buf)
            .encoding(Encoding::WithSuffix(
                KnownEncoding::AppCustom,
                payload.format.value().to_string().into(),
            ))
            .priority(priority)
            .with_attachment(attachment.build());
        putbuilder
            .res()
            .await
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to send with Zenoh"))?;

        Ok(())
    }

    async fn send_request(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // Assume we only have Value here, no reference for shared memory
            let msg = "The data in UPayload should be Data::Value".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Transform UAttributes to user attachment in Zenoh
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            let msg = "Unable to transform UAttributes to attachment".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Retrieve the callback
        let source_uuri = *attributes.source.0.ok_or_else(|| {
            let msg = "Lack of source address".to_string();
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;
        let hashmap_key = UPClientZenoh::to_zenoh_key_string(&source_uuri)?;
        let resp_callback = self
            .rpc_callback_map
            .lock()
            .unwrap()
            .get(&hashmap_key)
            .ok_or_else(|| {
                let msg = "Unable to get callback".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?
            .clone();
        let zenoh_callback = move |reply: Reply| {
            let msg = match reply.sample {
                Ok(sample) => {
                    // Get the encoding of UPayload
                    let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
                        let msg = "Unable to get the encoding".to_string();
                        log::error!("{msg}");
                        resp_callback(Err(UStatus::fail_with_code(UCode::INTERNAL, msg)));
                        return;
                    };
                    // Get UAttribute from the attachment
                    let Some(attachment) = sample.attachment() else {
                        let msg = "Unable to get the attachment".to_string();
                        log::error!("{msg}");
                        resp_callback(Err(UStatus::fail_with_code(UCode::INTERNAL, msg)));
                        return;
                    };
                    let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                        Ok(uattr) => uattr,
                        Err(e) => {
                            let msg =
                                format!("Unable to transform attachment to UAttributes: {e:?}");
                            log::error!("{msg}");
                            resp_callback(Err(UStatus::fail_with_code(UCode::INTERNAL, msg)));
                            return;
                        }
                    };
                    // Create UMessage
                    Ok(UMessage {
                        attributes: Some(u_attribute).into(),
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
                Err(e) => {
                    let msg = format!("Error while parsing Zenoh reply: {e:?}");
                    log::error!("{msg}");
                    Err(UStatus::fail_with_code(UCode::INTERNAL, msg))
                }
            };
            resp_callback(msg);
        };

        // Send query
        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        let getbuilder = self
            .session
            .get(zenoh_key)
            .with_value(value)
            .with_attachment(attachment.build())
            .target(QueryTarget::BestMatching)
            .timeout(Duration::from_millis(u64::from(
                attributes.ttl.unwrap_or(1000),
            )))
            .callback(zenoh_callback);
        getbuilder.res().await.map_err(|e| {
            let msg = format!("Unable to send get with Zenoh: {e:?}");
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INTERNAL, msg)
        })?;

        Ok(())
    }

    async fn send_response(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // Assume we only have Value here, no reference for shared memory
            let msg = "The data in UPayload should be Data::Value".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Transform UAttributes to user attachment in Zenoh
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            let msg = "Unable to transform UAttributes to attachment".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Find out the corresponding query from HashMap
        let reqid = attributes.reqid.to_string();
        let query = self
            .query_map
            .lock()
            .unwrap()
            .get(&reqid)
            .ok_or_else(|| {
                let msg = "query doesn't exist".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?
            .clone();

        // Send back the query
        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        let reply = Ok(Sample::new(
            KeyExpr::new(zenoh_key.to_string()).map_err(|e| {
                let msg = format!("Unable to create Zenoh key: {e:?}");
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?,
            value,
        ));
        query
            .reply(reply)
            .with_attachment(attachment.build())
            .map_err(|_| {
                let msg = "Unable to add attachment";
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?
            .res()
            .await
            .map_err(|e| {
                let msg = format!("Unable to reply with Zenoh: {e:?}");
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?;

        Ok(())
    }

    async fn register_publish_listener(
        &self,
        topic: &UUri,
        listener: Arc<UtransportListener>,
    ) -> Result<String, UStatus> {
        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(topic)?;

        // Generate listener string for users to delete
        let hashmap_key = format!(
            "{}_{:X}",
            zenoh_key,
            self.callback_counter.fetch_add(1, Ordering::SeqCst)
        );

        // Setup callback
        let callback = move |sample: Sample| {
            // Get the UAttribute from Zenoh user attachment
            let Some(attachment) = sample.attachment() else {
                let msg = "Unable to get attachment";
                log::error!("{msg}");
                listener(Err(UStatus::fail_with_code(UCode::INTERNAL, msg)));
                return;
            };
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    let msg = format!("Unable to transform attachment to UAttributes: {e:?}");
                    log::error!("{msg}");
                    listener(Err(UStatus::fail_with_code(UCode::INTERNAL, msg)));
                    return;
                }
            };
            // Create UPayload
            let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
                let msg = "Unable to get payload encoding";
                log::error!("{msg}");
                listener(Err(UStatus::fail_with_code(UCode::INTERNAL, msg)));
                return;
            };
            let u_payload = UPayload {
                length: Some(0),
                format: encoding.into(),
                data: Some(Data::Value(sample.payload.contiguous().to_vec())),
                ..Default::default()
            };
            // Create UMessage
            let msg = UMessage {
                attributes: Some(u_attribute).into(),
                payload: Some(u_payload).into(),
                ..Default::default()
            };
            listener(Ok(msg));
        };

        // Create Zenoh subscriber
        if let Ok(subscriber) = self
            .session
            .declare_subscriber(&zenoh_key)
            .callback_mut(callback)
            .res()
            .await
        {
            self.subscriber_map
                .lock()
                .unwrap()
                .insert(hashmap_key.clone(), subscriber);
        } else {
            let msg = "Unable to register callback with Zenoh";
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        }

        Ok(hashmap_key)
    }

    async fn register_request_listener(
        &self,
        topic: &UUri,
        listener: Arc<UtransportListener>,
    ) -> Result<String, UStatus> {
        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(topic)?;

        // Generate listener string for users to delete
        let hashmap_key = format!(
            "{}_{:X}",
            zenoh_key,
            self.callback_counter.fetch_add(1, Ordering::SeqCst)
        );

        // Setup callback
        let query_map = self.query_map.clone();
        let callback = move |query: Query| {
            // Create UAttribute from Zenoh user attachment
            let Some(attachment) = query.attachment() else {
                let msg = "Unable to get attachment".to_string();
                log::error!("{msg}");
                listener(Err(UStatus::fail_with_code(UCode::INTERNAL, msg)));
                return;
            };
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    let msg = format!("Unable to transform user attachment to UAttributes: {e:?}");
                    log::error!("{msg}");
                    listener(Err(UStatus::fail_with_code(UCode::INTERNAL, msg)));
                    return;
                }
            };
            // Create UPayload from Zenoh data
            let u_payload = match query.value() {
                Some(value) => {
                    let Some(encoding) = UPClientZenoh::to_upayload_format(&value.encoding) else {
                        let msg = "Unable to get payload encoding".to_string();
                        log::error!("{msg}");
                        listener(Err(UStatus::fail_with_code(UCode::INTERNAL, msg)));
                        return;
                    };
                    UPayload {
                        length: Some(0),
                        format: encoding.into(),
                        data: Some(Data::Value(value.payload.contiguous().to_vec())),
                        ..Default::default()
                    }
                }
                None => UPayload {
                    length: Some(0),
                    format: UPayloadFormat::UPAYLOAD_FORMAT_UNSPECIFIED.into(),
                    data: None,
                    ..Default::default()
                },
            };
            // Create UMessage and store the query into HashMap (Will be used in send_response)
            let msg = UMessage {
                attributes: Some(u_attribute.clone()).into(),
                payload: Some(u_payload).into(),
                ..Default::default()
            };
            query_map
                .lock()
                .unwrap()
                .insert(u_attribute.id.to_string(), query);
            listener(Ok(msg));
        };

        // Create Zenoh queryable
        if let Ok(queryable) = self
            .session
            .declare_queryable(&zenoh_key)
            .callback_mut(callback)
            .res()
            .await
        {
            self.queryable_map
                .lock()
                .unwrap()
                .insert(hashmap_key.clone(), queryable);
        } else {
            let msg = "Unable to register callback with Zenoh".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        }

        Ok(hashmap_key)
    }

    fn register_response_listener(
        &self,
        topic: &UUri,
        listener: Arc<UtransportListener>,
    ) -> Result<String, UStatus> {
        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(topic)?;

        // Store the response callback (Will be used in send_request)
        self.rpc_callback_map
            .lock()
            .unwrap()
            .insert(zenoh_key.clone(), listener);

        Ok(zenoh_key)
    }
}

#[async_trait]
impl UTransport for UPClientZenoh {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let payload = *message.payload.0.ok_or_else(|| {
            let msg = "Invalid UPayload".to_string();
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;
        let attributes = *message.attributes.0.ok_or_else(|| {
            let msg = "Invalid UAttributes".to_string();
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;

        // Check the type of UAttributes (Publish / Request / Response)
        match attributes
            .type_
            .enum_value()
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to parse type"))?
        {
            UMessageType::UMESSAGE_TYPE_PUBLISH => {
                UAttributesValidators::Publish
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        let msg = format!("Wrong Publish UAttributes: {e:?}");
                        log::error!("{msg}");
                        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
                    })?;
                // Get Zenoh key: Publication => topic is source
                let topic = attributes.clone().source;
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Publish
                self.send_publish(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_NOTIFICATION => {
                UAttributesValidators::Publish
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        let msg = format!("Wrong Publish UAttributes: {e:?}");
                        log::error!("{msg}");
                        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
                    })?;
                // Get Zenoh key: Notification => topic is sink
                let topic = attributes.clone().sink;
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Publish
                self.send_publish(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_REQUEST => {
                UAttributesValidators::Request
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        let msg = format!("Wrong Request UAttributes: {e:?}");
                        log::error!("{msg}");
                        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
                    })?;
                // Get Zenoh key
                let topic = attributes.clone().sink;
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Request
                self.send_request(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_RESPONSE => {
                UAttributesValidators::Response
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        let msg = format!("Wrong Response UAttributes: {e:?}");
                        log::error!("{msg}");
                        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
                    })?;
                // Get Zenoh key
                let topic = attributes.clone().source;
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Response
                self.send_response(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_UNSPECIFIED => {
                let msg = "Wrong Message type in UAttributes".to_string();
                log::error!("{msg}");
                Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg))
            }
        }
    }

    async fn receive(&self, _topic: UUri) -> Result<UMessage, UStatus> {
        let msg = "Not implemented".to_string();
        log::error!("{msg}");
        Err(UStatus::fail_with_code(UCode::UNIMPLEMENTED, msg))
    }

    async fn register_listener(
        &self,
        topic: UUri,
        listener: Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>,
    ) -> Result<String, UStatus> {
        let listener = Arc::new(listener);
        if topic.authority.is_some() && topic.entity.is_none() && topic.resource.is_none() {
            // This is special UUri which means we need to register for all of Publish, Request, and Response
            // RPC response
            let mut listener_str = self.register_response_listener(&topic, listener.clone())?;
            // RPC request
            listener_str += "&";
            listener_str += &self
                .register_request_listener(&topic, listener.clone())
                .await?;
            // Normal publish
            listener_str += "&";
            listener_str += &self
                .register_publish_listener(&topic, listener.clone())
                .await?;
            Ok(listener_str)
        } else {
            // Do the validation
            UriValidator::validate(&topic)
                .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;

            if UriValidator::is_rpc_response(&topic) {
                // RPC response
                self.register_response_listener(&topic, listener.clone())
            } else if UriValidator::is_rpc_method(&topic) {
                // RPC request
                self.register_request_listener(&topic, listener.clone())
                    .await
            } else {
                // Normal publish
                self.register_publish_listener(&topic, listener.clone())
                    .await
            }
        }
    }

    async fn unregister_listener(&self, topic: UUri, listener: &str) -> Result<(), UStatus> {
        let mut pub_listener_str: Option<&str> = None;
        let mut req_listener_str: Option<&str> = None;
        let mut resp_listener_str: Option<&str> = None;

        if topic.authority.is_some() && topic.entity.is_none() && topic.resource.is_none() {
            // This is special UUri which means we need to unregister both listeners
            let listener_vec = listener.split('&').collect::<Vec<_>>();
            if listener_vec.len() != 3 {
                let msg = "Invalid listener string".to_string();
                log::error!("{msg}");
                return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
            }
            resp_listener_str = Some(listener_vec[0]);
            req_listener_str = Some(listener_vec[1]);
            pub_listener_str = Some(listener_vec[2]);
        } else {
            // Do the validation
            UriValidator::validate(&topic).map_err(|_| {
                let msg = "Invalid topic".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?;

            if UriValidator::is_rpc_response(&topic) {
                resp_listener_str = Some(listener);
            } else if UriValidator::is_rpc_method(&topic) {
                req_listener_str = Some(listener);
            } else {
                pub_listener_str = Some(listener);
            }
        }
        if let Some(listener) = resp_listener_str {
            // RPC response
            if self
                .rpc_callback_map
                .lock()
                .unwrap()
                .remove(listener)
                .is_none()
            {
                let msg = "RPC response callback doesn't exist".to_string();
                log::warn!("{msg}");
                return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
            }
        }
        if let Some(listener) = req_listener_str {
            // RPC request
            if self
                .queryable_map
                .lock()
                .unwrap()
                .remove(listener)
                .is_none()
            {
                let msg = "RPC request listener doesn't exist".to_string();
                log::warn!("{msg}");
                return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
            }
        }
        if let Some(listener) = pub_listener_str {
            // Normal publish
            if self
                .subscriber_map
                .lock()
                .unwrap()
                .remove(listener)
                .is_none()
            {
                let msg = "Publish listener doesn't exist".to_string();
                log::warn!("{msg}");
                return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
            }
        }
        Ok(())
    }
}
