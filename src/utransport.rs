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
use crate::{MessageFlag, UPClientZenoh, CB_RUNTIME};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tokio::{runtime::Handle, task};
use up_rust::{
    ComparableListener, UAttributes, UAttributesValidators, UCode, UListener, UMessage,
    UMessageType, UStatus, UTransport, UUri,
};
use zenoh::{
    prelude::*,
    query::{Query, QueryTarget, Reply},
    sample::{QoSBuilderTrait, Sample, ValueBuilderTrait},
};

#[inline]
fn invoke_block_callback(listener: &Arc<dyn UListener>, resp_msg: Result<UMessage, &str>) {
    match resp_msg {
        Ok(umsg) => {
            task::block_in_place(|| {
                Handle::current().block_on(listener.on_receive(umsg));
            });
        }
        Err(err_msg) => {
            log::error!("{err_msg}");
            task::block_in_place(|| {
                Handle::current()
                    .block_on(listener.on_error(UStatus::fail_with_code(UCode::INTERNAL, err_msg)));
            });
        }
    }
}

#[inline]
fn spawn_nonblock_callback(listener: &Arc<dyn UListener>, listener_msg: Result<UMessage, &str>) {
    let listener = listener.clone();
    match listener_msg {
        Ok(umsg) => {
            CB_RUNTIME.spawn(async move {
                listener.on_receive(umsg).await;
            });
        }
        Err(err_msg) => {
            log::error!("{err_msg}");
            let err_msg = err_msg.to_string();
            CB_RUNTIME.spawn(async move {
                listener
                    .on_error(UStatus::fail_with_code(UCode::INTERNAL, err_msg))
                    .await;
            });
        }
    }
}

impl UPClientZenoh {
    async fn send_publish_notification(
        &self,
        zenoh_key: &str,
        payload: &[u8],
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
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
            .put(zenoh_key, payload)
            .priority(priority)
            .attachment(attachment);
        putbuilder
            .await
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to send with Zenoh"))?;

        Ok(())
    }

    async fn send_request(
        &self,
        zenoh_key: &str,
        payload: &[u8],
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Transform UAttributes to user attachment in Zenoh
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            let msg = "Unable to transform UAttributes to attachment".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Retrieve the callback
        let resp_callback = self
            .rpc_callback_map
            .lock()
            .unwrap()
            .get(zenoh_key)
            .ok_or_else(|| {
                let msg = "Unable to get callback".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?
            .clone();
        let zenoh_callback = move |reply: Reply| {
            match reply.result() {
                Ok(sample) => {
                    // Get UAttribute from the attachment
                    let Some(attachment) = sample.attachment() else {
                        invoke_block_callback(&resp_callback, Err("Unable to get the attachment"));
                        return;
                    };
                    let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                        Ok(uattr) => uattr,
                        Err(e) => {
                            invoke_block_callback(
                                &resp_callback,
                                Err(&format!(
                                    "Transform attachment to UAttributes failed: {e:?}"
                                )),
                            );
                            return;
                        }
                    };
                    // Create UMessage
                    invoke_block_callback(
                        &resp_callback,
                        Ok(UMessage {
                            attributes: Some(u_attribute).into(),
                            payload: Some(sample.payload().into::<Vec<u8>>().into()),
                            ..Default::default()
                        }),
                    );
                }
                Err(e) => {
                    invoke_block_callback(
                        &resp_callback,
                        Err(&format!("Error while parsing Zenoh reply: {e:?}")),
                    );
                }
            }
        };

        // Send query
        let getbuilder = self
            .session
            .get(zenoh_key)
            .value(payload.to_vec())
            .attachment(attachment)
            .target(QueryTarget::BestMatching)
            .timeout(Duration::from_millis(u64::from(
                attributes.ttl.unwrap_or(1000),
            )))
            .callback(zenoh_callback);
        getbuilder.await.map_err(|e| {
            let msg = format!("Unable to send get with Zenoh: {e:?}");
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INTERNAL, msg)
        })?;

        Ok(())
    }

    async fn send_response(&self, payload: &[u8], attributes: UAttributes) -> Result<(), UStatus> {
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
            .remove(&reqid)
            .ok_or_else(|| {
                let msg = "query doesn't exist".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?
            .clone();

        // Send back the query
        query
            .reply(query.key_expr().clone(), payload)
            .attachment(attachment)
            .await
            .map_err(|e| {
                let msg = format!("Unable to reply with Zenoh: {e:?}");
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?;

        Ok(())
    }

    async fn register_publish_notification_listener(
        &self,
        zenoh_key: &String,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        // Setup callback
        let listener_cloned = listener.clone();
        let callback = move |sample: Sample| {
            // Get the UAttribute from Zenoh user attachment
            let Some(attachment) = sample.attachment() else {
                spawn_nonblock_callback(&listener_cloned, Err("Unable to get attachment"));
                return;
            };
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    spawn_nonblock_callback(
                        &listener_cloned,
                        Err(&format!(
                            "Unable to transform attachment to UAttributes: {e:?}"
                        )),
                    );
                    return;
                }
            };
            // Create UMessage
            let msg = UMessage {
                attributes: Some(u_attribute).into(),
                payload: Some(sample.payload().into::<Vec<u8>>().into()),
                ..Default::default()
            };
            spawn_nonblock_callback(&listener_cloned, Ok(msg));
        };

        // Create Zenoh subscriber
        if let Ok(subscriber) = self
            .session
            .declare_subscriber(zenoh_key)
            .callback_mut(callback)
            .await
        {
            self.subscriber_map.lock().unwrap().insert(
                (zenoh_key.clone(), ComparableListener::new(listener)),
                subscriber,
            );
        } else {
            let msg = "Unable to register callback with Zenoh";
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        }

        Ok(())
    }

    async fn register_request_listener(
        &self,
        zenoh_key: &String,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        // Setup callback
        let listener_cloned = listener.clone();
        let query_map = self.query_map.clone();
        let callback = move |query: Query| {
            // Create UAttribute from Zenoh user attachment
            let Some(attachment) = query.attachment() else {
                spawn_nonblock_callback(&listener_cloned, Err("Unable to get attachment"));
                return;
            };
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    spawn_nonblock_callback(
                        &listener_cloned,
                        Err(&format!(
                            "Unable to transform user attachment to UAttributes: {e:?}"
                        )),
                    );
                    return;
                }
            };
            // Create UMessage and store the query into HashMap (Will be used in send_response)
            let msg = UMessage {
                attributes: Some(u_attribute.clone()).into(),
                payload: query
                    .value()
                    .map(|value| value.payload().into::<Vec<u8>>().into()),
                ..Default::default()
            };
            query_map
                .lock()
                .unwrap()
                .insert(u_attribute.id.to_string(), query);
            spawn_nonblock_callback(&listener_cloned, Ok(msg));
        };

        // Create Zenoh queryable
        if let Ok(queryable) = self
            .session
            .declare_queryable(zenoh_key)
            .callback_mut(callback)
            .await
        {
            self.queryable_map.lock().unwrap().insert(
                (zenoh_key.clone(), ComparableListener::new(listener)),
                queryable,
            );
        } else {
            let msg = "Unable to register callback with Zenoh".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        }

        Ok(())
    }

    fn register_response_listener(&self, zenoh_key: &str, listener: Arc<dyn UListener>) {
        // Store the response callback (Will be used in send_request)
        self.rpc_callback_map
            .lock()
            .unwrap()
            .insert(zenoh_key.to_owned(), listener);
    }
}

#[async_trait]
impl UTransport for UPClientZenoh {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let attributes = *message.attributes.0.ok_or_else(|| {
            let msg = "Invalid UAttributes".to_string();
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;

        // Get Zenoh key
        let source = *attributes.clone().source.0.ok_or_else(|| {
            let msg = "attributes.source should not be empty".to_string();
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;
        let zenoh_key = if let Some(sink) = attributes.sink.clone().0 {
            self.to_zenoh_key_string(&source, Some(&sink))
        } else {
            self.to_zenoh_key_string(&source, None)
        };

        // Get payload
        let payload = if let Some(payload) = message.payload {
            payload.to_vec()
        } else {
            vec![]
        };

        // Check the type of UAttributes (Publish / Notification / Request / Response)
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
                // Send Publish
                self.send_publish_notification(&zenoh_key, &payload, attributes)
                    .await
            }
            UMessageType::UMESSAGE_TYPE_NOTIFICATION => {
                UAttributesValidators::Notification
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        let msg = format!("Wrong Notification UAttributes: {e:?}");
                        log::error!("{msg}");
                        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
                    })?;
                // Send Publish
                self.send_publish_notification(&zenoh_key, &payload, attributes)
                    .await
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
                // Send Request
                self.send_request(&zenoh_key, &payload, attributes).await
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
                // Send Response
                self.send_response(&payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_UNSPECIFIED => {
                let msg = "Wrong Message type in UAttributes".to_string();
                log::error!("{msg}");
                Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg))
            }
        }
    }

    async fn receive(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<UMessage, UStatus> {
        let msg = "Not implemented".to_string();
        log::error!("{msg}");
        Err(UStatus::fail_with_code(UCode::UNIMPLEMENTED, msg))
    }

    async fn register_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        let flag = UPClientZenoh::get_listener_message_type(source_filter, sink_filter)?;
        // Publish & Notification
        if flag.contains(MessageFlag::Publish) || flag.contains(MessageFlag::Notification) {
            // Get Zenoh key
            let zenoh_key = self.to_zenoh_key_string(source_filter, sink_filter);
            self.register_publish_notification_listener(&zenoh_key, listener.clone())
                .await?;
        }
        // RPC request
        if flag.contains(MessageFlag::Request) {
            // Get Zenoh key
            let zenoh_key = self.to_zenoh_key_string(source_filter, sink_filter);
            self.register_request_listener(&zenoh_key, listener.clone())
                .await?;
        }
        // RPC response
        if flag.contains(MessageFlag::Response) {
            if let Some(sink_filter) = sink_filter {
                // Get Zenoh key
                let zenoh_key = self.to_zenoh_key_string(sink_filter, Some(source_filter));
                self.register_response_listener(&zenoh_key, listener.clone());
            } else {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    "Sink should not be None in Response",
                ));
            }
        }

        Ok(())
    }

    async fn unregister_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        let flag = UPClientZenoh::get_listener_message_type(source_filter, sink_filter)?;
        // Publish & Notification
        if flag.contains(MessageFlag::Publish) || flag.contains(MessageFlag::Notification) {
            // Get Zenoh key
            let zenoh_key = self.to_zenoh_key_string(source_filter, sink_filter);
            if self
                .subscriber_map
                .lock()
                .unwrap()
                .remove(&(zenoh_key.clone(), ComparableListener::new(listener.clone())))
                .is_none()
            {
                let msg = "Publish / Notifcation listener doesn't exist".to_string();
                log::warn!("{msg}");
                return Err(UStatus::fail_with_code(UCode::NOT_FOUND, msg));
            }
        }
        // RPC request
        if flag.contains(MessageFlag::Request) {
            // Get Zenoh key
            let zenoh_key = self.to_zenoh_key_string(source_filter, sink_filter);
            if self
                .queryable_map
                .lock()
                .unwrap()
                .remove(&(zenoh_key.clone(), ComparableListener::new(listener.clone())))
                .is_none()
            {
                let msg = "RPC request listener doesn't exist".to_string();
                log::warn!("{msg}");
                return Err(UStatus::fail_with_code(UCode::NOT_FOUND, msg));
            }
        }
        // RPC response
        if flag.contains(MessageFlag::Response) {
            if let Some(sink_filter) = sink_filter {
                // Get Zenoh key
                let zenoh_key = self.to_zenoh_key_string(sink_filter, Some(source_filter));
                if self
                    .rpc_callback_map
                    .lock()
                    .unwrap()
                    .remove(&zenoh_key)
                    .is_none()
                {
                    let msg = "RPC response callback doesn't exist".to_string();
                    log::warn!("{msg}");
                    return Err(UStatus::fail_with_code(UCode::NOT_FOUND, msg));
                }
            } else {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    "Sink should not be None in Response",
                ));
            }
        }

        Ok(())
    }
}
