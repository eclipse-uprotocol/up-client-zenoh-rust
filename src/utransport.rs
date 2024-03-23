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
use std::{sync::Arc, time::Duration};
use up_rust::ComparableListener;
use up_rust::UListener;
use up_rust::{
    Data, UAttributes, UAttributesValidators, UCode, UMessage, UMessageType, UPayload,
    UPayloadFormat, UStatus, UTransport, UUri, UriValidator,
};
use zenoh::sample::Attachment;
use zenoh::{
    prelude::{r#async::*, Sample},
    query::Reply,
    queryable::Query,
};

impl UPClientZenoh {
    async fn send_and_process_response_callbacks(
        &self,
        zenoh_key: &str,
        value: Value,
        attachment: Attachment,
        listener: Arc<ComparableListener>,
    ) -> Result<(), UStatus> {
        let zenoh_callback = move |reply: Reply| {
            let res = match reply.sample {
                Ok(sample) => {
                    let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
                        listener.on_error(UStatus::fail_with_code(
                            UCode::INTERNAL,
                            "Unable to get the encoding",
                        ));
                        return;
                    };
                    // TODO: Get the attributes
                    // Create UAttribute
                    let Some(attachment) = sample.attachment() else {
                        listener.on_error(UStatus::fail_with_code(
                            UCode::INTERNAL,
                            "Unable to get attachment",
                        ));
                        return;
                    };
                    let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                        Ok(uattr) => uattr,
                        Err(e) => {
                            log::error!("attachment_to_uattributes error: {:?}", e);
                            listener.on_error(UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to decode attribute",
                            ));
                            return;
                        }
                    };
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
                Err(e) => Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Error while parsing Zenoh reply: {e:?}"),
                )),
            };
            match res {
                Ok(msg) => {
                    listener.on_receive(msg);
                }
                Err(err) => {
                    listener.on_error(err);
                }
            }
        };

        // TODO: Adjust the timeout
        let getbuilder = self
            .session
            .get(zenoh_key)
            .with_value(value.clone())
            .with_attachment(attachment.clone())
            .target(QueryTarget::BestMatching)
            .timeout(Duration::from_millis(1000))
            .callback(zenoh_callback);
        getbuilder.res().await.map_err(|e| {
            log::error!("Zenoh error: {e:?}");
            UStatus::fail_with_code(UCode::INTERNAL, "Unable to send get with Zenoh")
        })?;

        Ok(())
    }

    async fn send_publish(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // Assume we only have Value here, no reference for shared memory
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "The data in UPayload should be Data::Value",
            ));
        };

        // Serialized UAttributes into protobuf
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid uAttributes",
            ));
        };

        let priority =
            UPClientZenoh::map_zenoh_priority(attributes.priority.enum_value().map_err(|_| {
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid priority")
            })?);

        let putbuilder = self
            .session
            .put(zenoh_key, buf)
            .encoding(Encoding::WithSuffix(
                KnownEncoding::AppCustom,
                payload.format.value().to_string().into(),
            ))
            .priority(priority)
            .with_attachment(attachment.build());

        // Send data
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
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "The data in UPayload should be Data::Value",
            ));
        };

        // Serialized UAttributes into protobuf
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid uAttributes",
            ));
        };
        let attachment = attachment.build();

        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));

        let resp_listener_clone = attributes.source.0.clone().ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Lack of source address",
        ))?;

        let callbacks = {
            let rpc_callback_map = self.rpc_callback_map.lock().unwrap();
            rpc_callback_map
                .get(&resp_listener_clone)
                .map_or_else(Vec::new, |listener_set| {
                    listener_set.iter().cloned().collect::<Vec<_>>()
                })
        };

        if callbacks.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::NOT_FOUND,
                format!("No listeners registered for topic: {resp_listener_clone}",),
            ));
        }

        for listener in callbacks {
            self.send_and_process_response_callbacks(
                zenoh_key,
                value.clone(),
                attachment.clone(),
                listener.clone(),
            )
            .await?;
        }

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
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "The data in UPayload should be Data::Value",
            ));
        };

        // Serialized UAttributes into protobuf
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            log::error!("send_response: Invalid UAttributes");
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid UAttributes",
            ));
        };
        // Get reqid
        let reqid = attributes.reqid.to_string();

        // Send back query
        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        let reply = Ok(Sample::new(
            KeyExpr::new(zenoh_key.to_string()).map_err(|e| {
                log::error!("Unable to create Zenoh key: {e:?}");
                UStatus::fail_with_code(UCode::INTERNAL, "Unable to create Zenoh key")
            })?,
            value,
        ));
        let query = self
            .query_map
            .lock()
            .unwrap()
            .get(&reqid)
            .ok_or(UStatus::fail_with_code(
                UCode::INTERNAL,
                "query doesn't exist",
            ))?
            .clone();

        // Send data
        query
            .reply(reply)
            .with_attachment(attachment.build())
            .map_err(|_| {
                // TODO: The latest Zenoh version can print error.
                log::error!("Unable to add attachment");
                UStatus::fail_with_code(UCode::INTERNAL, "Unable to add attachment")
            })?
            .res()
            .await
            .map_err(|e| {
                log::error!("Unable to reply with Zenoh: {e:?}");
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to reply with Zenoh: {e:?}"),
                )
            })?;

        Ok(())
    }
    async fn register_publish_listener(
        &self,
        topic: &UUri,
        listener_wrapper: ComparableListener,
    ) -> Result<(), UStatus> {
        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(topic)?;

        // Setup callback
        let callback_listener_wrapper = listener_wrapper.clone();
        let callback = move |sample: Sample| {
            let listener_wrapper = callback_listener_wrapper.clone();
            // Create UAttribute
            let Some(attachment) = sample.attachment() else {
                listener_wrapper.on_error(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to get attachment",
                ));
                return;
            };
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    log::error!("attachment_to_uattributes error: {:?}", e);
                    listener_wrapper.on_error(UStatus::fail_with_code(
                        UCode::INTERNAL,
                        "Unable to decode attribute",
                    ));
                    return;
                }
            };
            // Create UPayload
            let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
                listener_wrapper.on_error(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to get payload encoding",
                ));
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
            listener_wrapper.on_receive(msg);
        };
        if let Ok(subscriber) = self
            .session
            .declare_subscriber(&zenoh_key)
            .callback_mut(callback)
            .res()
            .await
        {
            let mut subscribers = self.subscriber_map.lock().unwrap();
            subscribers.insert((topic.clone(), listener_wrapper), subscriber);
        } else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to register callback with Zenoh",
            ));
        }

        Ok(())
    }

    async fn register_request_listener(
        &self,
        topic: &UUri,
        listener_wrapper: ComparableListener,
    ) -> Result<(), UStatus> {
        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(topic)?;

        let query_map = self.query_map.clone();
        let callback_listener_wrapper = listener_wrapper.clone();
        // Setup callback
        let callback = move |query: Query| {
            let listener_wrapper = callback_listener_wrapper.clone();
            // Create UAttribute
            let Some(attachment) = query.attachment() else {
                listener_wrapper.on_error(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to get attachment",
                ));
                return;
            };
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    log::error!("attachment_to_uattributes error: {:?}", e);
                    listener_wrapper.on_error(UStatus::fail_with_code(
                        UCode::INTERNAL,
                        "Unable to decode attribute",
                    ));
                    return;
                }
            };
            // Create UPayload
            let u_payload = match query.value() {
                Some(value) => {
                    let Some(encoding) = UPClientZenoh::to_upayload_format(&value.encoding) else {
                        listener_wrapper.on_error(UStatus::fail_with_code(
                            UCode::INTERNAL,
                            "Unable to get payload encoding",
                        ));
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
            // Create UMessage
            let msg = UMessage {
                attributes: Some(u_attribute.clone()).into(),
                payload: Some(u_payload).into(),
                ..Default::default()
            };
            query_map
                .lock()
                .unwrap()
                .insert(u_attribute.id.to_string(), query);
            listener_wrapper.on_receive(msg);
        };
        if let Ok(queryable) = self
            .session
            .declare_queryable(&zenoh_key)
            .callback_mut(callback)
            .res()
            .await
        {
            let mut queryables = self.queryable_map.lock().unwrap();
            queryables.insert((topic.clone(), listener_wrapper), queryable);
        } else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to register callback with Zenoh",
            ));
        }

        Ok(())
    }

    fn register_response_listener(
        &self,
        topic: &UUri,
        listener_wrapper: ComparableListener,
    ) -> Result<(), UStatus> {
        let mut rpc_callback_map_guard = self.rpc_callback_map.lock().unwrap();

        let listeners = rpc_callback_map_guard.entry(topic.clone()).or_default();

        let newly_added = listeners.insert(Arc::new(listener_wrapper));
        if !newly_added {
            return Err(UStatus::fail_with_code(
                UCode::ALREADY_EXISTS,
                format!("Same listener already registered to topic: {topic:?}"),
            ));
        }

        Ok(())
    }

    fn unregister_publish_listener(
        &self,
        topic: &UUri,
        listener: &ComparableListener,
    ) -> Result<(), UStatus> {
        let mut subscriber_map = self.subscriber_map.lock().unwrap();
        if let Some(_subscriber) = subscriber_map.remove(&(topic.clone(), listener.clone())) {
            return Ok(());
        }
        Err(UStatus::fail_with_code(
            UCode::NOT_FOUND,
            format!("Publish listener not found: {topic:?}"),
        ))
    }

    fn unregister_request_listener(
        &self,
        topic: &UUri,
        listener: &ComparableListener,
    ) -> Result<(), UStatus> {
        let mut queryable_map = self.queryable_map.lock().unwrap();
        if let Some(_queryable) = queryable_map.remove(&(topic.clone(), listener.clone())) {
            return Ok(());
        }
        Err(UStatus::fail_with_code(
            UCode::NOT_FOUND,
            format!("Request listener not found: {topic:?}"),
        ))
    }

    fn unregister_response_listener(
        &self,
        topic: &UUri,
        listener: &ComparableListener,
    ) -> Result<(), UStatus> {
        let mut rpc_callback_map = self.rpc_callback_map.lock().unwrap();
        if let Some(callbacks) = rpc_callback_map.get_mut(topic) {
            if callbacks.remove(listener) {
                return Ok(());
            }
        }
        Err(UStatus::fail_with_code(
            UCode::NOT_FOUND,
            format!("Response listener not found: {topic:?}"),
        ))
    }
}

#[async_trait]
impl UTransport for UPClientZenoh {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let payload = *message.payload.0.ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Invalid uPayload",
        ))?;
        let attributes = *message.attributes.0.ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Invalid uAttributes",
        ))?;

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
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Publish UAttributes {e:?}"),
                        )
                    })?;
                // Get Zenoh key
                let topic = if attributes.sink.is_some() {
                    // If sink is not empty, this is Notification => topic is sink
                    attributes.clone().sink
                } else {
                    // If sink is empty, this is Publication => topic is source
                    attributes.clone().source
                };
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Publish
                self.send_publish(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_REQUEST => {
                UAttributesValidators::Request
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Request UAttributes {e:?}"),
                        )
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
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Response UAttributes {e:?}"),
                        )
                    })?;
                // Get Zenoh key
                let topic = attributes.clone().source;
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Response
                self.send_response(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_UNSPECIFIED => Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Wrong Message type in UAttributes",
            )),
        }
    }

    async fn receive(&self, _topic: UUri) -> Result<UMessage, UStatus> {
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "Not implemented",
        ))
    }

    async fn register_listener<T>(&self, topic: UUri, listener: Arc<T>) -> Result<(), UStatus>
    where
        T: UListener,
    {
        let listener_wrapper = ComparableListener::new(listener);

        if topic.authority.is_some() && topic.entity.is_none() && topic.resource.is_none() {
            // This is special UUri which means we need to register for all of Publish, Request, and Response
            // RPC response
            self.register_response_listener(&topic, listener_wrapper.clone())?;
            // RPC request
            self.register_request_listener(&topic, listener_wrapper.clone())
                .await?;
            // Normal publish
            self.register_publish_listener(&topic, listener_wrapper)
                .await?;
            Ok(())
        } else {
            // Do the validation
            UriValidator::validate(&topic)
                .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;

            if UriValidator::is_rpc_response(&topic) {
                // RPC response
                self.register_response_listener(&topic, listener_wrapper)
            } else if UriValidator::is_rpc_method(&topic) {
                // RPC request
                self.register_request_listener(&topic, listener_wrapper)
                    .await
            } else {
                // Normal publish
                self.register_publish_listener(&topic, listener_wrapper)
                    .await
            }
        }
    }

    async fn unregister_listener<T>(&self, topic: UUri, listener: Arc<T>) -> Result<(), UStatus>
    where
        T: UListener,
    {
        let listener_wrapper = ComparableListener::new(listener);
        let message_type =
            if topic.authority.is_some() && topic.entity.is_none() && topic.resource.is_none() {
                UMessageType::UMESSAGE_TYPE_UNSPECIFIED
            } else {
                UriValidator::validate(&topic).map_err(|_| {
                    UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic")
                })?;

                if UriValidator::is_rpc_response(&topic) {
                    UMessageType::UMESSAGE_TYPE_RESPONSE
                } else if UriValidator::is_rpc_method(&topic) {
                    UMessageType::UMESSAGE_TYPE_REQUEST
                } else {
                    UMessageType::UMESSAGE_TYPE_PUBLISH
                }
            };

        match message_type {
            UMessageType::UMESSAGE_TYPE_UNSPECIFIED => {
                let results = vec![
                    self.unregister_response_listener(&topic, &listener_wrapper),
                    self.unregister_request_listener(&topic, &listener_wrapper),
                    self.unregister_publish_listener(&topic, &listener_wrapper),
                ];

                // Filter out the Ok results, collect errors
                let errors: Vec<UStatus> = results.into_iter().filter_map(Result::err).collect();

                if !errors.is_empty() {
                    let first_error = errors[0].clone(); // Assuming UStatus implements Clone.
                    let error_messages: Vec<String> =
                        errors.into_iter().filter_map(|e| e.message).collect();
                    let combined_message = error_messages.join("; ");

                    return Err(UStatus {
                        code: first_error.code, // Preserve the first error code
                        message: Some(combined_message),
                        details: vec![],
                        special_fields: ::protobuf::SpecialFields::default(),
                    });
                }
            }
            UMessageType::UMESSAGE_TYPE_RESPONSE => {
                self.unregister_response_listener(&topic, &listener_wrapper)?;
            }
            UMessageType::UMESSAGE_TYPE_REQUEST => {
                self.unregister_request_listener(&topic, &listener_wrapper)?;
            }
            UMessageType::UMESSAGE_TYPE_PUBLISH => {
                self.unregister_publish_listener(&topic, &listener_wrapper)?;
            }
        }

        Ok(())
    }
}
