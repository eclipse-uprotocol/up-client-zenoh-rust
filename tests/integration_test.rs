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
use async_std::task::{self, block_on};
use std::{sync::Arc, time};
use up_client_zenoh::UPClientZenoh;
use up_rust::{
    rpc::{CallOptionsBuilder, RpcClient, RpcServer},
    transport::{builder::UMessageBuilder, datamodel::UTransport},
    uprotocol::{
        uri::uauthority::Number, Data, UAuthority, UCode, UEntity, UMessage, UMessageType,
        UPayload, UPayloadFormat, UResource, UStatus, UUri,
    },
    uri::builder::resourcebuilder::UResourceBuilder,
    uuid::builder::UUIDBuilder,
};

fn create_utransport_uuri(index: u8) -> UUri {
    if index == 1 {
        UUri {
            entity: Some(UEntity {
                name: "entity1".to_string(),
                version_major: Some(1),
                id: Some(1111),
                ..Default::default()
            })
            .into(),
            resource: Some(UResource {
                name: "name1".to_string(),
                instance: Some("instance1".to_string()),
                message: Some("message1".to_string()),
                id: Some(1111),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        }
    } else {
        UUri {
            entity: Some(UEntity {
                name: "body.access".to_string(),
                version_major: Some(1),
                id: Some(1234),
                ..Default::default()
            })
            .into(),
            resource: Some(UResource {
                name: "door".to_string(),
                instance: Some("front_left".to_string()),
                message: Some("Door".to_string()),
                id: Some(5678),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        }
    }
}

fn create_rpcserver_uuri() -> UUri {
    UUri {
        entity: Some(UEntity {
            name: "test_rpc.app".to_string(),
            version_major: Some(1),
            id: Some(1234),
            ..Default::default()
        })
        .into(),
        resource: Some(UResourceBuilder::for_rpc_request(
            Some("SimpleTest".to_string()),
            Some(5678),
        ))
        .into(),
        ..Default::default()
    }
}

fn create_authority() -> UAuthority {
    UAuthority {
        name: Some("UAuthName".to_string()),
        number: Some(Number::Id(vec![1, 2, 3, 10, 11, 12])),
        ..Default::default()
    }
}

fn create_special_uuri() -> UUri {
    UUri {
        authority: Some(create_authority()).into(),
        ..Default::default()
    }
}

#[async_std::test]
async fn test_utransport_register_and_unregister() {
    let upclient = UPClientZenoh::new().await.unwrap();
    let uuri = create_utransport_uuri(0);

    // Compare the return string
    let listener_string = upclient
        .register_listener(uuri.clone(), Box::new(|_| {}))
        .await
        .unwrap();
    assert_eq!(listener_string, "upl/0100162e04d20100_0");

    // Able to ungister
    upclient
        .unregister_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();

    // Unable to ungister
    let result = upclient
        .unregister_listener(uuri.clone(), &listener_string)
        .await;
    assert_eq!(
        result,
        Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Publish listener doesn't exist"
        ))
    );
}

#[async_std::test]
async fn test_rpcserver_register_and_unregister() {
    let upclient = UPClientZenoh::new().await.unwrap();
    let uuri = create_rpcserver_uuri();

    // Compare the return string
    let listener_string = upclient
        .register_rpc_listener(uuri.clone(), Box::new(|_| {}))
        .await
        .unwrap();
    assert_eq!(listener_string, "upl/0100162e04d20100_0");

    // Able to ungister
    upclient
        .unregister_rpc_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();

    // Unable to ungister
    let result = upclient
        .unregister_rpc_listener(uuri.clone(), &listener_string)
        .await;
    assert_eq!(
        result,
        Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "RPC request listener doesn't exist"
        ))
    );
}

#[async_std::test]
async fn test_utransport_special_uuri_register_and_unregister() {
    let upclient = UPClientZenoh::new().await.unwrap();
    let uuri = create_special_uuri();

    // Compare the return string
    let listener_string = upclient
        .register_listener(uuri.clone(), Box::new(|_| {}))
        .await
        .unwrap();
    assert_eq!(
        listener_string,
        "upr/060102030a0b0c/**_0&upr/060102030a0b0c/**_1&upr/060102030a0b0c/**_2"
    );

    // Able to ungister
    upclient
        .unregister_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();

    // Unable to ungister
    let result = upclient
        .unregister_listener(uuri.clone(), &listener_string)
        .await;
    assert_eq!(
        result,
        Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "RPC response callback doesn't exist"
        ))
    );
}

#[async_std::test]
async fn test_publish_and_subscribe() {
    let target_data = String::from("Hello World!");
    let upclient = UPClientZenoh::new().await.unwrap();
    let uuri = create_utransport_uuri(0);

    // Register the listener
    let uuri_cloned = uuri.clone();
    let data_cloned = target_data.clone();
    let listener = move |result: Result<UMessage, UStatus>| match result {
        Ok(msg) => {
            if let Data::Value(v) = msg.payload.unwrap().data.unwrap() {
                let value = v.into_iter().map(|c| c as char).collect::<String>();
                assert_eq!(msg.attributes.unwrap().source.unwrap(), uuri_cloned);
                assert_eq!(value, data_cloned);
            } else {
                panic!("The message should be Data::Value type.");
            }
        }
        Err(ustatus) => panic!("Internal Error: {ustatus:?}"),
    };
    let listener_string = upclient
        .register_listener(uuri.clone(), Box::new(listener))
        .await
        .unwrap();

    let umessage = UMessageBuilder::publish(&uuri)
        .build_with_payload(
            &UUIDBuilder::new(),
            target_data.as_bytes().to_vec().into(),
            UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
        )
        .unwrap();
    upclient.send(umessage).await.unwrap();

    // Waiting for the subscriber to receive data
    task::sleep(time::Duration::from_millis(1000)).await;

    // Cleanup
    upclient
        .unregister_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();
}

#[async_std::test]
async fn test_notification_and_subscribe() {
    let target_data = String::from("Hello World!");
    let upclient = UPClientZenoh::new().await.unwrap();
    let uuri = create_utransport_uuri(1);

    // Register the listener
    let uuri_cloned = uuri.clone();
    let data_cloned = target_data.clone();
    let listener = move |result: Result<UMessage, UStatus>| match result {
        Ok(msg) => {
            if let Data::Value(v) = msg.payload.unwrap().data.unwrap() {
                let value = v.into_iter().map(|c| c as char).collect::<String>();
                assert_eq!(msg.attributes.unwrap().sink.unwrap(), uuri_cloned);
                assert_eq!(value, data_cloned);
            } else {
                panic!("The message should be Data::Value type.");
            }
        }
        Err(ustatus) => panic!("Internal Error: {ustatus:?}"),
    };
    let listener_string = upclient
        .register_listener(uuri.clone(), Box::new(listener))
        .await
        .unwrap();

    let umessage = UMessageBuilder::notification(&uuri)
        .build_with_payload(
            &UUIDBuilder::new(),
            target_data.as_bytes().to_vec().into(),
            UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
        )
        .unwrap();
    upclient.send(umessage).await.unwrap();

    // Waiting for the subscriber to receive data
    task::sleep(time::Duration::from_millis(1000)).await;

    // Cleanup
    upclient
        .unregister_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();
}

#[async_std::test]
async fn test_rpc_server_client() {
    let upclient_client = UPClientZenoh::new().await.unwrap();
    let upclient_server = Arc::new(UPClientZenoh::new().await.unwrap());
    let request_data = String::from("This is the request data");
    let response_data = String::from("This is the response data");
    let uuri = create_rpcserver_uuri();

    // setup RpcServer callback
    let upclient_server_cloned = upclient_server.clone();
    let response_data_cloned = response_data.clone();
    let request_data_cloned = request_data.clone();
    let callback = move |result: Result<UMessage, UStatus>| {
        match result {
            Ok(msg) => {
                let UMessage {
                    attributes,
                    payload,
                    ..
                } = msg;
                // Get the UUri
                let source = attributes.clone().unwrap().source.unwrap();
                let sink = attributes.clone().unwrap().sink.unwrap();
                // Build the payload to send back
                if let Data::Value(v) = payload.unwrap().data.unwrap() {
                    let value = v.into_iter().map(|c| c as char).collect::<String>();
                    assert_eq!(request_data_cloned, value);
                } else {
                    panic!("The message should be Data::Value type.");
                }
                let upayload = UPayload {
                    length: Some(0),
                    format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
                    data: Some(Data::Value(response_data_cloned.as_bytes().to_vec())),
                    ..Default::default()
                };
                // Set the attributes type to Response
                let mut uattributes = attributes.unwrap();
                uattributes.type_ = UMessageType::UMESSAGE_TYPE_RESPONSE.into();
                uattributes.sink = Some(source.clone()).into();
                uattributes.source = Some(sink.clone()).into();
                // Send back result
                block_on(upclient_server_cloned.send(UMessage {
                    attributes: Some(uattributes).into(),
                    payload: Some(upayload).into(),
                    ..Default::default()
                }))
                .unwrap();
            }
            Err(ustatus) => {
                panic!("Internal Error: {ustatus:?}");
            }
        }
    };
    upclient_server
        .register_rpc_listener(uuri.clone(), Box::new(callback))
        .await
        .unwrap();
    // Need some time for queryable to run
    task::sleep(time::Duration::from_millis(1000)).await;

    // Run RpcClient
    let payload = UPayload {
        length: Some(0),
        format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
        data: Some(Data::Value(request_data.as_bytes().to_vec())),
        ..Default::default()
    };
    let result = upclient_client
        .invoke_method(uuri, payload, CallOptionsBuilder::default().build())
        .await;

    // Process the result
    if let Data::Value(v) = result.unwrap().payload.unwrap().data.unwrap() {
        let value = v.into_iter().map(|c| c as char).collect::<String>();
        assert_eq!(response_data, value);
    } else {
        panic!("Failed to get result from invoke_method.");
    }
}

#[async_std::test]
async fn test_register_listener_with_special_uuri() {
    let upclient1 = Arc::new(UPClientZenoh::new().await.unwrap());
    let upclient1_clone = upclient1.clone();
    let upclient2 = UPClientZenoh::new().await.unwrap();
    // Create data
    let publish_data = String::from("Hello World!");
    let publish_data_clone = publish_data.clone();
    let request_data = String::from("This is the request data");
    let request_data_clone = request_data.clone();

    // Register the listener
    let listener_uuri = create_special_uuri();
    let listener = move |result: Result<UMessage, UStatus>| match result {
        Ok(msg) => {
            let UMessage {
                attributes,
                payload,
                ..
            } = msg;
            let value = if let Data::Value(v) = payload.clone().unwrap().data.unwrap() {
                v.into_iter().map(|c| c as char).collect::<String>()
            } else {
                panic!("The message should be Data::Value type.");
            };
            match attributes.type_.enum_value().unwrap() {
                UMessageType::UMESSAGE_TYPE_PUBLISH => {
                    assert_eq!(publish_data_clone, value);
                }
                UMessageType::UMESSAGE_TYPE_REQUEST => {
                    assert_eq!(request_data_clone, value);
                    // Set the attributes type to Response
                    let mut uattributes = attributes.unwrap();
                    uattributes.type_ = UMessageType::UMESSAGE_TYPE_RESPONSE.into();
                    // Swap source and sink
                    (uattributes.sink, uattributes.source) =
                        (uattributes.source.clone(), uattributes.sink.clone());
                    // Send back result
                    block_on(upclient1_clone.send(UMessage {
                        attributes: Some(uattributes).into(),
                        payload,
                        ..Default::default()
                    }))
                    .unwrap();
                }
                UMessageType::UMESSAGE_TYPE_RESPONSE => {
                    panic!("Response type");
                }
                UMessageType::UMESSAGE_TYPE_UNSPECIFIED => {
                    panic!("Unknown type");
                }
            }
        }
        Err(ustatus) => panic!("Internal Error: {ustatus:?}"),
    };
    let listener_string = upclient1
        .register_listener(listener_uuri.clone(), Box::new(listener))
        .await
        .unwrap();

    // send Publish
    {
        let mut publish_uuri = create_utransport_uuri(0);
        publish_uuri.authority = Some(create_authority()).into();

        let umessage = UMessageBuilder::publish(&publish_uuri)
            .build_with_payload(
                &UUIDBuilder::new(),
                publish_data.as_bytes().to_vec().into(),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        upclient2.send(umessage).await.unwrap();

        // Waiting for the subscriber to receive data
        task::sleep(time::Duration::from_millis(1000)).await;
    }
    // send Request
    {
        let mut request_uuri = create_rpcserver_uuri();
        request_uuri.authority = Some(create_authority()).into();

        // Run RpcClient
        let payload = UPayload {
            length: Some(0),
            format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
            data: Some(Data::Value(request_data.as_bytes().to_vec())),
            ..Default::default()
        };
        let result = upclient2
            .invoke_method(request_uuri, payload, CallOptionsBuilder::default().build())
            .await;
        // Process the result
        if let Data::Value(v) = result.unwrap().payload.unwrap().data.unwrap() {
            let value = v.into_iter().map(|c| c as char).collect::<String>();
            assert_eq!(request_data, value);
        } else {
            panic!("Failed to get result from invoke_method.");
        }
    }

    // Cleanup
    upclient1
        .unregister_listener(listener_uuri, &listener_string)
        .await
        .unwrap();
}
