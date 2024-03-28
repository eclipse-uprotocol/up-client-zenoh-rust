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
pub mod test_lib;

use async_std::task::{self, block_on};
use std::{sync::Arc, time};
use up_client_zenoh::UPClientZenoh;
use up_rust::{
    CallOptions, Data, RpcClient, UMessage, UMessageBuilder, UMessageType, UPayload,
    UPayloadFormat, UStatus, UTransport, UUIDBuilder,
};
use zenoh::config::Config;

#[async_std::test]
async fn test_register_listener_with_special_uuri() {
    test_lib::before_test();

    // Initialization
    let upclient1 = Arc::new(UPClientZenoh::new(Config::default()).await.unwrap());
    let upclient1_clone = upclient1.clone();
    let upclient2 = UPClientZenoh::new(Config::default()).await.unwrap();
    let publish_data = String::from("Hello World!");
    let request_data = String::from("This is the request data");
    let response_data = String::from("This is the request data");

    // Register the listener
    let publish_data_cloned = publish_data.clone();
    let request_data_cloned = request_data.clone();
    let response_data_cloned = response_data.clone();
    let listener_uuri = test_lib::create_special_uuri();
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
                    assert_eq!(publish_data_cloned, value);
                }
                UMessageType::UMESSAGE_TYPE_NOTIFICATION => {
                    panic!("Notification type");
                }
                UMessageType::UMESSAGE_TYPE_REQUEST => {
                    assert_eq!(request_data_cloned, value);
                    // Send back result
                    let umessage = UMessageBuilder::response_for_request(&attributes)
                        .with_message_id(UUIDBuilder::new().build())
                        .build_with_payload(
                            response_data_cloned.as_bytes().to_vec().into(),
                            UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
                        )
                        .unwrap();
                    block_on(upclient1_clone.send(umessage)).unwrap();
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

    // Send Publish
    {
        // Initialization
        let mut publish_uuri = test_lib::create_utransport_uuri(0);
        publish_uuri.authority = Some(test_lib::create_authority()).into();

        // Send Publish data
        let umessage = UMessageBuilder::publish(publish_uuri)
            .with_message_id(UUIDBuilder::new().build())
            .build_with_payload(
                publish_data.as_bytes().to_vec().into(),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        upclient2.send(umessage).await.unwrap();

        // Waiting for the subscriber to receive data
        task::sleep(time::Duration::from_millis(1000)).await;
    }

    // Send Request
    {
        // Initialization
        let mut request_uuri = test_lib::create_rpcserver_uuri();
        request_uuri.authority = Some(test_lib::create_authority()).into();

        // RpcClient: Send Request data
        let payload = UPayload {
            format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
            data: Some(Data::Value(request_data.as_bytes().to_vec())),
            ..Default::default()
        };
        let result = upclient2
            .invoke_method(
                request_uuri,
                payload,
                CallOptions {
                    ttl: 1000,
                    ..Default::default()
                },
            )
            .await;

        // Process the result
        if let Data::Value(v) = result.unwrap().payload.unwrap().data.unwrap() {
            let value = v.into_iter().map(|c| c as char).collect::<String>();
            assert_eq!(response_data, value);
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
