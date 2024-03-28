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
    CallOptions, Data, RpcClient, UMessage, UMessageBuilder, UPayload, UPayloadFormat, UStatus,
    UTransport, UUIDBuilder,
};
use zenoh::config::Config;

#[async_std::test]
async fn test_rpc_server_client() {
    test_lib::before_test();

    // Initialization
    let upclient_client = UPClientZenoh::new(Config::default()).await.unwrap();
    let upclient_server = Arc::new(UPClientZenoh::new(Config::default()).await.unwrap());
    let request_data = String::from("This is the request data");
    let response_data = String::from("This is the response data");
    let dst_uuri = test_lib::create_rpcserver_uuri();

    // Setup RpcServer callback
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
                // Check the payload of request
                if let Data::Value(v) = payload.unwrap().data.unwrap() {
                    let value = v.into_iter().map(|c| c as char).collect::<String>();
                    assert_eq!(request_data_cloned, value);
                } else {
                    panic!("The message should be Data::Value type.");
                }
                // Send back result
                let umessage = UMessageBuilder::response_for_request(&attributes)
                    .with_message_id(UUIDBuilder::new().build())
                    .build_with_payload(
                        response_data_cloned.as_bytes().to_vec().into(),
                        UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
                    )
                    .unwrap();
                block_on(upclient_server_cloned.send(umessage)).unwrap();
            }
            Err(ustatus) => {
                panic!("Internal Error: {ustatus:?}");
            }
        }
    };
    let listener_string = upclient_server
        .register_listener(dst_uuri.clone(), Box::new(callback))
        .await
        .unwrap();

    // Need some time for queryable to run
    task::sleep(time::Duration::from_millis(1000)).await;

    // Send Request with invoke_method
    {
        let payload = UPayload {
            format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
            data: Some(Data::Value(request_data.as_bytes().to_vec())),
            ..Default::default()
        };
        let result = upclient_client
            .invoke_method(
                dst_uuri.clone(),
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
            assert_eq!(response_data.clone(), value);
        } else {
            panic!("Failed to get result from invoke_method.");
        }
    }

    // Send Request with send
    {
        // Register Response callback
        let callback = move |result: Result<UMessage, UStatus>| {
            match result {
                Ok(msg) => {
                    let UMessage { payload, .. } = msg;
                    // Check the response data
                    if let Data::Value(v) = payload.unwrap().data.unwrap() {
                        let value = v.into_iter().map(|c| c as char).collect::<String>();
                        assert_eq!(response_data, value);
                    } else {
                        panic!("The message should be Data::Value type.");
                    }
                }
                Err(ustatus) => {
                    panic!("Internal Error: {ustatus:?}");
                }
            }
        };
        let response_uuri = upclient_client.get_response_uuri();
        upclient_client
            .register_listener(response_uuri.clone(), Box::new(callback))
            .await
            .unwrap();
        // Send request
        let umessage = UMessageBuilder::request(dst_uuri.clone(), response_uuri, 3000)
            .with_message_id(UUIDBuilder::new().build())
            .build_with_payload(
                request_data.as_bytes().to_vec().into(),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        // TODO: We should not have reqid here, but validate Request will fail
        let UMessage {
            attributes,
            payload,
            ..
        } = umessage;
        let mut attributes = attributes.unwrap();
        attributes.reqid = Some(UUIDBuilder::new().build()).into();
        let umessage = UMessage {
            attributes: Some(attributes).into(),
            payload,
            ..Default::default()
        };
        upclient_client.send(umessage).await.unwrap();

        // Waiting for the callback to process data
        task::sleep(time::Duration::from_millis(1000)).await;
    }

    // Cleanup
    upclient_server
        .unregister_listener(dst_uuri.clone(), &listener_string)
        .await
        .unwrap();
}
