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
pub mod test_lib;

use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use test_case::test_case;
use tokio::{
    runtime::Handle,
    task,
    time::{sleep, Duration},
};
use up_client_zenoh::UPClientZenoh;
use up_rust::{
    RpcClient, UListener, UMessage, UMessageBuilder, UPayloadFormat, UStatus, UTransport, UUri,
};

struct RequestListener {
    up_client: Arc<UPClientZenoh>,
    request_data: String,
    response_data: String,
}
impl RequestListener {
    fn new(up_client: Arc<UPClientZenoh>, request_data: String, response_data: String) -> Self {
        RequestListener {
            up_client,
            request_data,
            response_data,
        }
    }
}
#[async_trait]
impl UListener for RequestListener {
    async fn on_receive(&self, msg: UMessage) {
        let UMessage {
            attributes,
            payload,
            ..
        } = msg;
        // Check the payload of request
        let value = payload
            .unwrap()
            .into_iter()
            .map(|c| c as char)
            .collect::<String>();
        assert_eq!(self.request_data, value);
        // Send back result
        let umessage = UMessageBuilder::response_for_request(&attributes)
            .build_with_payload(
                self.response_data.clone(),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        task::block_in_place(|| {
            Handle::current()
                .block_on(self.up_client.send(umessage))
                .unwrap();
        });
    }
    async fn on_error(&self, err: UStatus) {
        panic!("Internal Error: {err:?}");
    }
}

struct ResponseListener {
    response_data: Arc<Mutex<String>>,
}
impl ResponseListener {
    fn new() -> Self {
        ResponseListener {
            response_data: Arc::new(Mutex::new(String::new())),
        }
    }
    fn get_response_data(&self) -> String {
        self.response_data.lock().unwrap().clone()
    }
}
#[async_trait]
impl UListener for ResponseListener {
    async fn on_receive(&self, msg: UMessage) {
        let UMessage { payload, .. } = msg;
        // Check the response data
        let value = payload
            .unwrap()
            .into_iter()
            .map(|c| c as char)
            .collect::<String>();
        *self.response_data.lock().unwrap() = value;
    }
    async fn on_error(&self, err: UStatus) {
        panic!("Internal Error: {err:?}");
    }
}

#[test_case(&test_lib::new_uuri("requester", 1, 1, 0), &test_lib::new_uuri("responder", 2, 1, 1), &test_lib::new_uuri("*", 0xFFFF, 0xFF, 0xFFFF), Some(&test_lib::new_uuri("responder", 2, 1, 1)); "Any source UUri")]
#[test_case(&test_lib::new_uuri("requester", 1, 1, 0), &test_lib::new_uuri("responder", 2, 1, 1), &test_lib::new_uuri("requester", 1, 1, 0), Some(&test_lib::new_uuri("responder", 2, 1, 1)); "Specific source UUri")]
#[tokio::test(flavor = "multi_thread")]
async fn test_rpc_server_client(
    src_uuri: &UUri,
    sink_uuri: &UUri,
    src_filter: &UUri,
    sink_filter: Option<&UUri>,
) {
    test_lib::before_test();

    // Initialization
    let upclient_client = test_lib::create_up_client_zenoh("requester").await.unwrap();
    let upclient_server = Arc::new(test_lib::create_up_client_zenoh("responder").await.unwrap());
    let request_data = String::from("This is the request data");
    let response_data = String::from("This is the response data");

    // Setup RpcServer callback
    let request_listener = Arc::new(RequestListener::new(
        upclient_server.clone(),
        request_data.clone(),
        response_data.clone(),
    ));
    upclient_server
        .register_listener(src_filter, sink_filter, request_listener.clone())
        .await
        .unwrap();
    // Need some time for queryable to run
    sleep(Duration::from_millis(1000)).await;

    // Send Request with invoke_method
    {
        let result = upclient_client
            .invoke_method(
                (*sink_uuri).clone(),
                UMessageBuilder::request((*sink_uuri).clone(), (*src_uuri).clone(), 1000)
                    .build_with_payload(request_data.clone(), UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
                    .unwrap(),
            )
            .await;

        // Process the result
        let payload = result.unwrap().payload.unwrap();
        let value = payload.into_iter().map(|c| c as char).collect::<String>();
        assert_eq!(response_data.clone(), value);
    }

    // Send Request with send
    {
        // Register Response callback
        let response_listener = Arc::new(ResponseListener::new());
        upclient_client
            .register_listener(sink_uuri, Some(src_uuri), response_listener.clone())
            .await
            .unwrap();

        // Send request
        let umessage = UMessageBuilder::request((*sink_uuri).clone(), (*src_uuri).clone(), 1000)
            .build_with_payload(request_data.clone(), UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
            .unwrap();
        upclient_client.send(umessage).await.unwrap();

        // Waiting for the callback to process data
        sleep(Duration::from_millis(2000)).await;

        // Compare the result
        assert_eq!(response_listener.get_response_data(), response_data);

        // Cleanup
        upclient_client
            .unregister_listener(sink_uuri, Some(src_uuri), response_listener.clone())
            .await
            .unwrap();
    }

    // Cleanup
    upclient_server
        .unregister_listener(src_filter, sink_filter, request_listener.clone())
        .await
        .unwrap();
}
