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
use tokio::time::{sleep, Duration};
use up_rust::{UListener, UMessage, UMessageBuilder, UPayloadFormat, UStatus, UTransport, UUri};

struct PublishNotificationListener {
    recv_data: Arc<Mutex<String>>,
}
impl PublishNotificationListener {
    fn new() -> Self {
        PublishNotificationListener {
            recv_data: Arc::new(Mutex::new(String::new())),
        }
    }
    fn get_recv_data(&self) -> String {
        self.recv_data.lock().unwrap().clone()
    }
}
#[async_trait]
impl UListener for PublishNotificationListener {
    async fn on_receive(&self, msg: UMessage) {
        let data = msg.payload.unwrap();
        let value = data.into_iter().map(|c| c as char).collect::<String>();
        *self.recv_data.lock().unwrap() = value;
    }
    async fn on_error(&self, err: UStatus) {
        panic!("Internal Error: {err:?}");
    }
}

#[test_case(&test_lib::new_uuri("publisher", 1, 1, 0x8000), &test_lib::new_uuri("publisher", 1, 1, 0x8000); "Normal UUri")]
#[test_case(&test_lib::new_uuri("publisher", 2, 1, 0x8001), &test_lib::new_uuri("publisher", 0xFFFF, 0xFF, 0xFFFF); "Special UUri")]
#[tokio::test(flavor = "multi_thread")]
async fn test_publish_and_subscribe(publish_uuri: &UUri, listen_uuri: &UUri) {
    test_lib::before_test();

    // Initialization
    let target_data = String::from("Hello World!");
    let upclient_send = test_lib::create_up_client_zenoh("publisher").await.unwrap();
    let upclient_recv = test_lib::create_up_client_zenoh("subscriber")
        .await
        .unwrap();

    // Register the listener
    let pub_listener = Arc::new(PublishNotificationListener::new());
    upclient_recv
        .register_listener(listen_uuri, None, pub_listener.clone())
        .await
        .unwrap();
    // Waiting for listener to take effect
    sleep(Duration::from_millis(1000)).await;

    // Send UMessage
    let umessage = UMessageBuilder::publish((*publish_uuri).clone())
        .build_with_payload(target_data.clone(), UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
        .unwrap();
    upclient_send.send(umessage).await.unwrap();

    // Waiting for the subscriber to receive data
    sleep(Duration::from_millis(1000)).await;

    // Compare the result
    assert_eq!(pub_listener.get_recv_data(), target_data);

    // Cleanup
    upclient_recv
        .unregister_listener(&listen_uuri.clone(), None, pub_listener)
        .await
        .unwrap();
}

#[test_case(&test_lib::new_uuri("sender", 1, 1, 0x8000), &test_lib::new_uuri("receiver", 2, 1, 0), &test_lib::new_uuri("*", 0xFFFF, 0xFF, 0xFFFF), Some(&test_lib::new_uuri("receiver", 2, 1, 0)); "Any source UUri")]
#[test_case(&test_lib::new_uuri("sender", 1, 1, 0x8000), &test_lib::new_uuri("receiver", 2, 1, 0), &test_lib::new_uuri("sender", 1, 1, 0x8000), Some(&test_lib::new_uuri("receiver", 2, 1, 0)); "Specific source UUri")]
#[tokio::test(flavor = "multi_thread")]
async fn test_notification_and_subscribe(
    src_uuri: &UUri,
    sink_uuri: &UUri,
    src_filter: &UUri,
    sink_filter: Option<&UUri>,
) {
    test_lib::before_test();

    // Initialization
    let target_data = String::from("Hello World!");
    let upclient_sender = test_lib::create_up_client_zenoh("sender").await.unwrap();
    let upclient_receiver = test_lib::create_up_client_zenoh("receiver").await.unwrap();

    // Register the listener
    let notification_listener = Arc::new(PublishNotificationListener::new());
    upclient_receiver
        .register_listener(src_filter, sink_filter, notification_listener.clone())
        .await
        .unwrap();
    // Waiting for listener to take effect
    sleep(Duration::from_millis(1000)).await;

    // Send UMessage
    let umessage = UMessageBuilder::notification((*src_uuri).clone(), (*sink_uuri).clone())
        .build_with_payload(target_data.clone(), UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
        .unwrap();
    upclient_sender.send(umessage).await.unwrap();

    // Waiting for the subscriber to receive data
    sleep(Duration::from_millis(1000)).await;

    // Compare the result
    assert_eq!(notification_listener.get_recv_data(), target_data);

    // Cleanup
    upclient_receiver
        .unregister_listener(src_filter, sink_filter, notification_listener)
        .await
        .unwrap();
}
