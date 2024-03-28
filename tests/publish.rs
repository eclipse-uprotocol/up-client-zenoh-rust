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

use async_std::task;
use std::time;
use up_client_zenoh::UPClientZenoh;
use up_rust::{Data, UMessage, UMessageBuilder, UPayloadFormat, UStatus, UTransport, UUIDBuilder};
use zenoh::config::Config;

#[async_std::test]
async fn test_publish_and_subscribe() {
    test_lib::before_test();

    // Initialization
    let target_data = String::from("Hello World!");
    let upclient = UPClientZenoh::new(Config::default()).await.unwrap();
    let uuri = test_lib::create_utransport_uuri(0);

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

    // Send UMessage
    let umessage = UMessageBuilder::publish(uuri.clone())
        .with_message_id(UUIDBuilder::new().build())
        .build_with_payload(
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
    test_lib::before_test();

    // Initialization
    let target_data = String::from("Hello World!");
    let upclient = UPClientZenoh::new(Config::default()).await.unwrap();
    let src_uuri = test_lib::create_utransport_uuri(0);
    let dst_uuri = test_lib::create_utransport_uuri(1);

    // Register the listener
    let uuri_cloned = dst_uuri.clone();
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
        .register_listener(dst_uuri.clone(), Box::new(listener))
        .await
        .unwrap();

    // Send UMessage
    // TODO: up_rust has bugs while creating UMessageBuilder::notification
    let umessage = UMessageBuilder::notification(src_uuri.clone(), dst_uuri.clone())
        .with_message_id(UUIDBuilder::new().build())
        .build_with_payload(
            target_data.as_bytes().to_vec().into(),
            UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
        )
        .unwrap();
    upclient.send(umessage).await.unwrap();

    // Waiting for the subscriber to receive data
    task::sleep(time::Duration::from_millis(1000)).await;

    // Cleanup
    upclient
        .unregister_listener(dst_uuri.clone(), &listener_string)
        .await
        .unwrap();
}
