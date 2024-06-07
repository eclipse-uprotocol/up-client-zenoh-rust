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

use std::sync::Arc;

use async_trait::async_trait;
use test_case::test_case;
use up_rust::{UListener, UMessage, UStatus, UTransport, UUri};

struct FooListener;
#[async_trait]
impl UListener for FooListener {
    async fn on_receive(&self, _msg: UMessage) {}
    async fn on_error(&self, _err: UStatus) {}
}

#[test_case(&test_lib::new_uuri("src", 1, 1, 0x8000), None; "Listen to Publish")]
#[test_case(&test_lib::new_uuri("*", 0xFFFF, 0xFF, 0xFFFF), Some(&test_lib::new_uuri("dst", 2, 1, 0)); "Listen to Notification")]
#[test_case(&test_lib::new_uuri("src", 1, 1, 0x8000), Some(&test_lib::new_uuri("dst", 2, 1, 0)); "Listen to specific Notification")]
#[test_case(&test_lib::new_uuri("*", 0xFFFF, 0xFF, 0xFFFF), Some(&test_lib::new_uuri("dst", 2, 1, 0x0001)); "Listen to Request")]
#[test_case(&test_lib::new_uuri("src", 1, 1, 0), Some(&test_lib::new_uuri("dst", 2, 1, 0x0001)); "Listen to specific Request")]
#[test_case(&test_lib::new_uuri("*", 0xFFFF, 0xFF, 0xFFFF), Some(&test_lib::new_uuri("dst", 2, 1, 0)); "Listen to Response")]
#[test_case(&test_lib::new_uuri("src", 1, 1, 0x0001), Some(&test_lib::new_uuri("dst", 2, 1, 0)); "Listen to specific Response")]
#[test_case(&test_lib::new_uuri("*", 0xFFFF, 0xFF, 0xFFFF), None; "Listen to all Publish")]
#[tokio::test(flavor = "multi_thread")]
async fn test_register_and_unregister(source_filter: &UUri, sink_filter: Option<&UUri>) {
    test_lib::before_test();

    // Initialization
    let upclient = test_lib::create_up_client_zenoh("myvehicle").await.unwrap();
    let foo_listener = Arc::new(FooListener);

    // Register the listener
    upclient
        .register_listener(source_filter, sink_filter, foo_listener.clone())
        .await
        .unwrap();

    // Able to ungister
    upclient
        .unregister_listener(source_filter, sink_filter, foo_listener.clone())
        .await
        .unwrap();

    // Unable to ungister
    let result = upclient
        .unregister_listener(source_filter, sink_filter, foo_listener.clone())
        .await;
    assert!(result.is_err());
}
