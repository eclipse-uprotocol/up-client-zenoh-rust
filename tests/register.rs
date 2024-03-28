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

use up_client_zenoh::UPClientZenoh;
use up_rust::{UCode, UStatus, UTransport};
use zenoh::config::Config;

#[async_std::test]
async fn test_utransport_register_and_unregister() {
    test_lib::before_test();

    // Initialization
    let upclient = UPClientZenoh::new(Config::default()).await.unwrap();
    let uuri = test_lib::create_utransport_uuri(0);

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
    test_lib::before_test();

    // Initialization
    let upclient = UPClientZenoh::new(Config::default()).await.unwrap();
    let uuri = test_lib::create_rpcserver_uuri();

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
            "RPC request listener doesn't exist"
        ))
    );
}

#[async_std::test]
async fn test_utransport_special_uuri_register_and_unregister() {
    test_lib::before_test();

    // Initialization
    let upclient = UPClientZenoh::new(Config::default()).await.unwrap();
    let uuri = test_lib::create_special_uuri();

    // Compare the return string
    let listener_string = upclient
        .register_listener(uuri.clone(), Box::new(|_| {}))
        .await
        .unwrap();
    assert_eq!(
        listener_string,
        "upr/060102030a0b0c/**&upr/060102030a0b0c/**_0&upr/060102030a0b0c/**_1"
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
