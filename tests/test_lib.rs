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
use std::sync::Once;
use up_rust::{Number, UAuthority, UEntity, UResource, UResourceBuilder, UUri};

static INIT: Once = Once::new();

pub fn before_test() {
    INIT.call_once(env_logger::init);
}

#[allow(clippy::must_use_candidate)]
pub fn create_utransport_uuri(index: u8) -> UUri {
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

#[allow(clippy::must_use_candidate)]
pub fn create_rpcserver_uuri() -> UUri {
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

#[allow(clippy::must_use_candidate)]
pub fn create_authority() -> UAuthority {
    UAuthority {
        name: Some("UAuthName".to_string()),
        number: Some(Number::Id(vec![1, 2, 3, 10, 11, 12])),
        ..Default::default()
    }
}

#[allow(clippy::must_use_candidate)]
pub fn create_special_uuri() -> UUri {
    UUri {
        authority: Some(create_authority()).into(),
        ..Default::default()
    }
}
