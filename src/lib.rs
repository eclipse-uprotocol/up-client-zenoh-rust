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
pub mod rpc;
pub mod utransport;

use protobuf::{Enum, Message};
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, Mutex},
};
use up_rust::{
    UAttributes, UCode, UEntity, UMessage, UPayloadFormat, UPriority, UResourceBuilder, UStatus,
    UUri,
};
use zenoh::{
    config::Config,
    prelude::r#async::*,
    queryable::{Query, Queryable},
    sample::{Attachment, AttachmentBuilder},
    subscriber::Subscriber,
};

pub type UtransportListener = Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>;

const UATTRIBUTE_VERSION: u8 = 1;

pub struct ZenohListener {}
pub struct UPClientZenoh {
    session: Arc<Session>,
    // Able to unregister Subscriber
    subscriber_map: Arc<Mutex<HashMap<String, Subscriber<'static, ()>>>>,
    // Able to unregister Queryable
    queryable_map: Arc<Mutex<HashMap<String, Queryable<'static, ()>>>>,
    // Save the reqid to be able to send back response
    query_map: Arc<Mutex<HashMap<String, Query>>>,
    // Save the callback for RPC response
    rpc_callback_map: Arc<Mutex<HashMap<String, Arc<UtransportListener>>>>,
    // Used to identify different callback
    callback_counter: AtomicU64,
    // Source UUri in RPC
    uuri: UUri,
}

impl UPClientZenoh {
    /// Create `UPClientZenoh` by applying the Zenoh configuration.
    /// The `UUri` will be generated randomly.
    ///
    /// # Arguments
    ///
    /// * `config` - Zenoh configuration. You can refer to [here](https://github.com/eclipse-zenoh/zenoh/blob/0.10.1-rc/DEFAULT_CONFIG.json5) for more configuration details.
    ///
    /// # Errors
    /// Will return `Err` if unable to create `UPClientZenoh`
    ///
    /// # Examples
    ///
    /// ```
    /// # async_std::task::block_on(async {
    ///     use up_client_zenoh::UPClientZenoh;
    ///     use zenoh::config::Config;
    ///     let upclient = UPClientZenoh::new(Config::default()).await.unwrap();
    /// # });
    /// ```
    pub async fn new(config: Config) -> Result<UPClientZenoh, UStatus> {
        let uuri = UUri {
            entity: Some(UEntity {
                name: "default.entity".to_string(),
                id: Some(u32::from(rand::random::<u16>())),
                version_major: Some(1),
                version_minor: None,
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        UPClientZenoh::new_with_uuri(config, uuri).await
    }

    /// Create `UPClientZenoh` by applying the Zenoh configuration and self-defined `UUri`.
    ///
    /// # Arguments
    ///
    /// * `config` - Zenoh configuration. You can refer to [here](https://github.com/eclipse-zenoh/zenoh/blob/0.10.1-rc/DEFAULT_CONFIG.json5) for more configuration details.
    /// * `uuri` - The `UUri` which is put in source while sending RPC request.
    ///
    /// # Errors
    /// Will return `Err` if unable to create `UPClientZenoh`
    ///
    /// # Examples
    ///
    /// ```
    /// # async_std::task::block_on(async {
    ///     use up_client_zenoh::UPClientZenoh;
    ///     use up_rust::{UEntity, UUri};
    ///     use zenoh::config::Config;
    ///     let uuri = UUri {
    ///         entity: Some(UEntity {
    ///             name: "default.entity".to_string(),
    ///             id: Some(u32::from(rand::random::<u16>())),
    ///             version_major: Some(1),
    ///             version_minor: None,
    ///             ..Default::default()
    ///         })
    ///         .into(),
    ///         ..Default::default()
    ///     };
    ///     let upclient = UPClientZenoh::new_with_uuri(Config::default(), uuri).await.unwrap();
    /// # });
    /// ```
    pub async fn new_with_uuri(config: Config, uuri: UUri) -> Result<UPClientZenoh, UStatus> {
        let Ok(session) = zenoh::open(config).res().await else {
            let msg = "Unable to open Zenoh session".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        };
        Ok(UPClientZenoh {
            session: Arc::new(session),
            subscriber_map: Arc::new(Mutex::new(HashMap::new())),
            queryable_map: Arc::new(Mutex::new(HashMap::new())),
            query_map: Arc::new(Mutex::new(HashMap::new())),
            rpc_callback_map: Arc::new(Mutex::new(HashMap::new())),
            callback_counter: AtomicU64::new(0),
            uuri,
        })
    }

    /// Get the `UUri` of `UPClientZenoh` in for RPC response
    ///
    /// # Examples
    ///
    /// ```
    /// # async_std::task::block_on(async {
    ///     use up_client_zenoh::UPClientZenoh;
    ///     use up_rust::{UUri, UriValidator};
    ///     use zenoh::config::Config;
    ///     let upclient = UPClientZenoh::new(Config::default()).await.unwrap();
    ///     let uuri = upclient.get_response_uuri();
    ///     assert!(uuri.authority.is_none());
    ///     assert!(UriValidator::is_rpc_response(&uuri));
    ///     assert_eq!(uuri.entity.unwrap().name, "default.entity");
    /// # });
    /// ```
    pub fn get_response_uuri(&self) -> UUri {
        let mut source = self.uuri.clone();
        source.resource = Some(UResourceBuilder::for_rpc_response()).into();
        source
    }

    fn get_uauth_from_uuri(uri: &UUri) -> Result<String, UStatus> {
        if let Some(authority) = uri.authority.as_ref() {
            let buf: Vec<u8> = authority.try_into().map_err(|_| {
                let msg = "Unable to transform UAuthority into micro form".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?;
            Ok(buf
                .iter()
                .fold(String::new(), |s, c| s + &format!("{c:02x}")))
        } else {
            let msg = "UAuthority is empty".to_string();
            log::error!("{msg}");
            Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg))
        }
    }

    // The UURI format should be "upr/<UAuthority id or ip>/<the rest of remote UUri>" or "upl/<local UUri>"
    fn to_zenoh_key_string(uri: &UUri) -> Result<String, UStatus> {
        if uri.authority.is_some() && uri.entity.is_none() && uri.resource.is_none() {
            Ok(String::from("upr/") + &UPClientZenoh::get_uauth_from_uuri(uri)? + "/**")
        } else {
            let micro_uuri: Vec<u8> = uri.try_into().map_err(|e| {
                let msg = format!("Unable to serialize into micro format: {e}");
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?;
            // If the UUri is larger than 8 bytes, then it should be remote UUri with UAuthority
            // We should prepend it to the Zenoh key.
            let mut micro_zenoh_key = if micro_uuri.len() > 8 {
                String::from("upr/")
                    + &micro_uuri[8..]
                        .iter()
                        .fold(String::new(), |s, c| s + &format!("{c:02x}"))
                    + "/"
            } else {
                String::from("upl/")
            };
            // The rest part of UUri (UEntity + UResource)
            micro_zenoh_key += &micro_uuri[..8]
                .iter()
                .fold(String::new(), |s, c| s + &format!("{c:02x}"));
            Ok(micro_zenoh_key)
        }
    }

    #[allow(clippy::match_same_arms)]
    fn map_zenoh_priority(upriority: UPriority) -> Priority {
        match upriority {
            UPriority::UPRIORITY_CS0 => Priority::Background,
            UPriority::UPRIORITY_CS1 => Priority::DataLow,
            UPriority::UPRIORITY_CS2 => Priority::Data,
            UPriority::UPRIORITY_CS3 => Priority::DataHigh,
            UPriority::UPRIORITY_CS4 => Priority::InteractiveLow,
            UPriority::UPRIORITY_CS5 => Priority::InteractiveHigh,
            UPriority::UPRIORITY_CS6 => Priority::RealTime,
            // If uProtocol prioritiy isn't specified, use CS1(DataLow) by default.
            // https://github.com/eclipse-uprotocol/uprotocol-spec/blob/main/basics/qos.adoc
            UPriority::UPRIORITY_UNSPECIFIED => Priority::DataLow,
        }
    }

    fn to_upayload_format(encoding: &Encoding) -> Option<UPayloadFormat> {
        let Ok(value) = encoding.suffix().parse::<i32>() else {
            return None;
        };
        UPayloadFormat::from_i32(value)
    }

    fn uattributes_to_attachment(uattributes: &UAttributes) -> anyhow::Result<AttachmentBuilder> {
        let mut attachment = AttachmentBuilder::new();
        attachment.insert("", &UATTRIBUTE_VERSION.to_le_bytes());
        attachment.insert("", &uattributes.write_to_bytes()?);
        Ok(attachment)
    }

    fn attachment_to_uattributes(attachment: &Attachment) -> anyhow::Result<UAttributes> {
        let mut attachment_iter = attachment.iter();
        if let Some((_, value)) = attachment_iter.next() {
            let version = *value.as_slice().first().ok_or_else(|| {
                let msg = "UAttributes version is empty".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?;
            if version != 1 {
                let msg = "UAttributes version should be 1".to_string();
                log::error!("{msg}");
                return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
            }
        } else {
            let msg = "Unable to get the UAttributes version".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
        }
        let uattributes = if let Some((_, value)) = attachment_iter.next() {
            UAttributes::parse_from_bytes(value.as_slice())?
        } else {
            let msg = "Unable to get the UAttributes".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
        };
        Ok(uattributes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use up_rust::{Number, UAuthority, UEntity, UResource, UUri};

    #[test]
    fn test_to_zenoh_key_string() {
        // create uuri for test
        let uuri = UUri {
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
        };
        assert_eq!(
            UPClientZenoh::to_zenoh_key_string(&uuri).unwrap(),
            String::from("upl/0100162e04d20100")
        );
        // create special uuri for test
        let uuri = UUri {
            authority: Some(UAuthority {
                name: Some("UAuthName".to_string()),
                number: Some(Number::Id(vec![1, 2, 3, 10, 11, 12])),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        assert_eq!(
            UPClientZenoh::to_zenoh_key_string(&uuri).unwrap(),
            String::from("upr/060102030a0b0c/**")
        );
    }
}
