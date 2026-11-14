//! Bearer tokens: a signed statement of which database a caller may use.
//!
//! `ddb_<payload>.<signature>` where the payload is base64url JSON and the
//! signature is HMAC-SHA256 under the deployment's signing key. Verification
//! needs no storage. A database revokes every token minted for it by raising
//! its `token_generation`, which is checked once the database is loaded.
use crate::{Error, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Access {
    Read,
    Write,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    /// A database name, or `*` for an administrator.
    pub db: String,
    pub access: Access,
    /// Unix seconds after which the token is refused; 0 never expires.
    #[serde(default)]
    pub exp: u64,
    /// The database's token generation when this was minted.
    #[serde(default)]
    pub generation: u32,
}

impl Grant {
    pub fn admin() -> Self {
        Self {
            db: "*".into(),
            access: Access::Admin,
            exp: 0,
            generation: 0,
        }
    }

    pub fn is_admin(&self) -> bool {
        self.db == "*" && self.access == Access::Admin
    }

    /// Whether this grant covers `needed` access to `db`. The database's
    /// current token generation is checked separately, once it is known.
    pub fn allows(&self, db: &str, needed: Access) -> bool {
        self.is_admin() || (self.db == db && self.access != Access::Admin && self.access >= needed)
    }
}

/// The signing key, followed by older keys that are still accepted.
#[derive(Clone)]
pub struct Keys(Vec<Vec<u8>>);

/// What a relay proof signs. No token payload can equal it: those are
/// base64url, which has no space.
const RELAY: &str = "durable-db relay";

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl Keys {
    pub fn from_secret(secret: &[u8]) -> Self {
        Self(vec![secret.to_vec()])
    }

    /// One hex key per line; the first signs, all verify.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let keys = text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| {
                let key = hex::decode(line).map_err(|_| anyhow::anyhow!("signing keys are hex"))?;
                anyhow::ensure!(key.len() >= 32, "a signing key is at least 32 bytes");
                Ok(key)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(!keys.is_empty(), "the key file holds no signing key");
        Ok(Self(keys))
    }

    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read signing key {}: {e}", path.display()))?;
        Self::parse(&text)
    }

    /// Load the key file, creating it with a fresh key readable only by its owner.
    pub fn load_or_create(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            use std::io::Write;
            let key: [u8; 32] = rand::random();
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = options.open(path)?;
            writeln!(file, "{}", hex::encode(key))?;
        }
        Self::load(path)
    }

    fn sign(key: &[u8], payload: &str) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(payload.as_bytes());
        mac.finalize().into_bytes().to_vec()
    }

    /// What a node sends with a request it relays to a peer. Only a holder
    /// of the signing key can produce it, so a client cannot claim to be one.
    pub fn relay_proof(&self) -> String {
        hex::encode(Self::sign(&self.0[0], RELAY))
    }

    /// Whether `proof` was made by a node that holds one of the keys.
    pub fn relayed(&self, proof: &[u8]) -> bool {
        let Ok(proof) = hex::decode(proof) else {
            return false;
        };
        self.0.iter().any(|key| {
            let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
            mac.update(RELAY.as_bytes());
            mac.verify_slice(&proof).is_ok()
        })
    }

    pub fn mint(&self, grant: &Grant) -> String {
        let payload = B64.encode(serde_json::to_vec(grant).expect("grant encodes"));
        let signature = B64.encode(Self::sign(&self.0[0], &payload));
        format!("ddb_{payload}.{signature}")
    }

    pub fn verify(&self, token: &str) -> Result<Grant> {
        let (payload, signature) = token
            .strip_prefix("ddb_")
            .and_then(|rest| rest.split_once('.'))
            .ok_or(Error::Unauthorized)?;
        let signature = B64.decode(signature).map_err(|_| Error::Unauthorized)?;
        let genuine = self.0.iter().any(|key| {
            let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
            mac.update(payload.as_bytes());
            mac.verify_slice(&signature).is_ok()
        });
        if !genuine {
            return Err(Error::Unauthorized);
        }
        let grant: Grant = B64
            .decode(payload)
            .ok()
            .and_then(|json| serde_json::from_slice(&json).ok())
            .ok_or(Error::Unauthorized)?;
        if grant.exp != 0 && grant.exp <= now() {
            return Err(Error::Unauthorized);
        }
        Ok(grant)
    }
}

pub fn expiry(ttl_seconds: u64) -> u64 {
    if ttl_seconds == 0 {
        0
    } else {
        now().saturating_add(ttl_seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_verify_only_under_their_key_and_scope() {
        let keys = Keys::from_secret(&[7; 32]);
        let grant = Grant {
            db: "app".into(),
            access: Access::Write,
            exp: 0,
            generation: 3,
        };
        let token = keys.mint(&grant);
        assert_eq!(keys.verify(&token).unwrap(), grant);
        assert!(grant.allows("app", Access::Read) && grant.allows("app", Access::Write));
        assert!(!grant.allows("app", Access::Admin) && !grant.allows("other", Access::Read));
        assert!(Grant::admin().allows("anything", Access::Admin));

        assert!(Keys::from_secret(&[8; 32]).verify(&token).is_err());
        let forged = token.replace("ddb_", "ddb_e");
        assert!(keys.verify(&forged).is_err());
        let expired = keys.mint(&Grant {
            exp: 1,
            ..grant.clone()
        });
        assert!(keys.verify(&expired).is_err());
        // A database-scoped token cannot claim administrator access.
        let sneaky = Grant {
            db: "app".into(),
            access: Access::Admin,
            exp: 0,
            generation: 0,
        };
        assert!(!sneaky.allows("app", Access::Read));
    }

    #[test]
    fn only_a_key_holder_can_say_it_relayed() {
        let keys = Keys::from_secret(&[7; 32]);
        let proof = keys.relay_proof();
        assert!(keys.relayed(proof.as_bytes()));
        for forged in ["1", "", "true", &proof[2..], &hex::encode([0u8; 32])] {
            assert!(!keys.relayed(forged.as_bytes()), "{forged:?}");
        }
        assert!(!Keys::from_secret(&[8; 32]).relayed(proof.as_bytes()));
        // During a rotation, peers that sign with the older key are still peers.
        let rotated = Keys::parse(&format!(
            "{}\n{}\n",
            hex::encode([9; 32]),
            hex::encode([7; 32])
        ))
        .unwrap();
        assert!(rotated.relayed(proof.as_bytes()));
        // A token's signature is not a proof, nor the other way around.
        let token = keys.mint(&Grant::admin());
        let signature = token.rsplit('.').next().unwrap();
        assert!(!keys.relayed(signature.as_bytes()));
        assert!(keys.verify(&format!("ddb_{RELAY}.{proof}")).is_err());
    }

    #[test]
    fn a_lifetime_beyond_the_calendar_does_not_wrap() {
        assert_eq!(expiry(0), 0);
        assert_eq!(expiry(u64::MAX), u64::MAX);
        assert!(expiry(60) > now());
    }

    #[test]
    fn older_keys_still_verify_after_rotation() {
        let old = Keys::from_secret(&[1; 32]);
        let token = old.mint(&Grant::admin());
        let rotated = Keys::parse(&format!(
            "{}\n{}\n",
            hex::encode([2; 32]),
            hex::encode([1; 32])
        ))
        .unwrap();
        assert!(rotated.verify(&token).is_ok());
        assert!(old.verify(&rotated.mint(&Grant::admin())).is_err());
    }
}
