use crate::{Error, Result, engine::validate_db};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Claims {
    pub database: String,
    pub write: bool,
    pub expires: u64,
}
pub struct Auth {
    key: Vec<u8>,
}
impl Auth {
    pub fn new(hex_key: &str) -> anyhow::Result<Self> {
        let key = hex::decode(hex_key.trim())?;
        anyhow::ensure!(
            key.len() == 32,
            "admin key must contain exactly 32 random bytes encoded as hex"
        );
        Ok(Self { key })
    }
    pub fn admin_token(&self) -> String {
        format!("ddb_admin_{}", URL_SAFE_NO_PAD.encode(&self.key))
    }
    pub fn is_admin(&self, token: &str) -> bool {
        // Constant-time MAC comparison; do not compare secret strings directly.
        let mut expected = HmacSha256::new_from_slice(&self.key).expect("HMAC key");
        expected.update(self.admin_token().as_bytes());
        let mut actual = HmacSha256::new_from_slice(&self.key).expect("HMAC key");
        actual.update(token.as_bytes());
        actual
            .verify_slice(&expected.finalize().into_bytes())
            .is_ok()
    }
    pub fn issue(&self, database: String, write: bool, ttl: u64) -> Result<String> {
        validate_db(&database)?;
        if ttl == 0 || ttl > 86400 * 30 {
            return Err(Error::Invalid(
                "token TTL must be 1 second to 30 days".into(),
            ));
        }
        let claims = Claims {
            database,
            write,
            expires: now().saturating_add(ttl),
        };
        let payload =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).map_err(anyhow::Error::from)?);
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC key");
        mac.update(payload.as_bytes());
        Ok(format!(
            "{payload}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        ))
    }
    pub fn claims(&self, token: &str) -> Result<Claims> {
        if token.len() > 2048 {
            return Err(Error::Unauthorized);
        }
        let (payload, signature) = token.split_once('.').ok_or(Error::Unauthorized)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| Error::Unauthorized)?;
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC key");
        mac.update(payload.as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| Error::Unauthorized)?;
        let claims: Claims = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(payload)
                .map_err(|_| Error::Unauthorized)?,
        )
        .map_err(|_| Error::Unauthorized)?;
        if claims.expires <= now() {
            return Err(Error::Unauthorized);
        }
        validate_db(&claims.database).map_err(|_| Error::Unauthorized)?;
        Ok(claims)
    }
    pub fn authorize(&self, token: &str, db: &str, write: bool) -> Result<()> {
        if self.is_admin(token) {
            return Ok(());
        }
        let claims = self.claims(token)?;
        if claims.database != db || (write && !claims.write) {
            return Err(Error::Unauthorized);
        }
        Ok(())
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}
