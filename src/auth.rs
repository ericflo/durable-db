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
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricsClaims {
    expires: u64,
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
    /// A separate MAC domain prevents a monitoring capability from being
    /// interpreted as a database token, even if its prefix is removed.
    pub fn metrics_token(&self, ttl: u64) -> Result<String> {
        if ttl == 0 || ttl > 86400 * 30 {
            return Err(Error::Invalid(
                "token TTL must be 1 second to 30 days".into(),
            ));
        }
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&MetricsClaims {
                expires: now().saturating_add(ttl),
            })
            .map_err(anyhow::Error::from)?,
        );
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC key");
        mac.update(b"durable-db.metrics.v1\0");
        mac.update(payload.as_bytes());
        Ok(format!(
            "ddb_metrics_{payload}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        ))
    }
    pub fn is_metrics(&self, token: &str) -> bool {
        if token.len() > 2048 {
            return false;
        }
        let Some((payload, signature)) = token
            .strip_prefix("ddb_metrics_")
            .and_then(|t| t.split_once('.'))
        else {
            return false;
        };
        let Ok(signature) = URL_SAFE_NO_PAD.decode(signature) else {
            return false;
        };
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC key");
        mac.update(b"durable-db.metrics.v1\0");
        mac.update(payload.as_bytes());
        if mac.verify_slice(&signature).is_err() {
            return false;
        }
        let Ok(payload) = URL_SAFE_NO_PAD.decode(payload) else {
            return false;
        };
        serde_json::from_slice::<MetricsClaims>(&payload).is_ok_and(|claims| claims.expires > now())
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn an_expired_but_correctly_signed_metrics_token_is_rejected() {
        let auth = Auth::new(&"19".repeat(32)).unwrap();
        let payload = URL_SAFE_NO_PAD.encode(br#"{"expires":1}"#);
        let mut mac = HmacSha256::new_from_slice(&auth.key).unwrap();
        mac.update(b"durable-db.metrics.v1\0");
        mac.update(payload.as_bytes());
        let token = format!(
            "ddb_metrics_{payload}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        );
        assert!(!auth.is_metrics(&token));
        let tenant = auth.issue("a".into(), true, 3600).unwrap();
        assert!(!auth.is_metrics(&format!("ddb_metrics_{tenant}")));
    }
}
