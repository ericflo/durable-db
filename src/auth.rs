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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<TokenPolicy>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenPolicy {
    pub token_id: String,
    pub incarnation: String,
    pub generation: u64,
}
/// Trusted embedded boundary. HTTP callers obtain this only after MAC and scope
/// verification; constructing one directly requires the embedded engine's trust.
pub enum Access {
    Administrator,
    Database(Claims),
}
impl Access {
    pub(crate) fn check_scope(&self, database: &str, write: bool) -> Result<()> {
        match self {
            Self::Administrator => Ok(()),
            Self::Database(claims) => claims.check_scope(database, write),
        }
    }
}
impl Claims {
    pub(crate) fn check_scope(&self, database: &str, write: bool) -> Result<()> {
        if self.expires <= now() || self.database != database || (write && !self.write) {
            return Err(Error::Unauthorized);
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpiringClaims {
    expires: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyringFile {
    format: u8,
    active: String,
    verify: Vec<String>,
    allow_legacy_admin: bool,
}
pub struct Auth {
    key: Vec<u8>,
    verification_keys: Vec<Vec<u8>>,
    allow_legacy_admin: bool,
}
impl Auth {
    /// Read a legacy hex secret or a strict, bounded version-1 JSON keyring.
    pub fn new(contents: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(contents.len() <= 16384, "signing-key file exceeds 16 KiB");
        let contents = contents.trim();
        if contents.starts_with('{') {
            // Serde's detailed type errors can quote secret values. Keep this
            // configuration boundary's diagnostics independent of its contents.
            let file: KeyringFile = serde_json::from_str(contents)
                .map_err(|_| anyhow::anyhow!("invalid signing-key file"))?;
            anyhow::ensure!(file.format == 1, "unsupported signing-key file format");
            anyhow::ensure!(
                file.verify.len() <= 3,
                "at most four signing keys are allowed"
            );
            let key = Self::decode_key(&file.active)?;
            let mut verification_keys = Vec::new();
            for encoded in file.verify {
                let previous = Self::decode_key(&encoded)?;
                anyhow::ensure!(
                    previous != key && !verification_keys.contains(&previous),
                    "duplicate signing key"
                );
                verification_keys.push(previous);
            }
            Ok(Self {
                key,
                verification_keys,
                allow_legacy_admin: file.allow_legacy_admin,
            })
        } else {
            Ok(Self {
                key: Self::decode_key(contents)?,
                verification_keys: Vec::new(),
                allow_legacy_admin: true,
            })
        }
    }
    fn decode_key(encoded: &str) -> anyhow::Result<Vec<u8>> {
        let key =
            hex::decode(encoded.trim()).map_err(|_| anyhow::anyhow!("invalid hex signing key"))?;
        anyhow::ensure!(
            key.len() == 32,
            "admin key must contain exactly 32 random bytes encoded as hex"
        );
        Ok(key)
    }
    /// Returns secret configuration bytes, never a value suitable for logging.
    pub fn keyring_document(
        active: &str,
        verify: Vec<String>,
        allow_legacy_admin: bool,
    ) -> anyhow::Result<String> {
        // Require raw hex inputs, so nested keyrings cannot silently change
        // which verification keys an operator intended to include.
        Self::decode_key(active)?;
        for key in &verify {
            Self::decode_key(key)?;
        }
        let document = serde_json::to_string_pretty(&KeyringFile {
            format: 1,
            active: active.trim().into(),
            verify: verify.into_iter().map(|k| k.trim().to_owned()).collect(),
            allow_legacy_admin,
        })?;
        Self::new(&document)?;
        Ok(document + "\n")
    }
    /// Legacy compatibility only: this bearer value exposes the signing key.
    /// Prefer administrator_token(), whose signed payload contains only expiry.
    pub fn admin_token(&self) -> String {
        format!("ddb_admin_{}", URL_SAFE_NO_PAD.encode(&self.key))
    }
    pub fn administrator_token(&self, ttl: u64) -> Result<String> {
        self.expiring_token("ddb_control_", b"durable-db.admin.v1\0", ttl)
    }
    /// A separate MAC domain prevents a monitoring capability from being
    /// interpreted as a database token, even if its prefix is removed.
    pub fn metrics_token(&self, ttl: u64) -> Result<String> {
        self.expiring_token("ddb_metrics_", b"durable-db.metrics.v1\0", ttl)
    }
    fn expiring_token(&self, prefix: &str, domain: &[u8], ttl: u64) -> Result<String> {
        if ttl == 0 || ttl > 86400 * 30 {
            return Err(Error::Invalid(
                "token TTL must be 1 second to 30 days".into(),
            ));
        }
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&ExpiringClaims {
                expires: now().saturating_add(ttl),
            })
            .map_err(anyhow::Error::from)?,
        );
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC key");
        mac.update(domain);
        mac.update(payload.as_bytes());
        Ok(format!(
            "{prefix}{payload}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        ))
    }
    pub fn is_metrics(&self, token: &str) -> bool {
        self.verify_expiring(token, "ddb_metrics_", b"durable-db.metrics.v1\0")
    }
    fn verify_expiring(&self, token: &str, prefix: &str, domain: &[u8]) -> bool {
        if token.len() > 2048 {
            return false;
        }
        let Some((payload, signature)) = token.strip_prefix(prefix).and_then(|t| t.split_once('.'))
        else {
            return false;
        };
        let Ok(signature) = URL_SAFE_NO_PAD.decode(signature) else {
            return false;
        };
        if !self.verify_mac(domain, payload.as_bytes(), &signature) {
            return false;
        }
        let Ok(payload) = URL_SAFE_NO_PAD.decode(payload) else {
            return false;
        };
        serde_json::from_slice::<ExpiringClaims>(&payload)
            .is_ok_and(|claims| claims.expires > now())
    }
    pub fn is_admin(&self, token: &str) -> bool {
        if token.len() > 2048 {
            return false;
        }
        if token.starts_with("ddb_control_") {
            return self.verify_expiring(token, "ddb_control_", b"durable-db.admin.v1\0");
        }
        if !self.allow_legacy_admin || !token.starts_with("ddb_admin_") {
            return false;
        }
        let mut accepted = false;
        for key in std::iter::once(&self.key).chain(&self.verification_keys) {
            // Constant-time MAC comparison; do not compare bearer secrets.
            let expected_token = format!("ddb_admin_{}", URL_SAFE_NO_PAD.encode(key));
            let mut expected = HmacSha256::new_from_slice(key).expect("HMAC key");
            expected.update(expected_token.as_bytes());
            let mut actual = HmacSha256::new_from_slice(key).expect("HMAC key");
            actual.update(token.as_bytes());
            accepted |= actual
                .verify_slice(&expected.finalize().into_bytes())
                .is_ok();
        }
        accepted
    }
    fn verify_mac(&self, domain: &[u8], payload: &[u8], signature: &[u8]) -> bool {
        let mut accepted = false;
        for key in std::iter::once(&self.key).chain(&self.verification_keys) {
            let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key");
            mac.update(domain);
            mac.update(payload);
            accepted |= mac.verify_slice(signature).is_ok();
        }
        accepted
    }
    pub fn issue(&self, database: String, write: bool, ttl: u64) -> Result<String> {
        self.issue_claims(database, write, ttl, None)
    }
    pub fn issue_for_policy(
        &self,
        database: String,
        write: bool,
        ttl: u64,
        incarnation: String,
        generation: u64,
    ) -> Result<(String, String)> {
        if !canonical_uuid(&incarnation) {
            return Err(Error::Invalid("invalid policy incarnation".into()));
        }
        let token_id = uuid::Uuid::new_v4().to_string();
        let token = self.issue_claims(
            database,
            write,
            ttl,
            Some(TokenPolicy {
                token_id: token_id.clone(),
                incarnation,
                generation,
            }),
        )?;
        Ok((token, token_id))
    }
    fn issue_claims(
        &self,
        database: String,
        write: bool,
        ttl: u64,
        policy: Option<TokenPolicy>,
    ) -> Result<String> {
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
            policy,
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
        if !self.verify_mac(b"", payload.as_bytes(), &signature) {
            return Err(Error::Unauthorized);
        }
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
        if claims
            .policy
            .as_ref()
            .is_some_and(|p| !canonical_uuid(&p.token_id) || !canonical_uuid(&p.incarnation))
        {
            return Err(Error::Unauthorized);
        }
        Ok(claims)
    }
    pub fn access(&self, token: &str, db: &str, write: bool) -> Result<Access> {
        if self.is_admin(token) {
            return Ok(Access::Administrator);
        }
        let claims = self.claims(token)?;
        claims.check_scope(db, write)?;
        Ok(Access::Database(claims))
    }
    /// Signature/scope check only. Public data access must also enforce the
    /// manifest policy through Engine::*_with_access.
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
pub(crate) fn canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value && !id.is_nil())
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
    fn bounded_keyrings_verify_overlap_but_issue_only_with_active_key() {
        let a = "11".repeat(32);
        let b = "22".repeat(32);
        let old = Auth::new(&a).unwrap();
        let new = Auth::new(&b).unwrap();
        let expanded =
            Auth::new(&Auth::keyring_document(&a, vec![b.clone()], false).unwrap()).unwrap();
        let promoted =
            Auth::new(&Auth::keyring_document(&b, vec![a.clone()], false).unwrap()).unwrap();
        let retired = Auth::new(&Auth::keyring_document(&b, vec![], false).unwrap()).unwrap();
        for issuer in [&old, &new] {
            let tenant = issuer.issue("test".into(), true, 3600).unwrap();
            let admin = issuer.administrator_token(3600).unwrap();
            let metrics = issuer.metrics_token(3600).unwrap();
            for verifier in [&expanded, &promoted] {
                assert!(verifier.claims(&tenant).is_ok());
                assert!(verifier.is_admin(&admin));
                assert!(verifier.is_metrics(&metrics));
                assert!(!verifier.is_admin(&issuer.admin_token()));
                assert!(!verifier.is_admin(&metrics));
                assert!(!verifier.is_metrics(&admin));
                assert!(verifier.claims(&admin).is_err());
                assert!(verifier.claims(&metrics).is_err());
                assert!(!verifier.is_admin(&metrics.replacen("ddb_metrics_", "ddb_control_", 1)));
                assert!(!verifier.is_metrics(&admin.replacen("ddb_control_", "ddb_metrics_", 1)));
            }
        }
        for (issuer, expected, rejected) in [(&expanded, &old, &new), (&promoted, &new, &old)] {
            let token = issuer.issue("test".into(), false, 3600).unwrap();
            assert!(expected.claims(&token).is_ok());
            assert!(rejected.claims(&token).is_err());
        }
        assert!(
            retired
                .claims(&old.issue("test".into(), true, 3600).unwrap())
                .is_err()
        );
        assert!(!retired.is_admin(&old.administrator_token(3600).unwrap()));
        assert!(!retired.is_metrics(&old.metrics_token(3600).unwrap()));
        let compatibility = Auth::new(&Auth::keyring_document(&b, vec![a], true).unwrap()).unwrap();
        assert!(compatibility.is_admin(&old.admin_token()));
        assert!(compatibility.is_admin(&new.admin_token()));
    }

    #[test]
    fn administrator_capability_contains_only_expiry_and_rejects_expired_or_forged_tokens() {
        let auth = Auth::new(&"39".repeat(32)).unwrap();
        let token = auth.administrator_token(3600).unwrap();
        let payload = token
            .strip_prefix("ddb_control_")
            .unwrap()
            .split_once('.')
            .unwrap()
            .0;
        let claims: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
        assert_eq!(claims.as_object().unwrap().len(), 1);
        assert!(claims["expires"].as_u64().unwrap() > now());
        for (prefix, domain) in [
            ("ddb_control_", b"durable-db.admin.v1\0".as_slice()),
            ("ddb_metrics_", b"durable-db.metrics.v1\0".as_slice()),
        ] {
            let payload = URL_SAFE_NO_PAD.encode(br#"{"expires":1}"#);
            let mut mac = HmacSha256::new_from_slice(&auth.key).unwrap();
            mac.update(domain);
            mac.update(payload.as_bytes());
            let expired = format!(
                "{prefix}{payload}.{}",
                URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
            );
            assert!(!auth.is_admin(&expired));
            assert!(!auth.is_metrics(&expired));
        }
        assert!(auth.administrator_token(0).is_err());
        assert!(auth.administrator_token(86400 * 30 + 1).is_err());
        assert!(!Auth::new(&"38".repeat(32)).unwrap().is_admin(&token));
        assert!(!auth.is_admin(&(token + "x")));
    }

    #[test]
    fn malformed_keyrings_fail_closed_without_exposing_secret_values() {
        let a = "44".repeat(32);
        assert!(Auth::keyring_document(&a, vec![a.clone()], false).is_err());
        assert!(Auth::keyring_document(&a, vec!["55".repeat(32); 2], false).is_err());
        assert!(
            Auth::keyring_document(
                &a,
                (5..9).map(|i| format!("{i:02}").repeat(32)).collect(),
                false
            )
            .is_err()
        );
        assert!(Auth::new(&"a".repeat(16385)).is_err());
        for value in [
            serde_json::json!({"format": 2, "active": a, "verify": [], "allow_legacy_admin": false}),
            serde_json::json!({"format": 1, "active": a, "verify": []}),
            serde_json::json!({"format": 1, "active": a, "verify": [], "allow_legacy_admin": false, "unknown": true}),
            serde_json::json!({"format": 1, "active": a, "verify": a, "allow_legacy_admin": false}),
        ] {
            let error = Auth::new(&value.to_string()).err().unwrap().to_string();
            assert!(!error.contains(&a));
        }
    }

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
