use super::*;
use crate::auth::{Access, canonical_uuid};

const MAX_REVOKED: usize = 128;
const MAX_RECEIPTS: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PolicyInfo {
    pub incarnation: String,
    pub generation: u64,
    pub revision: u64,
    pub revoked_tokens: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "action", rename_all = "snake_case")]
pub enum PolicyAction {
    Enable,
    Revoke { token_id: String },
    Rotate,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyRequest {
    pub request_id: String,
    pub expected_policy_revision: u64,
    pub change: PolicyAction,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyResult {
    pub policy: PolicyInfo,
    pub replayed: bool,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyReceipt {
    id: String,
    digest: String,
    result: PolicyInfo,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    incarnation: String,
    generation: u64,
    revision: u64,
    revoked: BTreeSet<String>,
    receipts: Vec<PolicyReceipt>,
}
impl Policy {
    pub(crate) fn created() -> Self {
        let mut policy = Self::new();
        policy.revision = 1;
        policy
    }
    fn new() -> Self {
        Self {
            incarnation: Uuid::new_v4().to_string(),
            generation: 0,
            revision: 0,
            revoked: BTreeSet::new(),
            receipts: Vec::new(),
        }
    }
    pub(crate) fn info(&self) -> PolicyInfo {
        PolicyInfo {
            incarnation: self.incarnation.clone(),
            generation: self.generation,
            revision: self.revision,
            revoked_tokens: self.revoked.len(),
        }
    }
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            canonical_uuid(&self.incarnation)
                && self.revision > 0
                && self.generation < self.revision
                && self.revoked.len() <= MAX_REVOKED
                && self.revoked.iter().all(|id| canonical_uuid(id))
                && self.receipts.len() <= MAX_RECEIPTS,
            "invalid stored access policy"
        );
        let mut ids = BTreeSet::new();
        let mut previous = 0;
        for receipt in &self.receipts {
            ensure!(
                !receipt.id.is_empty()
                    && receipt.id.len() <= 128
                    && ids.insert(&receipt.id)
                    && valid_digest(&receipt.digest)
                    && receipt.result.incarnation == self.incarnation
                    && receipt.result.revision > previous
                    && receipt.result.revision <= self.revision
                    && receipt.result.generation <= self.generation
                    && receipt.result.generation < receipt.result.revision
                    && receipt.result.revoked_tokens <= MAX_REVOKED,
                "invalid stored policy receipt"
            );
            previous = receipt.result.revision;
        }
        if let Some(receipt) = self.receipts.last() {
            ensure!(
                receipt.result == self.info(),
                "policy receipt does not match state"
            );
        } else {
            ensure!(
                self.revision == 1 && self.generation == 0 && self.revoked.is_empty(),
                "invalid initial policy"
            );
        }
        Ok(())
    }
}
pub(crate) fn authorize(
    policy: Option<&Policy>,
    db: &str,
    access: &Access,
    write: bool,
) -> Result<()> {
    let Access::Database(claims) = access else {
        return Ok(());
    };
    claims.check_scope(db, write)?;
    match (policy, &claims.policy) {
        (None, None) => Ok(()),
        (Some(policy), Some(token))
            if token.incarnation == policy.incarnation
                && token.generation == policy.generation
                && !policy.revoked.contains(&token.token_id)
                && canonical_uuid(&token.token_id) =>
        {
            Ok(())
        }
        _ => Err(Error::Unauthorized),
    }
}
impl Manifest {
    pub(crate) fn authorize(&self, db: &str, access: &Access, write: bool) -> Result<()> {
        authorize(self.policy.as_ref(), db, access, write)
    }
}
impl Engine {
    /// Administrative operation. HTTP routing must require the administrator.
    /// Exact retries recover the original result within the 32-receipt window.
    pub async fn change_policy(&self, db: &str, request: &PolicyRequest) -> Result<PolicyResult> {
        let digest = request.digest()?;
        let (mut manifest, version) = self.manifest(db).await?;
        let result = manifest
            .policy
            .get_or_insert_with(Policy::new)
            .change(request, &digest)?;
        if result.replayed {
            return Ok(result);
        }
        manifest.format = 2;
        manifest.fence = Uuid::new_v4().to_string();
        // The data revision is deliberately unchanged. The exact manifest
        // identity fences every write prepared before this policy change.
        manifest.validate(db)?;
        if !self
            .storage
            .cas(&head(db), Some(&version), encode_head(&manifest)?)
            .await?
        {
            return Err(Error::Conflict);
        }
        Ok(result)
    }
}

impl PolicyRequest {
    pub(crate) fn digest(&self) -> Result<String> {
        if self.request_id.is_empty() || self.request_id.len() > 128 {
            return Err(Error::Invalid(
                "policy request_id must contain 1..128 bytes".into(),
            ));
        }
        if let PolicyAction::Revoke { token_id } = &self.change
            && !canonical_uuid(token_id)
        {
            return Err(Error::Invalid("token_id must be a canonical UUID".into()));
        }
        Ok(hex::encode(Sha256::digest(
            serde_json::to_vec(self).context("encode policy request")?,
        )))
    }
}
impl Policy {
    pub(crate) fn change(&mut self, request: &PolicyRequest, digest: &str) -> Result<PolicyResult> {
        if let Some(receipt) = self.receipts.iter().find(|r| r.id == request.request_id) {
            if receipt.digest != digest {
                return Err(Error::Invalid(
                    "request_id was used for a different policy request".into(),
                ));
            }
            return Ok(PolicyResult {
                policy: receipt.result.clone(),
                replayed: true,
            });
        }
        if self.revision != request.expected_policy_revision {
            return Err(Error::Conflict);
        }
        if self.revision == 0 && !matches!(request.change, PolicyAction::Enable) {
            return Err(Error::Invalid(
                "enable access policy before revoking or rotating tokens".into(),
            ));
        }
        match &request.change {
            PolicyAction::Enable => {} // Never reset an existing policy.
            PolicyAction::Revoke { token_id } => {
                if !self.revoked.contains(token_id) && self.revoked.len() >= MAX_REVOKED {
                    return Err(Error::Limit(
                        "revocation set is full; rotate the credential generation".into(),
                    ));
                }
                self.revoked.insert(token_id.clone());
            }
            PolicyAction::Rotate => {
                self.generation = self
                    .generation
                    .checked_add(1)
                    .context("credential generation exhausted")?;
                self.revoked.clear();
            }
        }
        self.revision = self
            .revision
            .checked_add(1)
            .context("policy revision exhausted")?;
        let result = self.info();
        if self.receipts.len() >= MAX_RECEIPTS {
            self.receipts.remove(0);
        }
        self.receipts.push(PolicyReceipt {
            id: request.request_id.clone(),
            digest: digest.into(),
            result: result.clone(),
        });
        Ok(PolicyResult {
            policy: result,
            replayed: false,
        })
    }
}
