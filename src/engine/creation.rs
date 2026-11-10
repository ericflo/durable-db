use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRequest {
    pub request_id: String,
    pub limits: Limits,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateResult {
    pub info: Info,
    pub replayed: bool,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreationReceipt {
    id: String,
    digest: String,
    initial: Info,
}
pub(crate) fn digest(request: &CreateRequest) -> anyhow::Result<String> {
    let mut digest = Sha256::new();
    digest.update(b"durable-db.creation.v1\0");
    digest.update(serde_json::to_vec(request).context("encode creation intent")?);
    Ok(hex::encode(digest.finalize()))
}
impl CreationReceipt {
    pub(crate) fn limits(&self) -> &Limits {
        &self.initial.limits
    }
    pub(crate) fn validate(&self, policy: &access::Policy) -> anyhow::Result<()> {
        ensure!(
            !self.id.is_empty() && self.id.len() <= 128 && valid_digest(&self.digest),
            "invalid creation identity"
        );
        validate_limits(&self.initial.limits).context("invalid initial limits")?;
        ensure!(
            self.initial.revision == 0 && self.initial.bytes == 0 && self.initial.keys == 0,
            "invalid initial database info"
        );
        let original = self
            .initial
            .policy
            .as_ref()
            .context("missing creation policy")?;
        let current = policy.info();
        ensure!(
            original.incarnation == current.incarnation
                && original.generation == 0
                && original.revision == 1
                && original.revoked_tokens == 0,
            "invalid creation policy identity"
        );
        ensure!(
            self.digest
                == digest(&CreateRequest {
                    request_id: self.id.clone(),
                    limits: self.initial.limits.clone()
                })?,
            "invalid creation intent digest"
        );
        Ok(())
    }
}
impl Engine {
    /// Create an initially protected database. Exact retries recover the initial
    /// result for the lifetime of this database, independently of later writes.
    pub async fn create_protected(
        &self,
        db: &str,
        request: &CreateRequest,
    ) -> Result<CreateResult> {
        validate_db(db)?;
        request.validate()?;
        match self.manifest(db).await {
            Ok((existing, _)) => return recover(&existing, request),
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
        }
        let mut manifest = Manifest::empty(request.limits.clone(), Some(access::Policy::created()));
        let initial = manifest.info();
        manifest.creation = Some(CreationReceipt::created(
            request,
            manifest.policy.as_ref().unwrap(),
        )?);
        manifest.validate(db)?;
        if !self
            .storage
            .cas(&head(db), None, encode_head(&manifest)?)
            .await?
        {
            // A concurrent identical creation may have won. A fresh exact
            // receipt is the only witness that permits returning success.
            return recover(&self.manifest(db).await?.0, request);
        }
        Ok(CreateResult {
            info: initial,
            replayed: false,
        })
    }
}
fn recover(manifest: &Manifest, request: &CreateRequest) -> Result<CreateResult> {
    manifest
        .creation
        .as_ref()
        .ok_or(Error::Conflict)?
        .recover(request)
}
impl CreateRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_limits(&self.limits)?;
        if self.request_id.is_empty() || self.request_id.len() > 128 {
            return Err(Error::Invalid(
                "creation request_id must contain 1..128 bytes".into(),
            ));
        }
        Ok(())
    }
}
impl CreationReceipt {
    pub(crate) fn created(request: &CreateRequest, policy: &access::Policy) -> Result<Self> {
        request.validate()?;
        Ok(Self {
            id: request.request_id.clone(),
            digest: digest(request)?,
            initial: Info {
                revision: 0,
                bytes: 0,
                keys: 0,
                limits: request.limits.clone(),
                policy: Some(policy.info()),
            },
        })
    }
    pub(crate) fn recover(&self, request: &CreateRequest) -> Result<CreateResult> {
        if self.id != request.request_id {
            return Err(Error::Conflict);
        }
        if self.digest != digest(request)? {
            return Err(Error::Invalid(
                "request_id was used for a different creation request".into(),
            ));
        }
        Ok(CreateResult {
            info: self.initial.clone(),
            replayed: true,
        })
    }
}
