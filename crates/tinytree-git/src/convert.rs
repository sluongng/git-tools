use crate::{
    git::Git,
    state::{Mapping, Resources, hex, valid_hex},
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io::Read,
    pin::Pin,
    sync::Arc,
};
use tinytree_client::{MetadataOutcome, NativeClient};
use tinytree_model as m;
use tokio::time::Instant;

pub(crate) struct Converter {
    pub client: NativeClient,
    pub git: Git,
    pub owner: Arc<Resources>,
    pub mappings: BTreeMap<String, Mapping>,
    objects: BTreeMap<(u8, [u8; 32]), String>,
    pub new_commits: usize,
    pub file_bytes: u64,
    io_bytes: u64,
    metadata_held: u64,
    sdk_bytes: u64,
    pub disk_bytes: u64,
    entries: usize,
    seen_objects: usize,
}
impl Converter {
    pub fn new(
        client: NativeClient,
        git: Git,
        mappings: BTreeMap<String, Mapping>,
        disk_bytes: u64,
        sdk_bytes: u64,
    ) -> Self {
        Self {
            owner: git.owner.clone(),
            client,
            git,
            mappings,
            objects: BTreeMap::new(),
            new_commits: 0,
            file_bytes: 0,
            io_bytes: 0,
            metadata_held: 0,
            sdk_bytes,
            disk_bytes,
            entries: 0,
            seen_objects: 0,
        }
    }
    fn check(&self) -> Result<()> {
        ensure!(
            Instant::now() < self.git.deadline,
            "native conversion deadline exceeded"
        );
        Ok(())
    }
    fn account(&mut self, bytes: u64) -> Result<()> {
        self.io_bytes = self
            .io_bytes
            .checked_add(bytes)
            .context("native export byte overflow")?;
        ensure!(
            self.io_bytes <= self.owner.options.limits.max_total_bytes,
            "native export total byte limit exceeded"
        );
        Ok(())
    }
    fn memory(&self, extra: u64) -> Result<()> {
        let l = &self.owner.options.limits;
        let base = (l.max_file_bytes as u64)
            .saturating_mul(2)
            .saturating_add((l.max_state_bytes as u64).saturating_mul(4))
            .saturating_add((l.max_objects as u64).saturating_mul(256))
            .saturating_add(self.sdk_bytes);
        ensure!(
            base.saturating_add(self.metadata_held)
                .saturating_add(extra)
                <= l.max_memory_bytes,
            "native export memory budget exceeded"
        );
        Ok(())
    }
    async fn metadata(&mut self, key: m::ObjectKey) -> Result<(m::Object, u64)> {
        self.check()?;
        self.seen_objects += 1;
        ensure!(
            self.seen_objects <= self.owner.options.limits.max_objects,
            "native export object work limit exceeded"
        );
        let mut replies = self
            .client
            .read_objects(vec![key])
            .await
            .map_err(|_| anyhow::anyhow!("checked native metadata read failed"))?;
        let MetadataOutcome::Object(value) =
            replies.pop().context("native object outcome absent")?
        else {
            anyhow::bail!("selected native metadata is unavailable");
        };
        let descriptor = value.descriptor();
        ensure!(
            descriptor.key == key
                && descriptor.size_bytes == value.canonical_bytes().len() as u64
                && descriptor.size_bytes <= self.owner.options.limits.max_metadata_bytes as u64,
            "native metadata descriptor exceeds limits"
        );
        self.account(descriptor.size_bytes)?;
        let charge = descriptor
            .size_bytes
            .checked_mul(8)
            .and_then(|n| n.checked_add(4096))
            .context("native metadata memory overflow")?;
        self.memory(charge)?;
        self.metadata_held += charge;
        let object = value.object().clone();
        drop(value);
        Ok((object, charge))
    }
    async fn write(&mut self, kind: &str, bytes: Vec<u8>) -> Result<crate::git::Written> {
        self.check()?;
        let bound = (bytes.len() as u64)
            .checked_add(bytes.len() as u64 / 100)
            .and_then(|n| n.checked_add(4096))
            .context("Git object disk bound overflow")?;
        self.disk_bytes = self
            .disk_bytes
            .checked_add(bound)
            .context("Git export disk overflow")?;
        ensure!(
            self.disk_bytes <= self.owner.options.limits.max_destination_bytes,
            "Git destination disk budget exceeded"
        );
        self.git.object(kind, bytes).await
    }
    pub async fn commits(&mut self, head: &str) -> Result<String> {
        ensure!(valid_hex(head, 64), "invalid selected native head");
        let mut stack = vec![(head.to_string(), false)];
        let mut visiting = BTreeSet::new();
        let mut pending = BTreeMap::<String, (m::Commit, u64)>::new();
        let mut verified = BTreeSet::new();
        while let Some((id, exit)) = stack.pop() {
            self.check()?;
            if let Some(mapping) = self.mappings.get(&id) {
                if verified.insert(id.clone()) {
                    self.git
                        .verify_object(
                            "commit",
                            &mapping.git,
                            mapping.payload_blake3,
                            mapping.payload_size,
                        )
                        .await?;
                }
                continue;
            }
            if exit {
                let (commit, charge) = pending
                    .remove(&id)
                    .context("native parent traversal state missing")?;
                let parents = commit
                    .parents
                    .iter()
                    .filter(|p| **p != m::CommitId::ROOT)
                    .map(|p| hex(&p.0))
                    .collect::<Vec<_>>();
                let parent_oids = parents
                    .iter()
                    .map(|p| {
                        self.mappings
                            .get(p)
                            .map(|m| m.git.clone())
                            .context("native public parent was not exported")
                    })
                    .collect::<Result<Vec<_>>>()?;
                ensure!(
                    parent_oids.iter().collect::<BTreeSet<_>>().len() == parent_oids.len(),
                    "distinct native parents collapse to duplicate Git parents"
                );
                let tree = self.tree(commit.root_tree.terms[0], 0).await?;
                let raw = commit_bytes(&commit, &tree, &parent_oids)?;
                let written = self.write("commit", raw).await?;
                let estimate = self
                    .mappings
                    .values()
                    .try_fold(4096usize, |n, m| {
                        n.checked_add(512 + m.parents.len() * 80)
                            .context("mapping byte overflow")
                    })?
                    .checked_add(512 + parents.len() * 80)
                    .context("mapping byte overflow")?;
                ensure!(
                    estimate <= self.owner.options.limits.max_state_bytes,
                    "adapter mapping byte limit exceeded"
                );
                self.mappings.insert(
                    id.clone(),
                    Mapping {
                        git: written.oid,
                        tree,
                        parents,
                        payload_blake3: written.digest,
                        payload_size: written.size,
                    },
                );
                self.metadata_held -= charge;
                visiting.remove(&id);
                self.new_commits += 1;
                ensure!(
                    self.mappings.len() <= self.owner.options.limits.max_commits,
                    "adapter commit mapping limit exceeded"
                );
            } else {
                ensure!(
                    visiting.insert(id.clone()),
                    "selected public parent graph contains a cycle"
                );
                ensure!(
                    pending.len() + self.mappings.len() < self.owner.options.limits.max_commits,
                    "native commit traversal limit exceeded"
                );
                let key = m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(decode::<32>(&id)?),
                };
                let (object, charge) = self.metadata(key).await?;
                let m::Object::Commit(commit) = object else {
                    anyhow::bail!("native public parent is not a commit");
                };
                ensure!(
                    commit.secure_signature.is_none(),
                    "native signatures cannot be silently converted to Git signatures"
                );
                ensure!(
                    commit.root_tree.terms.len() == 1
                        && commit.root_tree.labels.iter().all(String::is_empty),
                    "unresolved native commit trees cannot be exported"
                );
                ensure!(
                    commit.parents.len() <= 4096,
                    "native parent count exceeds export limit"
                );
                stack.push((id.clone(), true));
                // PARENTS ONLY: predecessors and copy provenance are deliberately
                // absent from selection, mappings and all produced Git objects.
                for parent in commit
                    .parents
                    .iter()
                    .rev()
                    .filter(|p| **p != m::CommitId::ROOT)
                {
                    stack.push((hex(&parent.0), false));
                }
                pending.insert(id, (commit, charge));
            }
        }
        Ok(self
            .mappings
            .get(head)
            .context("selected commit mapping missing")?
            .git
            .clone())
    }
    fn tree<'a>(
        &'a mut self,
        id: m::TreeId,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> {
        Box::pin(async move {
            self.check()?;
            ensure!(
                depth < self.owner.options.limits.max_depth,
                "native tree depth limit exceeded"
            );
            let key = (m::Kind::Tree as u8, id.0);
            if let Some(oid) = self.objects.get(&key) {
                return Ok(oid.clone());
            }
            let (object, charge) = self
                .metadata(m::ObjectKey {
                    kind: m::Kind::Tree,
                    id: m::ObjectId(id.0),
                })
                .await?;
            let m::Object::Tree(tree) = object else {
                anyhow::bail!("native tree kind mismatch");
            };
            self.entries = self
                .entries
                .checked_add(tree.entries.len())
                .context("native entry count overflow")?;
            ensure!(
                self.entries <= self.owner.options.limits.max_entries,
                "native tree entry work limit exceeded"
            );
            let mut entries = Vec::with_capacity(tree.entries.len());
            for entry in tree.entries {
                ensure!(
                    !entry.name.is_empty()
                        && !entry.name.contains(['\0', '/'])
                        && !entry.name.eq_ignore_ascii_case(".git"),
                    "native path cannot be represented safely in Git"
                );
                let (mode, oid, is_tree) = match entry.value {
                    m::TreeValue::Tree(id) => ("40000", self.tree(id, depth + 1).await?, true),
                    m::TreeValue::File { id, executable, .. } => (
                        if executable { "100755" } else { "100644" },
                        self.file(id).await?,
                        false,
                    ),
                    m::TreeValue::Symlink { id, .. } => {
                        let key = (m::Kind::Symlink as u8, id.0);
                        let oid = if let Some(oid) = self.objects.get(&key) {
                            oid.clone()
                        } else {
                            let (object, symlink_charge) = self
                                .metadata(m::ObjectKey {
                                    kind: m::Kind::Symlink,
                                    id: m::ObjectId(id.0),
                                })
                                .await?;
                            let m::Object::Symlink(target) = object else {
                                anyhow::bail!("native symlink kind mismatch");
                            };
                            ensure!(!target.contains('\0'), "native symlink target contains NUL");
                            let oid = self.write("blob", target.into_bytes()).await?.oid;
                            self.metadata_held -= symlink_charge;
                            self.objects.insert(key, oid.clone());
                            oid
                        };
                        ("120000", oid, false)
                    }
                };
                let mut order = entry.name.as_bytes().to_vec();
                order.push(if is_tree { b'/' } else { 0 });
                entries.push((order, entry.name, mode, oid));
            }
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut raw = Vec::new();
            for (_, name, mode, oid) in entries {
                raw.extend_from_slice(mode.as_bytes());
                raw.push(b' ');
                raw.extend_from_slice(name.as_bytes());
                raw.push(0);
                raw.extend_from_slice(&decode::<20>(&oid)?);
                ensure!(
                    raw.len() <= self.owner.options.limits.max_metadata_bytes,
                    "Git tree output byte limit exceeded"
                );
            }
            let oid = self.write("tree", raw).await?.oid;
            self.metadata_held -= charge;
            self.objects.insert(key, oid.clone());
            Ok(oid)
        })
    }
    async fn file(&mut self, id: m::FileId) -> Result<String> {
        let key = (m::Kind::File as u8, id.0);
        if let Some(oid) = self.objects.get(&key) {
            return Ok(oid.clone());
        }
        self.check()?;
        self.seen_objects += 1;
        ensure!(
            self.seen_objects <= self.owner.options.limits.max_objects,
            "native file work limit exceeded"
        );
        self.memory(self.owner.options.limits.max_file_bytes as u64)?;
        let file = self
            .client
            .download_file(m::ObjectId(id.0), self.owner.spool.path())
            .await
            .map_err(|_| anyhow::anyhow!("checked native FILE download failed"))?;
        let descriptor = file.descriptor();
        ensure!(
            descriptor.key
                == m::ObjectKey {
                    kind: m::Kind::File,
                    id: m::ObjectId(id.0)
                }
                && descriptor.size_bytes <= self.owner.options.limits.max_file_bytes as u64,
            "native FILE exceeds export limits"
        );
        self.account(descriptor.size_bytes)?;
        self.file_bytes = self
            .file_bytes
            .checked_add(descriptor.size_bytes)
            .context("file byte overflow")?;
        let maximum = self.owner.options.limits.max_file_bytes;
        let physical = self.owner.clone();
        let bytes = tokio::task::spawn_blocking(move || {
            let _owner = physical;
            let mut bytes = Vec::new();
            file.open()?
                .take(maximum as u64 + 1)
                .read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() as u64 == descriptor.size_bytes && m::file_id(&bytes) == id,
                "verified FILE changed during export"
            );
            Ok::<_, anyhow::Error>(bytes)
        })
        .await??;
        let oid = self.write("blob", bytes).await?.oid;
        self.objects.insert(key, oid.clone());
        Ok(oid)
    }
    pub fn contains_ancestor(&self, ancestor: &str, head: &str) -> Result<bool> {
        let mut stack = vec![head.to_owned()];
        let mut seen = BTreeSet::new();
        while let Some(id) = stack.pop() {
            if id == ancestor {
                return Ok(true);
            }
            if !seen.insert(id.clone()) {
                continue;
            }
            ensure!(
                seen.len() <= self.owner.options.limits.max_commits,
                "native ancestry verification limit exceeded"
            );
            let mapping = self
                .mappings
                .get(&id)
                .context("native mapped parent history missing")?;
            stack.extend(mapping.parents.iter().cloned());
        }
        Ok(false)
    }
}
fn commit_bytes(commit: &m::Commit, tree: &str, parents: &[String]) -> Result<Vec<u8>> {
    let mut out = format!("tree {tree}\n").into_bytes();
    for parent in parents {
        out.extend_from_slice(format!("parent {parent}\n").as_bytes());
    }
    out.extend_from_slice(
        format!(
            "author {}\ncommitter {}\n\n",
            signature(&commit.author)?,
            signature(&commit.committer)?
        )
        .as_bytes(),
    );
    ensure!(
        !commit.description.contains('\0'),
        "native commit message contains NUL"
    );
    out.extend_from_slice(commit.description.as_bytes());
    Ok(out)
}
fn signature(s: &m::Signature) -> Result<String> {
    ensure!(
        !s.name.is_empty()
            && s.name.trim() == s.name
            && !s.name.contains(['\0', '\r', '\n', '<', '>'])
            && !s.email.is_empty()
            && !s
                .email
                .chars()
                .any(|c| c.is_whitespace() || matches!(c, '\0' | '<' | '>')),
        "native identity cannot be represented safely in Git"
    );
    ensure!(
        s.timestamp.millis_since_epoch >= 0
            && (-1439..=1439).contains(&s.timestamp.tz_offset_minutes),
        "native timestamp cannot be represented by export policy v1"
    );
    let seconds = s.timestamp.millis_since_epoch.div_euclid(1000);
    let offset = s.timestamp.tz_offset_minutes;
    Ok(format!(
        "{} <{}> {seconds} {}{:02}{:02}",
        s.name,
        s.email,
        if offset < 0 { '-' } else { '+' },
        offset.abs() / 60,
        offset.abs() % 60
    ))
}
pub(crate) fn decode<const N: usize>(text: &str) -> Result<[u8; N]> {
    ensure!(valid_hex(text, N * 2), "invalid native/Git identifier");
    let mut out = [0; N];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)?;
    }
    Ok(out)
}
