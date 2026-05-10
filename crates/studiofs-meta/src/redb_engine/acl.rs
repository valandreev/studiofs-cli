use super::*;

impl MetaAcl for RedbMetaEngine {
    fn set_acl_rule(&self, ino: Ino, principal: &str, access_level: AccessLevel) -> MetaResult<()> {
        let _ = self.get_attr(ino)?;
        let key = encode_acl_rule_key(ino, principal);
        let txn = self.begin_crash_safe_write()?;
        {
            let mut rules = txn
                .open_table(ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            rules
                .insert(key.as_slice(), access_level as u8)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut tombstones = txn
                .open_table(ACL_RULE_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = tombstones.remove(key.as_slice());
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal: principal.to_string(),
                    access_level,
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<()> {
        let key = encode_acl_rule_key(ino, principal);
        let txn = self.begin_crash_safe_write()?;
        {
            let mut rules = txn
                .open_table(ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = rules.remove(key.as_slice());
            let mut tombstones = txn
                .open_table(ACL_RULE_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            tombstones
                .insert(key.as_slice(), 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveAclRule {
                    ino,
                    principal: principal.to_string(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn get_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<Option<AccessLevel>> {
        let key = encode_acl_rule_key(ino, principal);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if Self::has_slice_marker(&txn, ACL_RULE_TOMBSTONES_TABLE, key.as_slice())? {
            return Ok(None);
        }
        let rules = txn
            .open_table(ACL_RULES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if let Some(value) = rules
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            return AccessLevel::from_u8(value.value())
                .ok_or_else(|| MetaError::Serialization("invalid ACL access_level".to_string()))
                .map(Some);
        }
        let base = txn
            .open_table(BASE_ACL_RULES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match base
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(value) => AccessLevel::from_u8(value.value())
                .ok_or_else(|| MetaError::Serialization("invalid ACL access_level".to_string()))
                .map(Some),
            None => Ok(None),
        }
    }
}
