use super::{Gid, Group, ROOT_GID, ROOT_UID, Uid, User, WHEEL_GID};
use crate::error::VfsError;
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRegistry {
    users: HashMap<Uid, User>,
    groups: HashMap<Gid, Group>,
    name_to_uid: HashMap<String, Uid>,
    name_to_gid: HashMap<String, Gid>,
    next_uid: Uid,
    next_gid: Gid,
}

impl Default for UserRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl UserRegistry {
    pub fn new() -> Self {
        let mut reg = UserRegistry {
            users: HashMap::new(),
            groups: HashMap::new(),
            name_to_uid: HashMap::new(),
            name_to_gid: HashMap::new(),
            next_uid: 1,
            next_gid: 2,
        };

        // Bootstrap: root group
        reg.groups.insert(
            ROOT_GID,
            Group {
                gid: ROOT_GID,
                name: "root".to_string(),
                members: vec![ROOT_UID],
            },
        );
        reg.name_to_gid.insert("root".to_string(), ROOT_GID);

        // Bootstrap: wheel group (admin)
        reg.groups.insert(
            WHEEL_GID,
            Group {
                gid: WHEEL_GID,
                name: "wheel".to_string(),
                members: vec![ROOT_UID],
            },
        );
        reg.name_to_gid.insert("wheel".to_string(), WHEEL_GID);

        // Bootstrap: root user
        reg.users.insert(
            ROOT_UID,
            User {
                uid: ROOT_UID,
                name: "root".to_string(),
                groups: vec![ROOT_GID, WHEEL_GID],
                api_token: None,
                is_agent: false,
            },
        );
        reg.name_to_uid.insert("root".to_string(), ROOT_UID);

        reg
    }

    pub fn add_user(
        &mut self,
        name: &str,
        is_agent: bool,
    ) -> Result<(Uid, Option<String>), VfsError> {
        if self.name_to_uid.contains_key(name) {
            return Err(VfsError::AuthError {
                message: format!("user already exists: {name}"),
            });
        }

        let uid = self.next_uid;
        self.next_uid += 1;

        // Create personal group
        let gid = self.next_gid;
        self.next_gid += 1;

        let group = Group {
            gid,
            name: name.to_string(),
            members: vec![uid],
        };
        self.groups.insert(gid, group);
        self.name_to_gid.insert(name.to_string(), gid);

        // Generate API token for agents
        let (api_token, raw_token) = if is_agent {
            let raw = generate_token();
            let hash = hash_token(&raw);
            (Some(hash), Some(raw))
        } else {
            (None, None)
        };

        let user = User {
            uid,
            name: name.to_string(),
            groups: vec![gid],
            api_token,
            is_agent,
        };
        self.users.insert(uid, user);
        self.name_to_uid.insert(name.to_string(), uid);

        Ok((uid, raw_token))
    }

    pub fn del_user(&mut self, name: &str) -> Result<(), VfsError> {
        let uid = self.lookup_uid(name).ok_or_else(|| VfsError::AuthError {
            message: format!("no such user: {name}"),
        })?;

        if uid == ROOT_UID {
            return Err(VfsError::AuthError {
                message: "cannot delete root".to_string(),
            });
        }

        // Remove from all groups
        for group in self.groups.values_mut() {
            group.members.retain(|&m| m != uid);
        }

        self.users.remove(&uid);
        self.name_to_uid.remove(name);
        Ok(())
    }

    pub fn add_group(&mut self, name: &str) -> Result<Gid, VfsError> {
        if self.name_to_gid.contains_key(name) {
            return Err(VfsError::AuthError {
                message: format!("group already exists: {name}"),
            });
        }

        let gid = self.next_gid;
        self.next_gid += 1;

        let group = Group {
            gid,
            name: name.to_string(),
            members: Vec::new(),
        };
        self.groups.insert(gid, group);
        self.name_to_gid.insert(name.to_string(), gid);
        Ok(gid)
    }

    pub fn del_group(&mut self, name: &str) -> Result<(), VfsError> {
        let gid = self.lookup_gid(name).ok_or_else(|| VfsError::AuthError {
            message: format!("no such group: {name}"),
        })?;

        if gid == ROOT_GID || gid == WHEEL_GID {
            return Err(VfsError::AuthError {
                message: format!("cannot delete system group: {name}"),
            });
        }

        // Remove group from all users' group lists
        for user in self.users.values_mut() {
            user.groups.retain(|&g| g != gid);
        }

        self.groups.remove(&gid);
        self.name_to_gid.remove(name);
        Ok(())
    }

    pub fn usermod_add_group(&mut self, username: &str, groupname: &str) -> Result<(), VfsError> {
        let uid = self
            .lookup_uid(username)
            .ok_or_else(|| VfsError::AuthError {
                message: format!("no such user: {username}"),
            })?;
        let gid = self
            .lookup_gid(groupname)
            .ok_or_else(|| VfsError::AuthError {
                message: format!("no such group: {groupname}"),
            })?;

        let user = self.users.get_mut(&uid).unwrap();
        if !user.groups.contains(&gid) {
            user.groups.push(gid);
        }

        let group = self.groups.get_mut(&gid).unwrap();
        if !group.members.contains(&uid) {
            group.members.push(uid);
        }

        Ok(())
    }

    pub fn usermod_remove_group(
        &mut self,
        username: &str,
        groupname: &str,
    ) -> Result<(), VfsError> {
        let uid = self
            .lookup_uid(username)
            .ok_or_else(|| VfsError::AuthError {
                message: format!("no such user: {username}"),
            })?;
        let gid = self
            .lookup_gid(groupname)
            .ok_or_else(|| VfsError::AuthError {
                message: format!("no such group: {groupname}"),
            })?;

        let user = self.users.get_mut(&uid).unwrap();
        user.groups.retain(|&g| g != gid);

        let group = self.groups.get_mut(&gid).unwrap();
        group.members.retain(|&m| m != uid);

        Ok(())
    }

    pub fn lookup_uid(&self, name: &str) -> Option<Uid> {
        self.name_to_uid.get(name).copied()
    }

    pub fn lookup_gid(&self, name: &str) -> Option<Gid> {
        self.name_to_gid.get(name).copied()
    }

    pub fn get_user(&self, uid: Uid) -> Option<&User> {
        self.users.get(&uid)
    }

    pub fn get_group(&self, gid: Gid) -> Option<&Group> {
        self.groups.get(&gid)
    }

    pub fn user_in_group(&self, uid: Uid, gid: Gid) -> bool {
        self.users
            .get(&uid)
            .map(|u| u.groups.contains(&gid))
            .unwrap_or(false)
    }

    pub fn authenticate_token(&self, raw_token: &str) -> Option<Uid> {
        self.users
            .values()
            .find(|u| {
                u.api_token
                    .as_deref()
                    .is_some_and(|stored_hash| token_hash_matches(stored_hash, raw_token))
            })
            .map(|u| u.uid)
    }

    pub fn list_users(&self) -> Vec<&User> {
        let mut users: Vec<&User> = self.users.values().collect();
        users.sort_by_key(|u| u.uid);
        users
    }

    pub fn list_groups(&self) -> Vec<&Group> {
        let mut groups: Vec<&Group> = self.groups.values().collect();
        groups.sort_by_key(|g| g.gid);
        groups
    }

    pub fn group_name(&self, gid: Gid) -> Option<&str> {
        self.groups.get(&gid).map(|g| g.name.as_str())
    }

    pub fn user_name(&self, uid: Uid) -> Option<&str> {
        self.users.get(&uid).map(|u| u.name.as_str())
    }

    pub fn is_wheel_member(&self, uid: Uid) -> bool {
        self.user_in_group(uid, WHEEL_GID)
    }
}

fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex_encode(&bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hash_token(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let result = hasher.finalize();
    hex_encode(&result)
}

fn token_hash_matches(stored_hash: &str, raw_token: &str) -> bool {
    let candidate_hash = hash_token(raw_token);
    constant_time_eq(stored_hash.as_bytes(), candidate_hash.as_bytes())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut diff = 0;
    for (left, right) in left.iter().zip(right) {
        diff |= left ^ right;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap() {
        let reg = UserRegistry::new();
        assert!(reg.get_user(ROOT_UID).is_some());
        assert_eq!(reg.get_user(ROOT_UID).unwrap().name, "root");
        assert!(reg.get_group(ROOT_GID).is_some());
        assert!(reg.get_group(WHEEL_GID).is_some());
        assert!(reg.is_wheel_member(ROOT_UID));
    }

    #[test]
    fn test_add_user() {
        let mut reg = UserRegistry::new();
        let (uid, token) = reg.add_user("alice", false).unwrap();
        assert_eq!(uid, 1);
        assert!(token.is_none());
        assert_eq!(reg.lookup_uid("alice"), Some(1));
        assert!(reg.get_user(1).is_some());
        // Personal group created
        assert!(reg.lookup_gid("alice").is_some());
    }

    #[test]
    fn test_add_agent() {
        let mut reg = UserRegistry::new();
        let (uid, token) = reg.add_user("bot1", true).unwrap();
        assert!(token.is_some());
        // Authenticate with token
        let raw = token.unwrap();
        assert_eq!(raw.len(), 64);
        assert!(raw.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(reg.authenticate_token(&raw), Some(uid));
        assert_eq!(reg.authenticate_token("wrong-token"), None);
        assert_ne!(
            reg.get_user(uid).unwrap().api_token.as_deref(),
            Some(raw.as_str())
        );
    }

    #[test]
    fn generated_agent_tokens_are_unique_hex_secrets() {
        let mut reg = UserRegistry::new();
        let mut tokens = std::collections::HashSet::new();

        for i in 0..128 {
            let (_, token) = reg.add_user(&format!("bot{i}"), true).unwrap();
            let token = token.expect("agent token");
            assert_eq!(token.len(), 64);
            assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
            assert!(tokens.insert(token), "agent token should be unique");
        }
    }

    #[test]
    fn token_hash_matching_accepts_legacy_hash_shape() {
        let raw = "agent-token-under-test";
        let stored_hash = hash_token(raw);

        assert!(token_hash_matches(&stored_hash, raw));
        assert!(!token_hash_matches(&stored_hash, "wrong-token"));
        assert!(!token_hash_matches("not-a-valid-hash", raw));
    }

    #[test]
    fn test_del_user() {
        let mut reg = UserRegistry::new();
        reg.add_user("bob", false).unwrap();
        assert!(reg.lookup_uid("bob").is_some());
        reg.del_user("bob").unwrap();
        assert!(reg.lookup_uid("bob").is_none());
    }

    #[test]
    fn test_cannot_delete_root() {
        let mut reg = UserRegistry::new();
        assert!(reg.del_user("root").is_err());
    }

    #[test]
    fn test_add_group_and_membership() {
        let mut reg = UserRegistry::new();
        reg.add_user("alice", false).unwrap();
        let gid = reg.add_group("devs").unwrap();
        reg.usermod_add_group("alice", "devs").unwrap();
        assert!(reg.user_in_group(1, gid));
        // Remove from group
        reg.usermod_remove_group("alice", "devs").unwrap();
        assert!(!reg.user_in_group(1, gid));
    }

    #[test]
    fn test_duplicate_user() {
        let mut reg = UserRegistry::new();
        reg.add_user("alice", false).unwrap();
        assert!(reg.add_user("alice", false).is_err());
    }

    #[test]
    fn test_cannot_delete_system_groups() {
        let mut reg = UserRegistry::new();
        assert!(reg.del_group("root").is_err());
        assert!(reg.del_group("wheel").is_err());
    }
}
