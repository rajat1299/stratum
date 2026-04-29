use crate::error::VfsError;
use crate::store::ObjectId;
use std::fmt;

pub const MAIN_REF: &str = "main";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitId(ObjectId);

impl CommitId {
    pub fn object_id(self) -> ObjectId {
        self.0
    }

    pub fn to_hex(self) -> String {
        self.0.to_hex()
    }

    pub fn short_hex(self) -> String {
        self.0.short_hex()
    }
}

impl From<ObjectId> for CommitId {
    fn from(id: ObjectId) -> Self {
        Self(id)
    }
}

impl From<CommitId> for ObjectId {
    fn from(id: CommitId) -> Self {
        id.0
    }
}

impl fmt::Display for CommitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RefName(String);

impl RefName {
    pub fn new(name: impl Into<String>) -> Result<Self, VfsError> {
        let name = name.into();
        validate_ref_name(&name)?;
        Ok(Self(name))
    }

    pub fn session(actor: &str, session: &str) -> Result<Self, VfsError> {
        Self::new(format!("agent/{actor}/{session}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for RefName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefUpdateExpectation {
    pub target: CommitId,
    pub version: u64,
}

impl RefUpdateExpectation {
    pub fn new(target: CommitId, version: u64) -> Self {
        Self { target, version }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VcsRef {
    pub name: RefName,
    pub target: CommitId,
    pub version: u64,
}

fn validate_ref_name(name: &str) -> Result<(), VfsError> {
    if name.is_empty()
        || name.starts_with('/')
        || name.ends_with('/')
        || name.contains("//")
        || name.contains("..")
        || name.contains("@{")
        || name.contains('\\')
        || name.starts_with("refs/")
    {
        return invalid_ref(name);
    }

    for component in name.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.starts_with('.')
            || component.ends_with(".lock")
        {
            return invalid_ref(name);
        }
    }

    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.'))
    {
        return invalid_ref(name);
    }

    let components: Vec<&str> = name.split('/').collect();
    match components.as_slice() {
        ["main"] => Ok(()),
        ["agent", _, _] | ["review", _] | ["archive", _] => Ok(()),
        _ => invalid_ref(name),
    }
}

fn invalid_ref<T>(name: &str) -> Result<T, VfsError> {
    Err(VfsError::InvalidArgs {
        message: format!("invalid ref name: {name}"),
    })
}
