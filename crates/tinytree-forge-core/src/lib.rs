//! Canonical Forge policy and review rules without repository storage or process
//! dependencies. Embeddings own authentication and atomic publication authority.
pub mod accounts;
pub mod account_commands;
pub mod check;
pub mod identity;
pub mod review;
pub mod review_commands;
pub mod review_admission;
pub mod projection;
pub mod wire;
pub mod search;
pub mod upload;
pub mod upload_scope;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid(&'static str),
    Limit(&'static str),
    Corrupt(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid forge request: {reason}"),
            Self::Limit(reason) => write!(f, "forge limit: {reason}"),
            Self::Corrupt(reason) => write!(f, "corrupt forge record: {reason}"),
        }
    }
}

impl std::error::Error for Error {}
