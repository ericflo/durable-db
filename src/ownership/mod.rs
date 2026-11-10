//! Components for the replacement ownership protocol.
//!
//! The native HTTP backend uses these under an injected clock contract.
//! The CLI runtime is not selected. A deadline is not authority:
//! provider confirmation, incarnation, placement and recovery must also agree.
pub mod root;
pub mod timing;
