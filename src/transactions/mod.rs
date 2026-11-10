//! Transaction components for the replacement journal engine.
//!
//! Validation tracks accepted writes, including those awaiting durability.
//! The live owner combines validation, journal durability, recovery and final
//! ownership checks. The public HTTP engine has not switched to it yet.
pub mod live;
pub mod record;
pub mod recovery;
pub mod validation;
