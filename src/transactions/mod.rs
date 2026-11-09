//! Transaction components for the replacement journal engine.
//!
//! Validation tracks accepted writes, including those awaiting durability. It
//! does not read values, upload batches, issue receipts, or authorize service.
//! The HTTP engine does not use this module yet.
pub mod validation;
