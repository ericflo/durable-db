//! Transaction components for the replacement journal engine.
//!
//! Validation tracks accepted writes, including those awaiting durability.
//! Records preserve evaluated mutations and receipts; recovery builds a private
//! view from selected history. The HTTP engine does not use these components yet.
pub mod record;
pub mod recovery;
pub mod validation;
