//! Drives the zakofish4 hub state machine over a WebSocket-shaped transport.
//!
//! [`serve`] owns one tap connection: the socket, the state machine and the
//! timers. It makes no protocol decisions of its own — every one comes from
//! [`zakofish4_common::state::handle_event`], and this crate only carries out
//! the actions that come back. That is what keeps the protocol testable over an
//! in-memory pipe, with no server and no port.
//!
//! The [`Transport`] trait is why: it is a stream of binary frames, not an
//! `axum::extract::ws::WebSocket`, so the real server is a thin shim over the
//! same driver the tests use.

pub mod backend;
pub mod driver;
pub mod handle;
mod timers;
pub mod transport;

pub use backend::HubBackend;
pub use driver::serve;
pub use handle::TapHandle;
pub use transport::Transport;

pub use zakofish4_common as common;
