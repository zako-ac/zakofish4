use std::fmt::Debug;

use async_trait::async_trait;

/// A bidirectional stream of binary frames.
///
/// The driver is written against this rather than against `axum::extract::ws`
/// so the protocol can be tested over an in-memory pipe, with no server, no
/// port, and no HTTP upgrade. The axum shim is a dozen lines on top.
#[async_trait]
pub trait Transport: Send + 'static {
    type Error: Debug + Send;

    /// Next frame, or `None` when the peer closed.
    async fn recv(&mut self) -> Option<Result<Vec<u8>, Self::Error>>;

    async fn send(&mut self, frame: Vec<u8>) -> Result<(), Self::Error>;

    async fn close(&mut self) -> Result<(), Self::Error>;
}

/// An in-memory [`Transport`] pair, for tests.
pub mod duplex {
    use super::Transport;
    use async_trait::async_trait;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    pub struct Pipe {
        rx: mpsc::Receiver<Vec<u8>>,
        tx: mpsc::Sender<Vec<u8>>,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("pipe closed")]
    pub struct Closed;

    /// Two ends wired to each other.
    pub fn pair(buffer: usize) -> (Pipe, Pipe) {
        let (a_tx, a_rx) = mpsc::channel(buffer);
        let (b_tx, b_rx) = mpsc::channel(buffer);
        (Pipe { rx: a_rx, tx: b_tx }, Pipe { rx: b_rx, tx: a_tx })
    }

    #[async_trait]
    impl Transport for Pipe {
        type Error = Closed;

        async fn recv(&mut self) -> Option<Result<Vec<u8>, Closed>> {
            self.rx.recv().await.map(Ok)
        }

        async fn send(&mut self, frame: Vec<u8>) -> Result<(), Closed> {
            self.tx.send(frame).await.map_err(|_| Closed)
        }

        async fn close(&mut self) -> Result<(), Closed> {
            self.rx.close();
            Ok(())
        }
    }
}
