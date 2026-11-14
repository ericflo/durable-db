//! Accepting connections.
//!
//! A connection costs memory and a descriptor before its caller has shown a
//! token, so everything here is bounded without knowing who is calling: how
//! many connections are open, how long one may take to send a request, and
//! how long one may leave an answer unread.
use crate::http::{App, router};
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    server::graceful::GracefulShutdown,
    service::TowerToHyperService,
};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
    sync::Semaphore,
    time::Sleep,
};

/// A request's head may be this large. Hyper holds this much per connection
/// at most, whatever is sent.
const HEAD_BYTES: usize = 32 << 10;

/// Serve `app` on `listener` until `shutdown` completes, then let requests
/// in flight finish.
pub async fn serve(
    listener: TcpListener,
    app: Arc<App>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let limits = app.limits.clone();
    let mut http = http1::Builder::new();
    http.timer(TokioTimer::new())
        // Also closes a connection that sends nothing between requests.
        .header_read_timeout(limits.head_timeout)
        .max_buf_size(HEAD_BYTES);
    let service = TowerToHyperService::new(router(app));
    let connections = GracefulShutdown::new();
    let slots = Arc::new(Semaphore::new(limits.max_connections));
    tokio::pin!(shutdown);
    loop {
        // At the limit, further connections wait in the listen queue.
        let slot = tokio::select! {
            _ = &mut shutdown => break,
            slot = slots.clone().acquire_owned() => slot.expect("the semaphore is never closed"),
        };
        let stream = tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(error) => {
                    // A connection that failed before it was accepted is the
                    // caller's affair. Anything else, such as running out of
                    // descriptors, passes with time.
                    if !matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionRefused
                    ) {
                        tracing::warn!(%error, "cannot accept a connection");
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    continue;
                }
            },
        };
        let _ = stream.set_nodelay(true);
        let io = TokioIo::new(Unstalled::new(stream, limits.stall_timeout));
        let connection = connections.watch(http.serve_connection(io, service.clone()));
        tokio::spawn(async move {
            // An error here is the caller's: it went away, sent something
            // that is not HTTP, or took too long.
            let _ = connection.await;
            drop(slot);
        });
    }
    drop(listener);
    // Idle connections close at once; the others when their answer is sent.
    let _ = tokio::time::timeout(limits.drain_timeout, connections.shutdown()).await;
    Ok(())
}

/// A connection that fails when a write makes no progress for `patience`.
/// A caller that stops reading its answer therefore does not keep it in this
/// server's memory: the connection ends, and what it held is released.
struct Unstalled<S> {
    inner: S,
    patience: Duration,
    /// Running while a write is waiting for the caller.
    stalled: Option<Pin<Box<Sleep>>>,
}

impl<S> Unstalled<S> {
    fn new(inner: S, patience: Duration) -> Self {
        Self {
            inner,
            patience,
            stalled: None,
        }
    }

    fn progress<T>(
        &mut self,
        cx: &mut Context<'_>,
        polled: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if polled.is_ready() {
            self.stalled = None;
            return polled;
        }
        let patience = self.patience;
        let waited = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(patience)));
        match waited.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "the caller stopped reading",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Unstalled<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Unstalled<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.progress(cx, polled)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.progress(cx, polled)
    }

    /// Hyper keeps a response body as it was given, without copying it into
    /// a buffer of its own, only for a stream that says this.
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let polled = Pin::new(&mut self.inner).poll_flush(cx);
        self.progress(cx, polled)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
