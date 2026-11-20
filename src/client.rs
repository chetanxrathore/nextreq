//! The HTTP engine: connection pooling, the shared Tokio runtime, and the
//! sync / async dispatch paths used by the Python `Client` and `AsyncClient`.
//!
//! Design notes
//! ------------
//! * One process-wide multi-threaded Tokio runtime drives all I/O.
//! * The sync path parks the calling Python thread in `Runtime::block_on` with
//!   the interpreter *detached* (GIL released), waking every 100 ms only to let
//!   Python deliver signals such as Ctrl-C.
//! * The async path runs the request on the runtime and resolves an
//!   `asyncio.Future` through [`LoopBridge`], which batches completions.
//! * The runtime is owned here (not by a helper crate) so that it can be
//!   replaced in a child process after `os.fork()`.
//! * Everything between "request prepared" and "response ready" is pure Rust and
//!   never touches the interpreter.
//! * Timeouts are enforced per request (not per client) so they can be
//!   overridden on every call:
//!     - `pool`    waiting for a connection slot (semaphore)
//!     - `connect` DNS + TCP + TLS, enforced inside a connector layer
//!     - `write`   no upload progress on the request body
//!     - `read`    waiting for response headers, then for each body chunk

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Version};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::{PyBackedBytes, PyBackedStr};
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyList, PyString, PyTuple};
use pyo3::IntoPyObjectExt;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::config::{build_tls_config, Timeout, Timeouts, TlsOptions};
use crate::errors::{classify, ConnectTimedOut, ErrorKind, NxError, NxResult, Phase};
use crate::sse::PyDecoder;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Request bodies at least this large are sent through [`TrackedBody`] so
/// upload progress (and therefore write timeouts) can be observed.
const TRACKED_BODY_THRESHOLD: usize = 256 * 1024;
/// Frame size used when streaming a tracked request body.
const TRACKED_BODY_CHUNK: usize = 128 * 1024;
/// How often a blocked sync call wakes up to let Python run signal handlers.
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

/// The live runtime. Leaked on purpose: it lives as long as the process, and
/// after a fork the parent's instance must never be dropped in the child.
static RUNTIME: AtomicPtr<tokio::runtime::Runtime> = AtomicPtr::new(std::ptr::null_mut());
/// Bumped in the child after every `os.fork()`; engines built under an older
/// generation rebuild themselves on first use.
static FORK_GENERATION: AtomicU64 = AtomicU64::new(0);

fn worker_threads() -> usize {
    std::env::var("NEXTREQ_WORKER_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or_else(|| {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            // Requests are I/O-bound and their results are consumed by one
            // Python thread, so a few workers are plenty; more only adds
            // work-stealing churn (measured: 4 is the sweet spot).
            cores.clamp(2, 4)
        })
}

fn build_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .thread_name("nextreq-io")
        .enable_all()
        .build()
        .expect("failed to start the nextreq I/O runtime")
}

/// The shared multi-threaded runtime, started on first use.
pub fn runtime() -> &'static tokio::runtime::Runtime {
    let existing = RUNTIME.load(Ordering::Acquire);
    if !existing.is_null() {
        // SAFETY: a published runtime is never freed (see `RUNTIME`).
        return unsafe { &*existing };
    }
    let fresh = Box::into_raw(Box::new(build_runtime()));
    match RUNTIME.compare_exchange(
        std::ptr::null_mut(),
        fresh,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        // SAFETY: `fresh` was just leaked and is now the published runtime.
        Ok(_) => unsafe { &*fresh },
        Err(winner) => {
            // Another thread started the runtime first; discard ours.
            // SAFETY: `fresh` was never published, so this is the only owner.
            unsafe { Box::from_raw(fresh) }.shutdown_background();
            // SAFETY: as above, a published runtime is never freed.
            unsafe { &*winner }
        }
    }
}

/// How many times this process image has been forked since nextreq loaded.
pub fn fork_generation() -> u64 {
    FORK_GENERATION.load(Ordering::Acquire)
}

/// Reset per-process state in the child after `os.fork()`.
///
/// Only the forking thread exists in the child, so the parent's runtime (its
/// worker threads, timers and connection tasks) is dead there. It is
/// abandoned rather than dropped, because its locks may have been held by
/// threads that no longer exist; a new runtime starts on the next request.
#[pyfunction]
pub fn after_fork_in_child() {
    RUNTIME.store(std::ptr::null_mut(), Ordering::Release);
    FORK_GENERATION.fetch_add(1, Ordering::AcqRel);
}

/// Run `fut` to completion on the shared runtime with the interpreter detached.
///
/// The calling thread wakes periodically so pending Python signals (Ctrl-C)
/// are honoured; if a handler raises, the future is dropped, which cancels the
/// in-flight request.
pub fn block_on_interruptible<F, T>(py: Python<'_>, fut: F) -> PyResult<T>
where
    F: Future<Output = T> + Send,
    T: Send,
{
    let rt = runtime();
    let mut fut = std::pin::pin!(fut);
    loop {
        let step = py.detach(|| {
            rt.block_on(async { tokio::time::timeout(SIGNAL_CHECK_INTERVAL, fut.as_mut()).await })
        });
        match step {
            Ok(value) => return Ok(value),
            Err(_elapsed) => py.check_signals()?,
        }
    }
}

/// Wrapper that defers `NxError -> PyErr` conversion until we are attached.
pub struct Outcome<T>(pub NxResult<T>);

impl<'py, T> IntoPyObject<'py> for Outcome<T>
where
    T: IntoPyObject<'py>,
{
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            Ok(value) => value.into_bound_py_any(py),
            Err(err) => Err(err.into_pyerr(py)),
        }
    }
}

// ---------------------------------------------------------------------------
// asyncio completion bridge
// ---------------------------------------------------------------------------
//
// Every awaitable this extension hands to Python is an `asyncio.Future`
// resolved through this bridge. Resolving each future on its own would cost,
// per result, a GIL acquisition from a foreign thread (which fights the event
// loop for the GIL) and a loop wake-up.
//
// Here a finished request is pushed onto a queue *without touching the
// interpreter*. The first completion after the queue was last drained
// schedules a single `loop.call_soon_threadsafe(bridge.drain)` (from the
// blocking pool, so I/O workers never wait for the GIL); `drain` then runs on
// the event-loop thread, converts every queued result to Python objects and
// resolves the futures. Completions are therefore batched, and result
// conversion happens on the thread that already holds the GIL.

/// Turns a finished Rust result into the Python object for its future.
type Converter = Box<dyn FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send>;

struct Completion {
    /// The `asyncio.Future` awaiting this result.
    future: Py<PyAny>,
    /// Keeps the bridge reachable from the wake-up thread.
    bridge: Py<LoopBridge>,
    result: Converter,
}

#[derive(Default)]
struct BridgeQueue {
    pending: Mutex<Vec<Completion>>,
    /// True while a `drain` call is scheduled on the event loop.
    wake_scheduled: AtomicBool,
}

impl BridgeQueue {
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Completion>> {
        self.pending.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Queue a completion; called from Tokio workers, never attached.
    fn complete(self: &Arc<Self>, completion: Completion) {
        self.lock().push(completion);
        if !self.wake_scheduled.swap(true, Ordering::AcqRel) {
            let queue = Arc::clone(self);
            runtime().spawn_blocking(move || wake_event_loop(&queue));
        }
    }
}

/// Ask the event loop to run `LoopBridge.drain`. Runs on the blocking pool.
fn wake_event_loop(queue: &BridgeQueue) {
    // `try_attach` declines (instead of blocking forever) when the interpreter
    // is shutting down; nobody is left to receive the results in that case.
    let _ = Python::try_attach(|py| loop {
        let bridge = queue.lock().last().map(|c| c.bridge.clone_ref(py));
        let Some(bridge) = bridge else {
            // An earlier drain already took everything. Release the flag, then
            // re-check so a completion that raced with us is not stranded.
            queue.wake_scheduled.store(false, Ordering::Release);
            if queue.lock().is_empty() || queue.wake_scheduled.swap(true, Ordering::AcqRel) {
                return;
            }
            continue;
        };
        let bridge = bridge.bind(py);
        let scheduled = bridge.getattr(intern!(py, "drain")).and_then(|drain| {
            let event_loop = bridge.get().event_loop.bind(py).call0()?;
            event_loop.call_method1(intern!(py, "call_soon_threadsafe"), (drain,))
        });
        if scheduled.is_err() {
            // The event loop is closed or gone, so nobody can await these results.
            let orphaned = std::mem::take(&mut *queue.lock());
            queue.wake_scheduled.store(false, Ordering::Release);
            drop(orphaned);
        }
        return;
    });
}

/// Batches results from the Tokio runtime onto one asyncio event loop.
///
/// There is one per event loop; get it with `loop_bridge(loop)`.
#[pyclass(frozen, module = "nextreq._nextreq", name = "LoopBridge")]
pub struct LoopBridge {
    queue: Arc<BridgeQueue>,
    /// `weakref.ref(loop)`: the bridge is cached per loop, so a strong
    /// reference here would keep every loop ever used alive.
    event_loop: Py<PyAny>,
}

static GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static WEAKREF_REF: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
/// `WeakKeyDictionary[loop, LoopBridge]`.
static BRIDGES: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// The bridge of `event_loop`, created on first use.
#[pyfunction]
pub fn loop_bridge<'py>(
    py: Python<'py>,
    event_loop: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, LoopBridge>> {
    let bridges = BRIDGES
        .get_or_try_init(py, || {
            py.import("weakref")?
                .getattr("WeakKeyDictionary")?
                .call0()
                .map(Bound::unbind)
        })?
        .bind(py);
    let existing = bridges.call_method1(intern!(py, "get"), (event_loop,))?;
    if let Ok(bridge) = existing.cast_into::<LoopBridge>() {
        return Ok(bridge);
    }
    let weakref = WEAKREF_REF
        .get_or_try_init(py, || {
            py.import("weakref")?.getattr("ref").map(Bound::unbind)
        })?
        .bind(py);
    let bridge = Bound::new(
        py,
        LoopBridge {
            queue: Arc::new(BridgeQueue::default()),
            event_loop: weakref.call1((event_loop,))?.unbind(),
        },
    )?;
    bridges.set_item(event_loop, &bridge)?;
    Ok(bridge)
}

/// Run `work` on the runtime and return an awaitable for its result: an
/// `asyncio.Future` on the running event loop. Cancelling the future aborts
/// the work.
pub fn spawn_awaitable<'py, F, T>(py: Python<'py>, work: F) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = NxResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    let event_loop = GET_RUNNING_LOOP
        .get_or_try_init(py, || {
            py.import("asyncio")?
                .getattr("get_running_loop")
                .map(Bound::unbind)
        })?
        .bind(py)
        .call0()?;
    let bridge = loop_bridge(py, &event_loop)?;
    let future = event_loop.call_method0(intern!(py, "create_future"))?;
    let handle = Bound::new(py, LoopBridge::spawn(&bridge, &future, work))?;
    future.call_method1(
        intern!(py, "add_done_callback"),
        (handle.getattr(intern!(py, "abort_if_cancelled"))?,),
    )?;
    Ok(future)
}

impl LoopBridge {
    /// Run `work` on the runtime and resolve `future` with its result.
    fn spawn<F, T>(slf: &Bound<'_, Self>, future: &Bound<'_, PyAny>, work: F) -> TaskHandle
    where
        F: Future<Output = NxResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py> + Send + 'static,
    {
        let queue = Arc::clone(&slf.get().queue);
        let bridge = slf.clone().unbind();
        let future = future.clone().unbind();
        let task = runtime().spawn(async move {
            let result = work.await;
            queue.complete(Completion {
                future,
                bridge,
                result: Box::new(move |py| Outcome(result).into_pyobject(py).map(Bound::unbind)),
            });
        });
        TaskHandle {
            abort: task.abort_handle(),
        }
    }
}

#[pymethods]
impl LoopBridge {
    /// Resolve every queued future. Runs on the event-loop thread.
    fn drain(&self, py: Python<'_>) {
        loop {
            // Clear the flag *before* taking the batch: a completion pushed
            // after this point schedules its own wake-up, so none is lost.
            self.queue.wake_scheduled.store(false, Ordering::Release);
            let batch = std::mem::take(&mut *self.queue.lock());
            if batch.is_empty() {
                return;
            }
            for completion in batch {
                let future = completion.future.bind(py);
                // A cancelled future must be left alone; dropping the result
                // releases its connection.
                let done = future
                    .call_method0(intern!(py, "done"))
                    .and_then(|done| done.is_truthy())
                    .unwrap_or(true);
                if done {
                    continue;
                }
                let delivered = match (completion.result)(py) {
                    Ok(value) => future.call_method1(intern!(py, "set_result"), (value,)),
                    Err(err) => {
                        future.call_method1(intern!(py, "set_exception"), (err.into_value(py),))
                    }
                };
                if let Err(err) = delivered {
                    err.write_unraisable(py, Some(future));
                }
            }
        }
    }

    /// Number of results waiting to be delivered (for diagnostics).
    #[getter]
    fn pending(&self) -> usize {
        self.queue.lock().len()
    }
}

/// Handle to an in-flight asynchronous request.
#[pyclass(frozen, module = "nextreq._nextreq", name = "TaskHandle")]
pub struct TaskHandle {
    abort: tokio::task::AbortHandle,
}

#[pymethods]
impl TaskHandle {
    /// Abort the request (no-op if it already finished). The connection is
    /// dropped and the future is never resolved.
    fn abort(&self) {
        self.abort.abort();
    }

    /// Done-callback for the future: stop the work if nobody awaits it.
    fn abort_if_cancelled(&self, future: &Bound<'_, PyAny>) -> PyResult<()> {
        if future
            .call_method0(intern!(future.py(), "cancelled"))?
            .is_truthy()?
        {
            self.abort.abort();
        }
        Ok(())
    }

    #[getter]
    fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }
}

// ---------------------------------------------------------------------------
// Per-request context shared with the connector layer and the body
// ---------------------------------------------------------------------------

tokio::task_local! {
    static REQUEST_CTX: Arc<RequestCtx>;
}

/// Progress markers for one request, used to attribute timeouts to a phase
/// and to report phase timings. Offsets are nanoseconds since `start`
/// (0 means "not set").
pub struct RequestCtx {
    start: Instant,
    connect_timeout: Option<Duration>,
    connect_begin: AtomicU64,
    connect_end: AtomicU64,
    progress: AtomicU64,
    body_done: AtomicBool,
    /// A streamed request body has no chunk ready: the upload is waiting for
    /// the application, not for the network, so the write timeout is paused.
    producer_waiting: AtomicBool,
    notify: Notify,
}

impl RequestCtx {
    fn new(connect_timeout: Option<Duration>, has_tracked_body: bool) -> Self {
        RequestCtx {
            start: Instant::now(),
            connect_timeout,
            connect_begin: AtomicU64::new(0),
            connect_end: AtomicU64::new(0),
            progress: AtomicU64::new(0),
            body_done: AtomicBool::new(!has_tracked_body),
            producer_waiting: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn now_offset(&self) -> u64 {
        (self.start.elapsed().as_nanos() as u64).max(1)
    }

    fn touch(&self) {
        self.progress.store(self.now_offset(), Ordering::Relaxed);
    }

    fn mark_connect_begin(&self) {
        self.connect_end.store(0, Ordering::Relaxed);
        self.connect_begin
            .store(self.now_offset(), Ordering::Relaxed);
    }

    fn mark_connect_end(&self) {
        self.connect_end.store(self.now_offset(), Ordering::Relaxed);
        self.notify.notify_one();
    }

    fn mark_producer_waiting(&self) {
        self.producer_waiting.store(true, Ordering::Relaxed);
    }

    /// A chunk of a streamed body was handed to the connection.
    fn mark_chunk_sent(&self) {
        self.touch();
        if self.producer_waiting.swap(false, Ordering::Relaxed) {
            // The write clock restarts now; let the watchdog re-arm.
            self.notify.notify_one();
        }
    }

    fn mark_body_done(&self) {
        self.touch();
        self.producer_waiting.store(false, Ordering::Relaxed);
        if !self.body_done.swap(true, Ordering::Relaxed) {
            self.notify.notify_one();
        }
    }

    /// Time spent establishing a new connection, if this request opened one.
    fn connect_duration(&self) -> Option<Duration> {
        let begin = self.connect_begin.load(Ordering::Relaxed);
        let end = self.connect_end.load(Ordering::Relaxed);
        (begin != 0 && end >= begin).then(|| Duration::from_nanos(end - begin))
    }

    /// The instant at which the current phase times out, and which timeout
    /// that would be. `None` means the current phase is unbounded.
    fn deadline(&self, t: &Timeouts) -> Option<(Instant, ErrorKind)> {
        let (phase_timeout, kind) = if self.body_done.load(Ordering::Relaxed) {
            (t.read?, ErrorKind::ReadTimeout)
        } else {
            if self.producer_waiting.load(Ordering::Relaxed) {
                return None;
            }
            (t.write?, ErrorKind::WriteTimeout)
        };
        let begin = self.connect_begin.load(Ordering::Relaxed);
        let end = self.connect_end.load(Ordering::Relaxed);
        if begin != 0 && end == 0 {
            // Still connecting. The connector layer enforces the connect
            // timeout itself; the phase clock only starts once it finishes.
            let connect = self.connect_timeout?;
            return Some((
                self.start + Duration::from_nanos(begin) + connect + phase_timeout,
                kind,
            ));
        }
        let base = self.progress.load(Ordering::Relaxed).max(end);
        Some((
            self.start + Duration::from_nanos(base) + phase_timeout,
            kind,
        ))
    }
}

/// Connector layer that applies the per-request connect timeout and records
/// how long connection establishment took.
#[derive(Clone)]
pub struct ConnectLayer;

impl<S> tower_layer::Layer<S> for ConnectLayer {
    type Service = ConnectService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectService { inner }
    }
}

#[derive(Clone)]
pub struct ConnectService<S> {
    inner: S,
}

impl<S, R> tower_service::Service<R> for ConnectService<S>
where
    S: tower_service::Service<R, Error = BoxError>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        // `call` runs synchronously inside the request future, so the
        // task-local set by `execute` is visible here.
        let ctx = REQUEST_CTX.try_with(Arc::clone).ok();
        let fut = self.inner.call(req);
        let Some(ctx) = ctx else {
            return Box::pin(fut);
        };
        ctx.mark_connect_begin();
        Box::pin(async move {
            let result = match ctx.connect_timeout {
                Some(limit) => match tokio::time::timeout(limit, fut).await {
                    Ok(result) => result,
                    Err(_) => Err(Box::new(ConnectTimedOut) as BoxError),
                },
                None => fut.await,
            };
            ctx.mark_connect_end();
            result
        })
    }
}

/// A request body that reports upload progress to the [`RequestCtx`].
struct TrackedBody {
    data: Bytes,
    ctx: Arc<RequestCtx>,
}

impl http_body::Body for TrackedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        if self.data.is_empty() {
            self.ctx.mark_body_done();
            return Poll::Ready(None);
        }
        let take = self.data.len().min(TRACKED_BODY_CHUNK);
        let chunk = self.data.split_to(take);
        if self.data.is_empty() {
            self.ctx.mark_body_done();
        } else {
            self.ctx.touch();
        }
        Poll::Ready(Some(Ok(http_body::Frame::data(chunk))))
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_empty()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.data.len() as u64)
    }
}

// ---------------------------------------------------------------------------
// Streamed request bodies
// ---------------------------------------------------------------------------

/// How many chunks a streamed request body may queue ahead of the socket.
const UPLOAD_QUEUE_CHUNKS: usize = 4;

#[derive(Default)]
struct UploadState {
    /// Set when the producer reached the end of the body. A channel that
    /// closes without it means the body was cut short, which must fail the
    /// request instead of looking like a complete (truncated) upload.
    finished: AtomicBool,
}

/// A request body fed chunk by chunk through a bounded channel, so only a
/// few chunks are ever held in memory and a slow connection applies
/// backpressure to whatever produces the data.
struct ChannelBody {
    chunks: tokio::sync::mpsc::Receiver<Bytes>,
    state: Arc<UploadState>,
    ctx: Arc<RequestCtx>,
    done: bool,
}

impl http_body::Body for ChannelBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.chunks.poll_recv(cx) {
            Poll::Pending => {
                self.ctx.mark_producer_waiting();
                Poll::Pending
            }
            Poll::Ready(Some(chunk)) => {
                self.ctx.mark_chunk_sent();
                Poll::Ready(Some(Ok(http_body::Frame::data(chunk))))
            }
            Poll::Ready(None) => {
                self.done = true;
                if self.state.finished.load(Ordering::Acquire) {
                    self.ctx.mark_body_done();
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(
                        "the request body ended before it was complete".into()
                    )))
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

/// The producing side of a streamed request body, for `AsyncClient`: Python
/// pushes chunks as its (async) iterator yields them.
///
/// Create one, pass it as the `body` of `HttpClient.send_async`, then feed it
/// and call `finish()`. Dropping it without `finish()` fails the request.
#[pyclass(frozen, module = "nextreq._nextreq", name = "UploadStream")]
pub struct UploadStream {
    sender: Mutex<Option<tokio::sync::mpsc::Sender<Bytes>>>,
    receiver: Mutex<Option<tokio::sync::mpsc::Receiver<Bytes>>>,
    state: Arc<UploadState>,
}

impl UploadStream {
    fn sender(&self) -> Option<tokio::sync::mpsc::Sender<Bytes>> {
        self.sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[pymethods]
impl UploadStream {
    #[new]
    fn py_new() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(UPLOAD_QUEUE_CHUNKS);
        UploadStream {
            sender: Mutex::new(Some(sender)),
            receiver: Mutex::new(Some(receiver)),
            state: Arc::new(UploadState::default()),
        }
    }

    /// Queue a chunk without waiting: 0 = queued, 1 = the queue is full (use
    /// `send`), 2 = the request no longer wants the body.
    fn try_send(&self, chunk: PyBackedBytes) -> u8 {
        use tokio::sync::mpsc::error::TrySendError;
        let Some(sender) = self.sender() else {
            return 2;
        };
        if chunk.is_empty() {
            return 0;
        }
        match sender.try_send(Bytes::copy_from_slice(&chunk)) {
            Ok(()) => 0,
            Err(TrySendError::Full(_)) => 1,
            Err(TrySendError::Closed(_)) => 2,
        }
    }

    /// Queue a chunk, waiting for room. The awaitable resolves to `False`
    /// when the request no longer wants the body.
    fn send<'py>(&self, py: Python<'py>, chunk: PyBackedBytes) -> PyResult<Bound<'py, PyAny>> {
        let sender = self.sender();
        let chunk = Bytes::copy_from_slice(&chunk);
        spawn_awaitable(py, async move {
            let delivered = match sender {
                Some(sender) => sender.send(chunk).await.is_ok(),
                None => false,
            };
            Ok::<bool, NxError>(delivered)
        })
    }

    /// The body is complete.
    fn finish(&self) {
        self.state.finished.store(true, Ordering::Release);
        *self.sender.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// The body could not be produced: fail the request.
    fn fail(&self) {
        *self.sender.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

/// The next non-empty chunk of a Python iterator of `bytes`.
fn next_upload_chunk(py: Python<'_>, iterator: &Py<PyAny>) -> PyResult<Option<Bytes>> {
    let iterator = iterator.bind(py);
    loop {
        match iterator.call_method0(intern!(py, "__next__")) {
            Ok(item) => {
                let data: PyBackedBytes = item.extract()?;
                if !data.is_empty() {
                    return Ok(Some(Bytes::copy_from_slice(&data)));
                }
            }
            Err(err) if err.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => {
                return Ok(None)
            }
            Err(err) => return Err(err),
        }
    }
}

/// Pull chunks from a Python iterator into the body channel. Runs inside
/// `block_on` on the thread that called `send()`, so the iterator is consumed
/// on the caller's own thread, one chunk at a time, as the connection makes
/// room for it.
async fn feed_upload(
    sender: tokio::sync::mpsc::Sender<Bytes>,
    state: Arc<UploadState>,
    iterator: Py<PyAny>,
    failure: Arc<Mutex<Option<PyErr>>>,
) {
    loop {
        let Ok(permit) = sender.reserve().await else {
            return; // the request finished (or failed) without needing the rest
        };
        match Python::attach(|py| next_upload_chunk(py, &iterator)) {
            Ok(Some(chunk)) => permit.send(chunk),
            Ok(None) => {
                state.finished.store(true, Ordering::Release);
                return;
            }
            Err(err) => {
                // Leaving the body unfinished fails the request; the caller
                // re-raises the iterator's own exception.
                *failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(err);
                return;
            }
        }
    }
}

/// Run a request whose body is fed by `feeder`.
async fn execute_with_upload<F>(
    prepared: Prepared,
    stream: bool,
    feeder: F,
) -> NxResult<RawResponse>
where
    F: Future<Output = ()>,
{
    let ctx = prepared.ctx.clone();
    let timeouts = prepared.timeouts;
    let request = execute(prepared, stream);
    tokio::pin!(request);
    tokio::pin!(feeder);
    let mut feeding = true;
    let response = loop {
        tokio::select! {
            result = &mut request => break result?,
            _ = &mut feeder, if feeding => feeding = false,
        }
    };
    if !(stream && feeding) {
        // The response is complete (or the body is): nothing left to send.
        return Ok(response);
    }
    // A streaming response arrived while the body is still being sent. The
    // caller cannot feed it once `send()` returns, so finish the upload here,
    // still watching for a stalled connection.
    loop {
        let deadline = ctx.deadline(&timeouts).map(|(at, _)| at);
        let sleeper = async {
            match deadline {
                Some(at) => tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = &mut feeder => return Ok(response),
            _ = sleeper => {
                if let Some((at, kind)) = ctx.deadline(&timeouts) {
                    if Instant::now() >= at {
                        return Err(timeout_error(kind, &timeouts));
                    }
                }
            }
            _ = ctx.notify.notified() => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Raw response handed back to Python
// ---------------------------------------------------------------------------

/// Phase timings in seconds.
#[derive(Debug, Clone, Copy)]
pub struct Timings {
    total: Duration,
    pool: Duration,
    connect: Option<Duration>,
    ttfb: Duration,
    download: Option<Duration>,
}

pub enum RawBody {
    Full(Vec<Bytes>),
    Stream(Box<StreamState>),
}

pub struct RawResponse {
    status: u16,
    version: Version,
    headers: HeaderMap,
    body: RawBody,
    timings: Timings,
}

fn version_str(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2",
        Version::HTTP_3 => "HTTP/3",
        _ => "HTTP/?",
    }
}

fn headers_to_py<'py>(py: Python<'py>, headers: &HeaderMap) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for (name, value) in headers.iter() {
        let value = match std::str::from_utf8(value.as_bytes()) {
            Ok(text) => PyString::new(py, text),
            Err(_) => {
                // Header values are opaque bytes; fall back to ISO-8859-1.
                let text: String = value.as_bytes().iter().map(|&b| b as char).collect();
                PyString::new(py, &text)
            }
        };
        list.append((name.as_str(), value))?;
    }
    Ok(list)
}

impl<'py> IntoPyObject<'py> for RawResponse {
    type Target = PyTuple;
    type Output = Bound<'py, PyTuple>;
    type Error = PyErr;

    /// `(status, http_version, headers, body, (total, pool, connect, ttfb, download))`
    /// where `body` is `bytes`, or a `ResponseStream` for streamed responses.
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let headers = headers_to_py(py, &self.headers)?;
        let body = match self.body {
            RawBody::Full(chunks) => join_chunks(py, &chunks)?.into_any(),
            RawBody::Stream(state) => Bound::new(py, ResponseStream::new(*state))?.into_any(),
        };
        let t = self.timings;
        let timings = (
            t.total.as_secs_f64(),
            t.pool.as_secs_f64(),
            t.connect.map(|d| d.as_secs_f64()),
            t.ttfb.as_secs_f64(),
            t.download.map(|d| d.as_secs_f64()),
        );
        PyTuple::new(
            py,
            [
                self.status.into_bound_py_any(py)?,
                version_str(self.version).into_bound_py_any(py)?,
                headers.into_any(),
                body,
                timings.into_bound_py_any(py)?,
            ],
        )
    }
}

// ---------------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------------

pub struct Inner {
    client: reqwest::Client,
    limiter: Option<Arc<Semaphore>>,
    pub ws_tls: Arc<rustls::ClientConfig>,
    /// The fork generation this engine (its pool and tasks) belongs to.
    generation: u64,
}

/// The body of a request being prepared.
enum RequestBody {
    Empty,
    Buffered(PyBackedBytes),
    Channel(tokio::sync::mpsc::Receiver<Bytes>, Arc<UploadState>),
}

/// A prepared request, ready to run without the interpreter.
pub struct Prepared {
    inner: Arc<Inner>,
    request: reqwest::Request,
    timeouts: Timeouts,
    ctx: Arc<RequestCtx>,
}

async fn acquire_slot(
    inner: &Inner,
    pool_timeout: Option<Duration>,
) -> NxResult<Option<OwnedSemaphorePermit>> {
    let Some(limiter) = &inner.limiter else {
        return Ok(None);
    };
    if let Ok(permit) = limiter.clone().try_acquire_owned() {
        return Ok(Some(permit));
    }
    let closed = |_| NxError::new(ErrorKind::ClientClosed, "the client has been closed");
    let acquire = limiter.clone().acquire_owned();
    let permit = match pool_timeout {
        Some(limit) => tokio::time::timeout(limit, acquire)
            .await
            .map_err(|_| {
                NxError::new(
                    ErrorKind::PoolTimeout,
                    format!(
                        "timed out after {:.3}s waiting for a free connection \
                         (all max_connections slots are busy)",
                        limit.as_secs_f64()
                    ),
                )
            })?
            .map_err(closed)?,
        None => acquire.await.map_err(closed)?,
    };
    Ok(Some(permit))
}

fn timeout_error(kind: ErrorKind, t: &Timeouts) -> NxError {
    let message = match kind {
        ErrorKind::WriteTimeout => format!(
            "timed out sending the request body (no upload progress for {:.3}s)",
            t.write.unwrap_or_default().as_secs_f64()
        ),
        _ => format!(
            "timed out waiting for the response (no data received for {:.3}s)",
            t.read.unwrap_or_default().as_secs_f64()
        ),
    };
    NxError::new(kind, message)
}

/// Await the response head, enforcing the write and read timeouts.
async fn send_watched<F>(fut: F, ctx: &RequestCtx, t: &Timeouts) -> NxResult<reqwest::Response>
where
    F: Future<Output = reqwest::Result<reqwest::Response>>,
{
    tokio::pin!(fut);
    if t.read.is_none() && t.write.is_none() {
        return fut.await.map_err(|e| classify(e, Phase::Send));
    }
    loop {
        let deadline = ctx.deadline(t).map(|(at, _)| at);
        let sleeper = async {
            match deadline {
                Some(at) => tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            result = &mut fut => return result.map_err(|e| classify(e, Phase::Send)),
            _ = sleeper => {
                // Progress may have moved the deadline; only fail if it truly passed.
                if let Some((at, kind)) = ctx.deadline(t) {
                    if Instant::now() >= at {
                        return Err(timeout_error(kind, t));
                    }
                }
            }
            _ = ctx.notify.notified() => {}
        }
    }
}

async fn next_chunk(
    response: &mut reqwest::Response,
    read_timeout: Option<Duration>,
) -> NxResult<Option<Bytes>> {
    let result = match read_timeout {
        Some(limit) => tokio::time::timeout(limit, response.chunk())
            .await
            .map_err(|_| {
                NxError::new(
                    ErrorKind::ReadTimeout,
                    format!(
                        "timed out reading the response body (no data received for {:.3}s)",
                        limit.as_secs_f64()
                    ),
                )
            })?,
        None => response.chunk().await,
    };
    result.map_err(|e| classify(e, Phase::Body))
}

/// Read the whole body as the list of chunks it arrived in.
///
/// The chunks are deliberately *not* concatenated here: they are copied once,
/// straight into the final Python `bytes` object (see [`join_chunks`]), which
/// halves the memory traffic for large bodies.
async fn read_body(
    response: &mut reqwest::Response,
    read_timeout: Option<Duration>,
) -> NxResult<Vec<Bytes>> {
    let mut chunks = Vec::new();
    while let Some(chunk) = next_chunk(response, read_timeout).await? {
        if !chunk.is_empty() {
            chunks.push(chunk);
        }
    }
    Ok(chunks)
}

/// Run one request. Pure Rust: safe to call from any Tokio worker.
pub async fn execute(prepared: Prepared, stream: bool) -> NxResult<RawResponse> {
    let Prepared {
        inner,
        request,
        timeouts,
        ctx,
    } = prepared;
    let started = Instant::now();
    let permit = acquire_slot(&inner, timeouts.pool).await?;
    let pool = started.elapsed();
    ctx.touch();

    let sent = Instant::now();
    let pending = REQUEST_CTX.scope(ctx.clone(), inner.client.execute(request));
    let mut response = send_watched(pending, &ctx, &timeouts).await?;
    let ttfb = sent.elapsed();

    let status = response.status().as_u16();
    let version = response.version();
    let headers = std::mem::take(response.headers_mut());
    let connect = ctx.connect_duration();

    if stream {
        return Ok(RawResponse {
            status,
            version,
            headers,
            timings: Timings {
                total: started.elapsed(),
                pool,
                connect,
                ttfb,
                download: None,
            },
            body: RawBody::Stream(Box::new(StreamState {
                response,
                _permit: permit,
                read_timeout: timeouts.read,
            })),
        });
    }

    let body_started = Instant::now();
    let body = read_body(&mut response, timeouts.read).await?;
    drop(response);
    drop(permit);
    Ok(RawResponse {
        status,
        version,
        headers,
        body: RawBody::Full(body),
        timings: Timings {
            total: started.elapsed(),
            pool,
            connect,
            ttfb,
            download: Some(body_started.elapsed()),
        },
    })
}

// ---------------------------------------------------------------------------
// Streaming responses
// ---------------------------------------------------------------------------

pub struct StreamState {
    response: reqwest::Response,
    /// Held until the body is drained or closed, so `max_connections` counts
    /// in-flight streams too.
    _permit: Option<OwnedSemaphorePermit>,
    read_timeout: Option<Duration>,
}

type SharedStream = Arc<tokio::sync::Mutex<Option<StreamState>>>;

async fn stream_next(state: &SharedStream) -> NxResult<Option<Bytes>> {
    let mut guard = state.lock().await;
    let Some(active) = guard.as_mut() else {
        return Ok(None);
    };
    match next_chunk(&mut active.response, active.read_timeout).await {
        Ok(Some(chunk)) => Ok(Some(chunk)),
        Ok(None) => {
            *guard = None;
            Ok(None)
        }
        Err(err) => {
            *guard = None;
            Err(err)
        }
    }
}

async fn stream_read_all(state: &SharedStream) -> NxResult<Vec<Bytes>> {
    let mut guard = state.lock().await;
    let Some(mut active) = guard.take() else {
        return Ok(Vec::new());
    };
    read_body(&mut active.response, active.read_timeout).await
}

/// How many chunks the pump may read ahead of the consumer.
const PUMP_QUEUE_CHUNKS: usize = 16;
/// Upper bound on the bytes handed to Python by one streaming read.
const MAX_BYTES_PER_READ: usize = 1024 * 1024;

/// Background task that reads a streamed response body ahead of Python.
///
/// Fetching one chunk is a round trip to the connection task. Doing that from
/// the Python thread costs a thread park/unpark per chunk; doing it from a
/// Tokio task lets the body arrive while Python is busy, so each streaming
/// read hands over everything received so far in a single crossing. That
/// matters most for many-small-chunk streams such as Server-Sent Events. The
/// bounded queue applies backpressure when Python is slower than the network.
struct StreamPump {
    queue: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<NxResult<Bytes>>>,
    /// An error that arrived after chunks were already collected for the caller.
    deferred_error: Mutex<Option<NxError>>,
    task: tokio::task::AbortHandle,
}

impl StreamPump {
    fn start(state: SharedStream) -> Arc<StreamPump> {
        let (sender, receiver) = tokio::sync::mpsc::channel(PUMP_QUEUE_CHUNKS);
        let task = runtime().spawn(async move {
            loop {
                match stream_next(&state).await {
                    Ok(Some(chunk)) => {
                        if sender.send(Ok(chunk)).await.is_err() {
                            break; // the consumer went away
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        let _ = sender.send(Err(err)).await;
                        break;
                    }
                }
            }
        });
        Arc::new(StreamPump {
            queue: tokio::sync::Mutex::new(receiver),
            deferred_error: Mutex::new(None),
            task: task.abort_handle(),
        })
    }

    fn take_deferred_error(&self) -> Option<NxError> {
        self.deferred_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    /// Wait for at least one chunk, then also take whatever else has already
    /// arrived (up to `max_bytes`). `None` means end of stream.
    async fn next_chunks(&self, max_bytes: usize) -> NxResult<Option<Vec<Bytes>>> {
        if let Some(err) = self.take_deferred_error() {
            return Err(err);
        }
        let mut queue = self.queue.lock().await;
        let Some(first) = queue.recv().await else {
            return Ok(None);
        };
        let first = first?;
        let mut total = first.len();
        let mut chunks = vec![first];
        while total < max_bytes {
            match queue.try_recv() {
                Ok(Ok(chunk)) => {
                    total += chunk.len();
                    chunks.push(chunk);
                }
                Ok(Err(err)) => {
                    // Deliver what we have; raise on the next call.
                    *self
                        .deferred_error
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()) = Some(err);
                    break;
                }
                Err(_) => break,
            }
        }
        Ok(Some(chunks))
    }

    /// Everything that is left, as the chunks it arrived in.
    async fn read_to_end(&self) -> NxResult<Vec<Bytes>> {
        let mut all = Vec::new();
        while let Some(chunks) = self.next_chunks(usize::MAX).await? {
            all.extend(chunks);
        }
        Ok(all)
    }
}

fn join_chunks<'py>(py: Python<'py>, chunks: &[Bytes]) -> PyResult<Bound<'py, PyBytes>> {
    match chunks {
        [] => return Ok(PyBytes::new(py, b"")),
        [only] => return Ok(PyBytes::new(py, only)),
        _ => {}
    }
    let total: usize = chunks.iter().map(Bytes::len).sum();
    PyBytes::new_with(py, total, |buffer| {
        let mut offset = 0;
        for chunk in chunks {
            buffer[offset..offset + chunk.len()].copy_from_slice(chunk);
            offset += chunk.len();
        }
        Ok(())
    })
}

/// `bytes | None` result of a streaming read.
pub struct ChunksOut(Option<Vec<Bytes>>);

impl<'py> IntoPyObject<'py> for ChunksOut {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self.0 {
            Some(chunks) => join_chunks(py, &chunks)?.into_any(),
            None => py.None().into_bound(py),
        })
    }
}

/// `list[SSEEvent] | None` result of a streaming SSE read. Decoding happens
/// here, while attached, so events are built directly as Python objects.
pub struct EventsOut {
    chunks: Option<Vec<Bytes>>,
    decoder: Arc<Mutex<PyDecoder>>,
}

impl<'py> IntoPyObject<'py> for EventsOut {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let mut decoder = self.decoder.lock().unwrap_or_else(|e| e.into_inner());
        match self.chunks {
            Some(chunks) => {
                let list = PyList::empty(py);
                decoder.feed_into(py, &chunks, &list)?;
                Ok(list.into_any())
            }
            None => {
                decoder.finish();
                Ok(py.None().into_bound(py))
            }
        }
    }
}

/// Handle to a response body that is still being received.
///
/// Every `read*` method has a blocking form (GIL released while waiting) and
/// an `a`-prefixed awaitable form.
#[pyclass(frozen, module = "nextreq._nextreq", name = "ResponseStream")]
pub struct ResponseStream {
    state: SharedStream,
    /// Started lazily by the first incremental read.
    pump: Mutex<Option<Arc<StreamPump>>>,
    decoder: Arc<Mutex<PyDecoder>>,
}

impl ResponseStream {
    fn new(state: StreamState) -> Self {
        ResponseStream {
            state: Arc::new(tokio::sync::Mutex::new(Some(state))),
            pump: Mutex::new(None),
            decoder: Arc::new(Mutex::new(PyDecoder::default())),
        }
    }

    fn existing_pump(&self) -> Option<Arc<StreamPump>> {
        self.pump.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn pump(&self) -> Arc<StreamPump> {
        self.pump
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(|| StreamPump::start(self.state.clone()))
            .clone()
    }
}

#[pymethods]
impl ResponseStream {
    /// The next piece of the body, or `None` at end of stream.
    ///
    /// Returns everything received so far (at least one chunk), so the size
    /// of the returned `bytes` varies.
    fn read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pump = self.pump();
        let result = block_on_interruptible(py, pump.next_chunks(MAX_BYTES_PER_READ))?;
        Outcome(result.map(ChunksOut)).into_pyobject(py)
    }

    /// Awaitable form of `read()`.
    fn aread<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pump = self.pump();
        spawn_awaitable(py, async move {
            pump.next_chunks(MAX_BYTES_PER_READ).await.map(ChunksOut)
        })
    }

    /// The rest of the body as one `bytes` object.
    fn read_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let result = match self.existing_pump() {
            Some(pump) => block_on_interruptible(py, pump.read_to_end())?,
            None => block_on_interruptible(py, stream_read_all(&self.state))?,
        };
        Outcome(result.map(|chunks| ChunksOut(Some(chunks)))).into_pyobject(py)
    }

    /// Awaitable form of `read_all()`.
    fn aread_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pump = self.existing_pump();
        let state = self.state.clone();
        spawn_awaitable(py, async move {
            let result = match pump {
                Some(pump) => pump.read_to_end().await,
                None => stream_read_all(&state).await,
            };
            result.map(|chunks| ChunksOut(Some(chunks)))
        })
    }

    /// Next batch of Server-Sent Events as a `list[SSEEvent]`, or `None` at
    /// end of stream.
    ///
    /// The body is read ahead on the Tokio runtime with the GIL released;
    /// each call decodes everything received so far. The list may be empty
    /// when the data received did not complete an event.
    fn read_events<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pump = self.pump();
        let result = block_on_interruptible(py, pump.next_chunks(MAX_BYTES_PER_READ))?;
        Outcome(result.map(|chunks| EventsOut {
            chunks,
            decoder: self.decoder.clone(),
        }))
        .into_pyobject(py)
    }

    /// Awaitable form of `read_events()`.
    fn aread_events<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pump = self.pump();
        let decoder = self.decoder.clone();
        spawn_awaitable(py, async move {
            pump.next_chunks(MAX_BYTES_PER_READ)
                .await
                .map(|chunks| EventsOut { chunks, decoder })
        })
    }

    /// Release the connection. Safe to call more than once.
    fn close(&self) {
        // Stop the pump first: it may be parked waiting for the next chunk
        // while holding the stream lock.
        if let Some(pump) = self.existing_pump() {
            pump.task.abort();
        }
        match self.state.try_lock() {
            Ok(mut guard) => *guard = None,
            Err(_) => {
                // A read is in flight; close as soon as it lets go.
                let state = self.state.clone();
                runtime().spawn(async move {
                    *state.lock().await = None;
                });
            }
        }
    }

    /// Whether the stream has been fully consumed or closed.
    #[getter]
    fn is_closed(&self) -> bool {
        match self.state.try_lock() {
            Ok(guard) => guard.is_none(),
            Err(_) => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Python-facing client
// ---------------------------------------------------------------------------

/// The Rust HTTP engine: a connection pool plus TLS / proxy configuration.
///
/// This is the low-level transport used by `nextreq.Client` and
/// `nextreq.AsyncClient`; it performs exactly one HTTP exchange per call
/// (no redirects, cookies or retries - those live in the Python layer).
#[pyclass(frozen, module = "nextreq._nextreq", name = "HttpClient")]
pub struct HttpClient {
    inner: RwLock<Option<Arc<Inner>>>,
    /// Kept so the engine can be rebuilt in a forked child.
    options: ClientOptions,
}

#[derive(Clone)]
struct ClientOptions {
    tls: TlsOptions,
    proxies: Vec<(String, String)>,
    trust_env: bool,
    http1: bool,
    http2: bool,
    http3: bool,
    max_connections: Option<usize>,
    max_keepalive_connections: Option<usize>,
    keepalive_expiry: Option<f64>,
}

fn config_error(err: impl std::fmt::Display) -> NxError {
    NxError::new(ErrorKind::LocalProtocolError, err.to_string())
}

fn build_inner(options: &ClientOptions) -> NxResult<Inner> {
    if !options.http1 && !options.http2 && !options.http3 {
        return Err(config_error(
            "at least one of http1, http2 or http3 must be enabled",
        ));
    }
    if options.http3 && !cfg!(feature = "http3") {
        return Err(config_error(
            "this build of nextreq was compiled without HTTP/3 support; rebuild with \
             RUSTFLAGS=\"--cfg reqwest_unstable\" and the `http3` cargo feature",
        ));
    }
    let mut alpn: Vec<&[u8]> = Vec::new();
    if options.http2 {
        alpn.push(b"h2");
    }
    if options.http1 {
        alpn.push(b"http/1.1");
    }
    let tls = build_tls_config(&options.tls, &alpn)?;
    let ws_tls = Arc::new(build_tls_config(&options.tls, &[b"http/1.1"])?);

    let mut builder = reqwest::Client::builder()
        .tls_backend_preconfigured(tls)
        .redirect(reqwest::redirect::Policy::none())
        .referer(false)
        .tcp_nodelay(true)
        .connector_layer(ConnectLayer)
        .pool_idle_timeout(options.keepalive_expiry.map(Duration::from_secs_f64))
        .pool_max_idle_per_host(options.max_keepalive_connections.unwrap_or(usize::MAX));

    if !options.http2 {
        builder = builder.http1_only();
    } else if !options.http1 {
        builder = builder.http2_prior_knowledge();
    }
    #[cfg(feature = "http3")]
    if options.http3 {
        builder = builder.http3_prior_knowledge();
    }

    if !options.trust_env {
        builder = builder.no_proxy();
    }
    for (scope, url) in &options.proxies {
        let proxy = match scope.as_str() {
            "http" => reqwest::Proxy::http(url.as_str()),
            "https" => reqwest::Proxy::https(url.as_str()),
            _ => reqwest::Proxy::all(url.as_str()),
        }
        .map_err(|e| {
            NxError::new(
                ErrorKind::InvalidURL,
                format!("invalid proxy URL {url:?}: {e}"),
            )
        })?;
        let proxy = if options.trust_env {
            proxy.no_proxy(reqwest::NoProxy::from_env())
        } else {
            proxy
        };
        builder = builder.proxy(proxy);
    }

    let client = builder.build().map_err(config_error)?;
    Ok(Inner {
        client,
        limiter: options
            .max_connections
            .map(|n| Arc::new(Semaphore::new(n.max(1)))),
        ws_tls,
        generation: fork_generation(),
    })
}

impl HttpClient {
    pub fn inner(&self) -> NxResult<Arc<Inner>> {
        let closed = || NxError::new(ErrorKind::ClientClosed, "the client has been closed");
        let generation = fork_generation();
        {
            let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
            match guard.as_ref() {
                None => return Err(closed()),
                Some(inner) if inner.generation == generation => return Ok(inner.clone()),
                Some(_) => {}
            }
        }
        // This process was forked after the engine was built. Its pooled
        // connections are shared with the parent and its background tasks
        // belong to a runtime that does not exist here, so start afresh.
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            None => return Err(closed()),
            Some(inner) if inner.generation == generation => return Ok(inner.clone()),
            Some(_) => {}
        }
        let rebuilt = {
            let _guard = runtime().enter();
            Arc::new(build_inner(&self.options)?)
        };
        if let Some(stale) = guard.replace(rebuilt.clone()) {
            // Never run its destructors: they would wake tasks on the
            // parent's runtime, whose locks may be held by threads that do
            // not exist in this process.
            std::mem::forget(stale);
        }
        Ok(rebuilt)
    }

    fn ws_options(
        &self,
        url: String,
        headers: Vec<(String, String)>,
        subprotocols: Option<Vec<String>>,
        timeout: Option<&Bound<'_, Timeout>>,
        max_message_size: Option<usize>,
        compression: bool,
    ) -> NxResult<crate::ws::ConnectOptions> {
        let inner = self.inner()?;
        Ok(crate::ws::ConnectOptions {
            url,
            headers,
            subprotocols: subprotocols.unwrap_or_default(),
            tls: inner.ws_tls.clone(),
            proxies: self.options.proxies.clone(),
            trust_env: self.options.trust_env,
            connect_timeout: timeout.and_then(|t| t.get().timeouts().connect),
            max_message_size,
            compression,
        })
    }

    /// Validate and assemble a request while attached to the interpreter.
    fn prepare(
        &self,
        method: &str,
        url: &str,
        headers: &Bound<'_, PyAny>,
        body: RequestBody,
        timeout: Option<&Bound<'_, Timeout>>,
    ) -> PyResult<NxResult<Prepared>> {
        let inner = match self.inner() {
            Ok(inner) => inner,
            Err(err) => return Ok(Err(err)),
        };
        let Ok(method) = Method::from_bytes(method.as_bytes()) else {
            return Ok(Err(NxError::new(
                ErrorKind::LocalProtocolError,
                format!("invalid HTTP method {method:?}"),
            )));
        };
        let parsed = match reqwest::Url::parse(url) {
            Ok(parsed) => parsed,
            Err(err) => {
                return Ok(Err(NxError::new(
                    ErrorKind::InvalidURL,
                    format!("invalid URL {url:?}: {err}"),
                )))
            }
        };
        match parsed.scheme() {
            "http" | "https" => {}
            other => {
                return Ok(Err(NxError::new(
                    ErrorKind::UnsupportedProtocol,
                    format!("unsupported URL scheme {other:?} in {url:?} (expected http or https)"),
                )))
            }
        }
        if parsed.host_str().is_none_or(str::is_empty) {
            return Ok(Err(NxError::new(
                ErrorKind::InvalidURL,
                format!("invalid URL {url:?}: missing host"),
            )));
        }

        let mut header_map = HeaderMap::new();
        for item in headers.try_iter()? {
            let (name, value): (PyBackedStr, PyBackedStr) = item?.extract()?;
            let (Ok(header_name), Ok(header_value)) = (
                HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_bytes(value.as_bytes()),
            ) else {
                return Ok(Err(NxError::new(
                    ErrorKind::LocalProtocolError,
                    format!("invalid header {:?}", &*name),
                )));
            };
            header_map.append(header_name, header_value);
        }

        let timeouts = match timeout {
            Some(t) => t.get().timeouts(),
            None => Timeouts::NONE,
        };
        let tracked = match &body {
            RequestBody::Empty => false,
            RequestBody::Buffered(data) => data.len() >= TRACKED_BODY_THRESHOLD,
            RequestBody::Channel(..) => true,
        };
        let ctx = Arc::new(RequestCtx::new(timeouts.connect, tracked));

        let mut request = reqwest::Request::new(method, parsed);
        *request.headers_mut() = header_map;
        match body {
            RequestBody::Empty => {}
            RequestBody::Buffered(body) => {
                let data = Bytes::copy_from_slice(&body);
                *request.body_mut() = Some(if tracked {
                    reqwest::Body::wrap(TrackedBody {
                        data,
                        ctx: ctx.clone(),
                    })
                } else {
                    reqwest::Body::from(data)
                });
            }
            RequestBody::Channel(chunks, state) => {
                *request.body_mut() = Some(reqwest::Body::wrap(ChannelBody {
                    chunks,
                    state,
                    ctx: ctx.clone(),
                    done: false,
                }));
            }
        }
        Ok(Ok(Prepared {
            inner,
            request,
            timeouts,
            ctx,
        }))
    }
}

#[pymethods]
impl HttpClient {
    #[new]
    #[pyo3(signature = (
        *,
        verify = true,
        ca_bundle = None,
        cert = None,
        key = None,
        proxies = None,
        trust_env = true,
        http1 = true,
        http2 = true,
        http3 = false,
        max_connections = Some(100),
        max_keepalive_connections = None,
        keepalive_expiry = Some(30.0),
    ))]
    #[allow(clippy::too_many_arguments)]
    fn py_new(
        py: Python<'_>,
        verify: bool,
        ca_bundle: Option<String>,
        cert: Option<String>,
        key: Option<String>,
        proxies: Option<Vec<(String, String)>>,
        trust_env: bool,
        http1: bool,
        http2: bool,
        http3: bool,
        max_connections: Option<usize>,
        max_keepalive_connections: Option<usize>,
        keepalive_expiry: Option<f64>,
    ) -> PyResult<Self> {
        if let Some(expiry) = keepalive_expiry {
            if !expiry.is_finite() || expiry < 0.0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "keepalive_expiry must be >= 0 or None",
                ));
            }
        }
        let options = ClientOptions {
            tls: TlsOptions {
                verify,
                ca_bundle,
                cert,
                key,
            },
            proxies: proxies.unwrap_or_default(),
            trust_env,
            http1,
            http2,
            http3,
            max_connections,
            max_keepalive_connections,
            keepalive_expiry,
        };
        // Reads certificate files; keep the interpreter free meanwhile.
        let built = py.detach(|| {
            // reqwest's builder may spawn onto the runtime (e.g. the DNS
            // resolver), so build inside the runtime context.
            let _guard = runtime().enter();
            build_inner(&options)
        });
        match built {
            Ok(inner) => Ok(HttpClient {
                inner: RwLock::new(Some(Arc::new(inner))),
                options,
            }),
            Err(err) => Err(err.into_pyerr(py)),
        }
    }

    /// Perform one request, blocking the calling thread (GIL released).
    ///
    /// `body` is `bytes`, or an iterator of `bytes` chunks that is consumed
    /// on the calling thread while the request is sent (a streamed upload).
    ///
    /// Returns `(status, http_version, headers, body, timings)`; `body` is
    /// `bytes`, or a `ResponseStream` when `stream` is true.
    #[pyo3(signature = (method, url, headers, body = None, timeout = None, stream = false))]
    #[allow(clippy::too_many_arguments)]
    fn send<'py>(
        &self,
        py: Python<'py>,
        method: &str,
        url: &str,
        headers: &Bound<'py, PyAny>,
        body: Option<&Bound<'py, PyAny>>,
        timeout: Option<&Bound<'py, Timeout>>,
        stream: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut upload = None;
        let body = match body {
            None => RequestBody::Empty,
            Some(body) => match body.extract::<PyBackedBytes>() {
                Ok(data) => RequestBody::Buffered(data),
                Err(_) => {
                    let (sender, receiver) = tokio::sync::mpsc::channel(UPLOAD_QUEUE_CHUNKS);
                    let state = Arc::new(UploadState::default());
                    upload = Some((sender, state.clone(), body.clone().unbind()));
                    RequestBody::Channel(receiver, state)
                }
            },
        };
        let prepared = match self.prepare(method, url, headers, body, timeout)? {
            Ok(prepared) => prepared,
            Err(err) => return Err(err.into_pyerr(py)),
        };
        let Some((sender, state, iterator)) = upload else {
            let result = block_on_interruptible(py, execute(prepared, stream))?;
            return Outcome(result).into_pyobject(py);
        };
        let failure = Arc::new(Mutex::new(None));
        let feeder = feed_upload(sender, state, iterator, failure.clone());
        let result = block_on_interruptible(py, execute_with_upload(prepared, stream, feeder))?;
        if let Some(err) = failure.lock().unwrap_or_else(|e| e.into_inner()).take() {
            return Err(err); // the body iterator raised: that is the real error
        }
        Outcome(result).into_pyobject(py)
    }

    /// Asynchronous form of `send()`.
    ///
    /// `body` is `bytes` or an `UploadStream` that Python feeds while the
    /// request is in flight.
    ///
    /// The request runs on the Tokio runtime and `future` (an
    /// `asyncio.Future` created on the bridge's event loop) is resolved with
    /// the same tuple `send()` returns, or with the exception. Returns a
    /// handle that can abort the request, e.g. when the awaiting task is
    /// cancelled.
    #[pyo3(signature = (bridge, future, method, url, headers, body = None, timeout = None, stream = false))]
    #[allow(clippy::too_many_arguments)]
    fn send_async<'py>(
        &self,
        py: Python<'py>,
        bridge: &Bound<'py, LoopBridge>,
        future: &Bound<'py, PyAny>,
        method: &str,
        url: &str,
        headers: &Bound<'py, PyAny>,
        body: Option<&Bound<'py, PyAny>>,
        timeout: Option<&Bound<'py, Timeout>>,
        stream: bool,
    ) -> PyResult<TaskHandle> {
        let body = match body {
            None => RequestBody::Empty,
            Some(body) => match body.cast::<UploadStream>() {
                Ok(upload) => {
                    let upload = upload.get();
                    let receiver = upload
                        .receiver
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .take()
                        .ok_or_else(|| {
                            pyo3::exceptions::PyRuntimeError::new_err(
                                "this UploadStream is already attached to a request",
                            )
                        })?;
                    RequestBody::Channel(receiver, upload.state.clone())
                }
                Err(_) => RequestBody::Buffered(body.extract()?),
            },
        };
        let prepared = match self.prepare(method, url, headers, body, timeout)? {
            Ok(prepared) => prepared,
            Err(err) => return Err(err.into_pyerr(py)),
        };
        Ok(LoopBridge::spawn(bridge, future, execute(prepared, stream)))
    }

    /// Open a WebSocket connection (awaitable).
    #[pyo3(signature = (url, headers, subprotocols = None, timeout = None, max_message_size = None, compression = false))]
    #[allow(clippy::too_many_arguments)]
    fn ws_connect<'py>(
        &self,
        py: Python<'py>,
        url: String,
        headers: Vec<(String, String)>,
        subprotocols: Option<Vec<String>>,
        timeout: Option<&Bound<'py, Timeout>>,
        max_message_size: Option<usize>,
        compression: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = match self.ws_options(
            url,
            headers,
            subprotocols,
            timeout,
            max_message_size,
            compression,
        ) {
            Ok(options) => options,
            Err(err) => return Err(err.into_pyerr(py)),
        };
        spawn_awaitable(py, crate::ws::connect(options))
    }

    /// Open a WebSocket connection, blocking the calling thread (GIL released).
    #[pyo3(signature = (url, headers, subprotocols = None, timeout = None, max_message_size = None, compression = false))]
    #[allow(clippy::too_many_arguments)]
    fn ws_connect_blocking<'py>(
        &self,
        py: Python<'py>,
        url: String,
        headers: Vec<(String, String)>,
        subprotocols: Option<Vec<String>>,
        timeout: Option<&Bound<'py, Timeout>>,
        max_message_size: Option<usize>,
        compression: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = match self.ws_options(
            url,
            headers,
            subprotocols,
            timeout,
            max_message_size,
            compression,
        ) {
            Ok(options) => options,
            Err(err) => return Err(err.into_pyerr(py)),
        };
        let result = block_on_interruptible(py, crate::ws::connect(options))?;
        Outcome(result).into_pyobject(py)
    }

    /// Drop the connection pool. In-flight requests finish; new ones fail.
    fn close(&self) {
        *self.inner.write().unwrap_or_else(|e| e.into_inner()) = None;
    }

    #[getter]
    fn is_closed(&self) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
    }
}

/// The request target (`/path?query`) exactly as it is sent on the wire for
/// `url`, i.e. after the URL parser's percent-encoding. Digest authentication
/// signs this string, so it has to match byte for byte.
#[pyfunction]
pub fn request_target(url: &str) -> PyResult<String> {
    let parsed = reqwest::Url::parse(url).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid URL {url:?}: {e}"))
    })?;
    let mut target = parsed.path().to_owned();
    if let Some(query) = parsed.query() {
        target.push('?');
        target.push_str(query);
    }
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timeouts(read: Option<u64>, write: Option<u64>) -> Timeouts {
        Timeouts {
            connect: Some(Duration::from_secs(5)),
            read: read.map(Duration::from_secs),
            write: write.map(Duration::from_secs),
            pool: None,
        }
    }

    #[test]
    fn version_strings() {
        assert_eq!(version_str(Version::HTTP_10), "HTTP/1.0");
        assert_eq!(version_str(Version::HTTP_11), "HTTP/1.1");
        assert_eq!(version_str(Version::HTTP_2), "HTTP/2");
        assert_eq!(version_str(Version::HTTP_3), "HTTP/3");
    }

    #[test]
    fn deadline_uses_read_timeout_when_there_is_no_tracked_body() {
        let ctx = RequestCtx::new(Some(Duration::from_secs(5)), false);
        let (at, kind) = ctx.deadline(&timeouts(Some(30), Some(10))).unwrap();
        assert_eq!(kind, ErrorKind::ReadTimeout);
        let remaining = at.duration_since(ctx.start);
        assert_eq!(remaining, Duration::from_secs(30));
        assert!(ctx.deadline(&timeouts(None, Some(10))).is_none());
    }

    #[test]
    fn deadline_switches_from_write_to_read_when_body_completes() {
        let ctx = RequestCtx::new(Some(Duration::from_secs(5)), true);
        let (_, kind) = ctx.deadline(&timeouts(Some(30), Some(10))).unwrap();
        assert_eq!(kind, ErrorKind::WriteTimeout);
        assert!(ctx.deadline(&timeouts(Some(30), None)).is_none());
        ctx.mark_body_done();
        let (_, kind) = ctx.deadline(&timeouts(Some(30), Some(10))).unwrap();
        assert_eq!(kind, ErrorKind::ReadTimeout);
    }

    #[test]
    fn deadline_is_extended_while_connecting() {
        let ctx = RequestCtx::new(Some(Duration::from_secs(5)), false);
        ctx.mark_connect_begin();
        let begin = Duration::from_nanos(ctx.connect_begin.load(Ordering::Relaxed));
        let (at, _) = ctx.deadline(&timeouts(Some(30), None)).unwrap();
        assert_eq!(
            at.duration_since(ctx.start),
            begin + Duration::from_secs(35)
        );
        assert!(ctx.connect_duration().is_none());

        ctx.mark_connect_end();
        let end = Duration::from_nanos(ctx.connect_end.load(Ordering::Relaxed));
        let (at, _) = ctx.deadline(&timeouts(Some(30), None)).unwrap();
        assert_eq!(at.duration_since(ctx.start), end + Duration::from_secs(30));
        assert_eq!(ctx.connect_duration(), Some(end - begin));
    }

    #[test]
    fn deadline_is_unbounded_while_connecting_without_connect_timeout() {
        let ctx = RequestCtx::new(None, false);
        ctx.mark_connect_begin();
        assert!(ctx.deadline(&timeouts(Some(30), None)).is_none());
    }

    #[test]
    fn tracked_body_reports_progress_in_chunks() {
        use http_body::Body;
        let ctx = Arc::new(RequestCtx::new(None, true));
        let total = TRACKED_BODY_CHUNK * 2 + 17;
        let mut body = TrackedBody {
            data: Bytes::from(vec![7u8; total]),
            ctx: ctx.clone(),
        };
        assert_eq!(body.size_hint().exact(), Some(total as u64));
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut seen = 0;
        let mut frames = 0;
        while let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut body).poll_frame(&mut cx) {
            seen += frame.into_data().unwrap().len();
            frames += 1;
            if seen < total {
                assert!(!ctx.body_done.load(Ordering::Relaxed));
            }
        }
        assert_eq!(seen, total);
        assert_eq!(frames, 3);
        assert!(body.is_end_stream());
        assert!(ctx.body_done.load(Ordering::Relaxed));
    }

    #[test]
    fn build_inner_validates_protocol_selection() {
        let options = ClientOptions {
            tls: TlsOptions {
                verify: true,
                ..Default::default()
            },
            proxies: Vec::new(),
            trust_env: false,
            http1: false,
            http2: false,
            http3: false,
            max_connections: Some(4),
            max_keepalive_connections: None,
            keepalive_expiry: None,
        };
        let err = build_inner(&options).err().unwrap();
        assert_eq!(err.kind, ErrorKind::LocalProtocolError);
    }
}
