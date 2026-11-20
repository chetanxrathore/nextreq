//! A WHATWG-compliant Server-Sent Events parser.
//!
//! Implements the "event stream interpretation" algorithm from the HTML
//! standard (<https://html.spec.whatwg.org/multipage/server-sent-events.html>):
//!
//! * lines end with CRLF, LF or a lone CR (including a CRLF split across chunks)
//! * one leading UTF-8 byte order mark is ignored
//! * lines starting with `:` are comments
//! * only one space after the field colon is stripped
//! * `data` fields accumulate, joined by `\n`
//! * `id` fields containing NUL are ignored, and the last event ID persists
//! * `retry` is only honoured when it is all ASCII digits
//! * an event with an empty data buffer is not dispatched
//! * an event that is not terminated by a blank line is discarded at EOF
//!
//! The decoder is incremental and allocation-light: complete lines inside a
//! chunk are parsed in place, and only a trailing partial line is buffered.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

/// One dispatched event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: String,
    pub data: String,
    pub id: Option<String>,
    pub retry: Option<u64>,
}

/// Incremental event-stream decoder.
#[derive(Debug, Default)]
pub struct SseDecoder {
    /// Bytes of a line that has not been terminated yet.
    partial: Vec<u8>,
    /// The previous chunk ended with CR; a leading LF must be swallowed.
    pending_cr: bool,
    /// Whether the first line (which may carry a BOM) has been seen.
    started: bool,
    data: String,
    has_data: bool,
    event: String,
    block_retry: Option<u64>,
    last_event_id: Option<String>,
    reconnection_time: Option<u64>,
}

impl SseDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// The last event ID seen on the stream (for `Last-Event-ID` on reconnect).
    pub fn last_event_id(&self) -> Option<&str> {
        self.last_event_id.as_deref()
    }

    /// The most recent `retry:` value, in milliseconds.
    pub fn reconnection_time(&self) -> Option<u64> {
        self.reconnection_time
    }

    /// Feed a chunk of the response body, appending dispatched events to `out`.
    pub fn feed(&mut self, mut chunk: &[u8], out: &mut Vec<SseEvent>) {
        if self.pending_cr {
            if chunk.is_empty() {
                return;
            }
            self.pending_cr = false;
            if chunk[0] == b'\n' {
                chunk = &chunk[1..];
            }
        }
        while !chunk.is_empty() {
            let Some(pos) = chunk.iter().position(|&b| b == b'\n' || b == b'\r') else {
                self.partial.extend_from_slice(chunk);
                return;
            };
            let terminator = chunk[pos];
            if self.partial.is_empty() {
                self.process_line(&chunk[..pos], out);
            } else {
                self.partial.extend_from_slice(&chunk[..pos]);
                let line = std::mem::take(&mut self.partial);
                self.process_line(&line, out);
                // Reuse the allocation for the next partial line.
                self.partial = line;
                self.partial.clear();
            }
            chunk = &chunk[pos + 1..];
            if terminator == b'\r' {
                match chunk.first() {
                    Some(b'\n') => chunk = &chunk[1..],
                    Some(_) => {}
                    None => self.pending_cr = true,
                }
            }
        }
    }

    /// Signal end of stream. Per the specification any incomplete event is
    /// discarded, so this only resets the buffers.
    pub fn finish(&mut self) {
        self.partial.clear();
        self.pending_cr = false;
        self.data.clear();
        self.has_data = false;
        self.event.clear();
        self.block_retry = None;
    }

    fn process_line(&mut self, mut line: &[u8], out: &mut Vec<SseEvent>) {
        if !self.started {
            self.started = true;
            if line.starts_with(&[0xEF, 0xBB, 0xBF]) {
                line = &line[3..];
            }
        }
        if line.is_empty() {
            self.dispatch(out);
            return;
        }
        if line[0] == b':' {
            return; // comment
        }
        let (field, value) = match line.iter().position(|&b| b == b':') {
            Some(colon) => {
                let mut value = &line[colon + 1..];
                if value.first() == Some(&b' ') {
                    value = &value[1..];
                }
                (&line[..colon], value)
            }
            None => (line, &[][..]),
        };
        match field {
            b"data" => {
                self.data.push_str(&String::from_utf8_lossy(value));
                self.data.push('\n');
                self.has_data = true;
            }
            b"event" => {
                self.event.clear();
                self.event.push_str(&String::from_utf8_lossy(value));
            }
            b"id" => {
                if !value.contains(&0) {
                    self.last_event_id = if value.is_empty() {
                        None
                    } else {
                        Some(String::from_utf8_lossy(value).into_owned())
                    };
                }
            }
            b"retry" if !value.is_empty() && value.iter().all(u8::is_ascii_digit) => {
                // Values too large for a u64 are ignored rather than wrapped.
                if let Some(ms) = std::str::from_utf8(value)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    self.reconnection_time = Some(ms);
                    self.block_retry = Some(ms);
                }
            }
            _ => {} // unknown fields are ignored
        }
    }

    fn dispatch(&mut self, out: &mut Vec<SseEvent>) {
        let retry = self.block_retry.take();
        if !self.has_data {
            self.event.clear();
            return;
        }
        // Strip the single trailing LF added after the last data line.
        if self.data.ends_with('\n') {
            self.data.pop();
        }
        let event = if self.event.is_empty() {
            String::from("message")
        } else {
            std::mem::take(&mut self.event)
        };
        out.push(SseEvent {
            event,
            data: std::mem::take(&mut self.data),
            id: self.last_event_id.clone(),
            retry,
        });
        self.has_data = false;
    }
}

/// Convert parsed events into a Python `list[tuple[str, str, str | None, int | None]]`.
pub fn events_to_py<'py>(py: Python<'py>, events: Vec<SseEvent>) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for ev in events {
        let tuple = PyTuple::new(
            py,
            [
                ev.event.into_pyobject(py)?.into_any(),
                ev.data.into_pyobject(py)?.into_any(),
                ev.id.into_pyobject(py)?.into_any(),
                ev.retry.into_pyobject(py)?.into_any(),
            ],
        )?;
        list.append(tuple)?;
    }
    Ok(list)
}

/// Incremental Server-Sent Events parser exposed to Python.
///
/// `feed(data)` returns a list of `(event, data, id, retry)` tuples for every
/// event completed by that chunk.
#[pyclass(frozen, module = "nextreq._nextreq", name = "SSEParser")]
pub struct SSEParser {
    inner: Mutex<SseDecoder>,
}

#[pymethods]
impl SSEParser {
    #[new]
    fn new() -> Self {
        SSEParser {
            inner: Mutex::new(SseDecoder::new()),
        }
    }

    /// Feed raw bytes; returns the events completed by this chunk.
    fn feed<'py>(&self, py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyList>> {
        let mut out = Vec::new();
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .feed(data, &mut out);
        events_to_py(py, out)
    }

    /// Mark end of stream, discarding any unterminated event.
    fn finish(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .finish();
    }

    /// The last event ID seen so far, if any.
    #[getter]
    fn last_event_id(&self) -> Option<String> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last_event_id()
            .map(str::to_owned)
    }

    /// The most recent `retry:` reconnection time in milliseconds, if any.
    #[getter]
    fn retry(&self) -> Option<u64> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reconnection_time()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(chunks: &[&[u8]]) -> Vec<SseEvent> {
        let mut decoder = SseDecoder::new();
        let mut out = Vec::new();
        for chunk in chunks {
            decoder.feed(chunk, &mut out);
        }
        decoder.finish();
        out
    }

    fn ev(event: &str, data: &str, id: Option<&str>, retry: Option<u64>) -> SseEvent {
        SseEvent {
            event: event.to_owned(),
            data: data.to_owned(),
            id: id.map(str::to_owned),
            retry,
        }
    }

    #[test]
    fn simple_message() {
        assert_eq!(
            parse(&[b"data: hello\n\n"]),
            vec![ev("message", "hello", None, None)]
        );
    }

    #[test]
    fn multi_line_data_is_joined_with_lf() {
        assert_eq!(
            parse(&[b"data: YHOO\ndata: +2\ndata: 10\n\n"]),
            vec![ev("message", "YHOO\n+2\n10", None, None)]
        );
    }

    #[test]
    fn named_event_id_and_retry() {
        assert_eq!(
            parse(&[b"event: update\nid: 42\nretry: 1500\ndata: {}\n\n"]),
            vec![ev("update", "{}", Some("42"), Some(1500))]
        );
    }

    #[test]
    fn all_three_line_endings() {
        let expected = vec![
            ev("message", "a", None, None),
            ev("message", "b", None, None),
        ];
        assert_eq!(parse(&[b"data: a\n\ndata: b\n\n"]), expected);
        assert_eq!(parse(&[b"data: a\r\n\r\ndata: b\r\n\r\n"]), expected);
        assert_eq!(parse(&[b"data: a\r\rdata: b\r\r"]), expected);
    }

    #[test]
    fn crlf_split_across_chunks_is_one_line_ending() {
        assert_eq!(
            parse(&[b"data: a\r", b"\n\r", b"\ndata: b\r\n\r\n"]),
            vec![
                ev("message", "a", None, None),
                ev("message", "b", None, None)
            ]
        );
    }

    #[test]
    fn arbitrary_chunk_boundaries() {
        let stream = b"event: tick\nid: 7\ndata: first\ndata: second\n\n: comment\ndata: x\n\n";
        let expected = vec![
            ev("tick", "first\nsecond", Some("7"), None),
            ev("message", "x", Some("7"), None),
        ];
        for split in 0..stream.len() {
            let (a, b) = stream.split_at(split);
            assert_eq!(parse(&[a, b]), expected, "split at {split}");
        }
        let bytes: Vec<&[u8]> = stream.chunks(1).collect();
        assert_eq!(parse(&bytes), expected);
    }

    #[test]
    fn comments_and_unknown_fields_are_ignored() {
        assert_eq!(
            parse(&[b": keep-alive\nfoo: bar\ndata: ok\n\n"]),
            vec![ev("message", "ok", None, None)]
        );
    }

    #[test]
    fn only_one_leading_space_is_stripped() {
        assert_eq!(
            parse(&[b"data:  two spaces\n\ndata:none\n\n"]),
            vec![
                ev("message", " two spaces", None, None),
                ev("message", "none", None, None)
            ]
        );
    }

    #[test]
    fn field_without_colon_has_empty_value() {
        // `data` alone appends an empty line; two of them give "\n".
        assert_eq!(
            parse(&[b"data\n\ndata\ndata\n\n"]),
            vec![
                ev("message", "", None, None),
                ev("message", "\n", None, None)
            ]
        );
    }

    #[test]
    fn event_without_data_is_not_dispatched() {
        assert_eq!(
            parse(&[b"event: ping\n\ndata: real\n\n"]),
            vec![ev("message", "real", None, None)]
        );
    }

    #[test]
    fn last_event_id_persists_and_can_be_reset() {
        assert_eq!(
            parse(&[b"id: 1\ndata: a\n\ndata: b\n\nid\ndata: c\n\n"]),
            vec![
                ev("message", "a", Some("1"), None),
                ev("message", "b", Some("1"), None),
                ev("message", "c", None, None),
            ]
        );
    }

    #[test]
    fn id_with_nul_is_ignored() {
        assert_eq!(
            parse(&[b"id: 1\ndata: a\n\nid: b\0d\ndata: b\n\n"]),
            vec![
                ev("message", "a", Some("1"), None),
                ev("message", "b", Some("1"), None)
            ]
        );
    }

    #[test]
    fn retry_must_be_all_digits() {
        let mut decoder = SseDecoder::new();
        let mut out = Vec::new();
        decoder.feed(b"retry: 10x\ndata: a\n\nretry: 250\ndata: b\n\n", &mut out);
        assert_eq!(
            out,
            vec![
                ev("message", "a", None, None),
                ev("message", "b", None, Some(250))
            ]
        );
        assert_eq!(decoder.reconnection_time(), Some(250));
    }

    #[test]
    fn retry_only_block_updates_reconnection_time_without_dispatch() {
        let mut decoder = SseDecoder::new();
        let mut out = Vec::new();
        decoder.feed(b"retry: 3000\n\ndata: a\n\n", &mut out);
        assert_eq!(out, vec![ev("message", "a", None, None)]);
        assert_eq!(decoder.reconnection_time(), Some(3000));
    }

    #[test]
    fn leading_bom_is_stripped_once() {
        assert_eq!(
            parse(&[b"\xEF\xBB\xBFdata: a\n\n"]),
            vec![ev("message", "a", None, None)]
        );
        // A BOM split across chunks is still handled because it is examined
        // only once the first full line is assembled.
        assert_eq!(
            parse(&[b"\xEF\xBB", b"\xBFdata: a\n\n"]),
            vec![ev("message", "a", None, None)]
        );
    }

    #[test]
    fn unterminated_event_is_discarded_at_eof() {
        assert_eq!(
            parse(&[b"data: complete\n\ndata: incomplete\n"]),
            vec![ev("message", "complete", None, None)]
        );
    }

    #[test]
    fn invalid_utf8_is_replaced() {
        let out = parse(&[b"data: \xFF\xFEok\n\n"]);
        assert_eq!(out.len(), 1);
        assert!(out[0].data.ends_with("ok"));
        assert!(out[0].data.contains('\u{FFFD}'));
    }

    #[test]
    fn multibyte_utf8_split_across_chunks() {
        let stream = "data: h\u{e9}llo \u{1F600}\n\n".as_bytes();
        for split in 0..stream.len() {
            let (a, b) = stream.split_at(split);
            assert_eq!(
                parse(&[a, b]),
                vec![ev("message", "h\u{e9}llo \u{1F600}", None, None)],
                "split at {split}"
            );
        }
    }

    #[test]
    fn many_events_in_one_chunk() {
        let mut stream = Vec::new();
        for i in 0..1000 {
            stream.extend_from_slice(format!("id: {i}\ndata: payload-{i}\n\n").as_bytes());
        }
        let out = parse(&[&stream]);
        assert_eq!(out.len(), 1000);
        assert_eq!(out[999], ev("message", "payload-999", Some("999"), None));
    }
}
