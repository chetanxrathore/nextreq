"""Bookkeeping for ``Client.download()``: the partial file and how to resume it.

A download is written to ``<name>.part`` and renamed when it is complete, so a
file that exists under its real name is always whole. Next to the partial file,
``<name>.part.json`` records the URL and the validator (``ETag`` or
``Last-Modified``) of the copy being downloaded. A later request can then ask
for the rest with ``Range`` and ``If-Range``, and the server sends the whole
file again if it changed in the meantime.
"""

from __future__ import annotations

import contextlib
import json
import os
import pathlib
import re
from collections.abc import Callable, Iterable
from typing import IO, Final, Union

from .exceptions import RemoteProtocolError
from .models import Request, Response

__all__ = ["WRITE_BATCH", "Download", "ProgressCallback"]

ProgressCallback = Callable[[int, Union[int, None]], object]
"""Called with ``(bytes_on_disk, total_bytes_or_None)`` as a download proceeds."""

WRITE_BATCH: Final = 1024 * 1024
_CONTENT_RANGE_RE: Final = re.compile(r"\s*bytes\s+(\d+)-(\d+)/(\d+|\*)\s*$", re.IGNORECASE)


def _validator(response: Response) -> str | None:
    """A value for ``If-Range``: a strong ``ETag``, or else ``Last-Modified``."""
    etag = response.headers.get("etag")
    if etag and not etag.startswith("W/"):
        return etag
    return response.headers.get("last-modified")


def _is_encoded(response: Response) -> bool:
    encoding = response.headers.get("content-encoding", "").strip().lower()
    return encoding not in ("", "identity")


class Download:
    """The state of one download across its requests (internal)."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        resume: bool,
        on_progress: ProgressCallback | None,
    ) -> None:
        self.path: Final = pathlib.Path(os.fspath(path))
        self.part: Final = self.path.with_name(self.path.name + ".part")
        self.meta: Final = self.path.with_name(self.path.name + ".part.json")
        self._resume = resume
        self._on_progress = on_progress
        self._file: IO[bytes] | None = None
        self._validator: str | None = None
        self._url: str | None = None
        #: Bytes of the file that are on disk.
        self.received = 0
        #: The size of the whole file, when the server said.
        self.total: int | None = None
        #: Whether the last request asked for a part of the file.
        self.ranged = False

    # -- before a request -------------------------------------------------

    def prepare(self, request: Request) -> None:
        """Ask for the bytes themselves, and only for those that are missing."""
        url = str(request.url)
        if self._url is None:
            self._url = url
            if self._resume:
                self._load_previous(url)
        headers = request.headers
        # A compressed transfer cannot be resumed: offsets would count
        # compressed bytes while the file holds the decoded ones.
        headers["accept-encoding"] = "identity"
        self.ranged = self.received > 0 and self._validator is not None
        if self.ranged:
            assert self._validator is not None
            headers["range"] = f"bytes={self.received}-"
            headers["if-range"] = self._validator
        else:
            self.received = 0

    def _load_previous(self, url: str) -> None:
        """Pick up a partial file that an earlier call left behind."""
        try:
            saved = json.loads(self.meta.read_text(encoding="utf-8"))
            size = self.part.stat().st_size
        except (OSError, ValueError):
            return
        if not isinstance(saved, dict) or saved.get("url") != url:
            return
        validator = saved.get("validator")
        if isinstance(validator, str) and validator and size > 0:
            self._validator = validator
            self.received = size

    def restart(self) -> None:
        """Forget the partial file (the server could not continue it)."""
        self._validator = None
        self.received = 0

    # -- during a response ------------------------------------------------

    def begin(self, response: Response) -> None:
        """Decide from the answer whether to append or start over; open the file."""
        appending = False
        self.total = None
        if self.ranged and response.status_code == 206:
            match = _CONTENT_RANGE_RE.match(response.headers.get("content-range", ""))
            if match is None or int(match.group(1)) != self.received:
                raise RemoteProtocolError(
                    f"asked for bytes from {self.received}, but the server answered with "
                    f"Content-Range {response.headers.get('content-range')!r}",
                    request=response.request,
                )
            appending = True
            if match.group(3) != "*":
                self.total = int(match.group(3))
        else:
            # A full answer: a new file, or one that changed since the part
            # was downloaded (``If-Range`` did not match).
            self.received = 0
            length = response.headers.get("content-length")
            if length is not None and length.isdigit() and not _is_encoded(response):
                self.total = int(length)
            self._validator = None if _is_encoded(response) else _validator(response)

        if self._validator is not None:
            record = {"url": self._url, "validator": self._validator}
            self.meta.write_text(json.dumps(record), encoding="utf-8")
        else:
            with contextlib.suppress(OSError):
                self.meta.unlink()
        self._file = open(self.part, "ab" if appending else "wb")  # noqa: SIM115
        self.report()

    def write(self, chunks: Iterable[bytes]) -> None:
        """Append ``chunks`` to the file (the async client calls this in a thread)."""
        assert self._file is not None
        for chunk in chunks:
            self._file.write(chunk)
            self.received += len(chunk)

    def report(self) -> None:
        """Tell the progress callback how far the download is."""
        if self._on_progress is not None:
            self._on_progress(self.received, self.total)

    # -- afterwards -------------------------------------------------------

    def interrupted(self) -> None:
        """Close the file; keep the part only if it can be continued later."""
        if self._file is not None:
            self._file.close()
            self._file = None
            if self._validator is None:
                with contextlib.suppress(OSError):
                    self.part.unlink()

    def complete(self, response: Response) -> pathlib.Path:
        """Check the size, then move the finished file to its real name."""
        assert self._file is not None
        self._file.close()
        self._file = None
        if self.total is not None and self.received != self.total:
            if self._validator is None:
                with contextlib.suppress(OSError):
                    self.part.unlink()
            raise RemoteProtocolError(
                f"the download ended after {self.received} of {self.total} bytes",
                request=response.request,
            )
        os.replace(self.part, self.path)
        with contextlib.suppress(OSError):
            self.meta.unlink()
        return self.path
