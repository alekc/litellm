"""ASGI app for `/metrics` that keeps registry rendering off the event loop.

``prometheus_client.make_asgi_app`` collects and serializes the whole registry
inline in the coroutine, so a large scrape (tens of MB on high cardinality
deployments) blocks every other request on the loop for its whole duration. This
app renders in a worker thread instead, shares one render across concurrent
identical scrapes, and streams the payload back in chunks.
"""

from __future__ import annotations

import asyncio
import gzip
from collections.abc import Iterator
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

from prometheus_client import CollectorRegistry
from prometheus_client.exposition import choose_encoder, gzip_accepted
from starlette.requests import Request
from starlette.responses import StreamingResponse
from starlette.types import ASGIApp, Receive, Scope, Send

RESPONSE_CHUNK_SIZE_BYTES: Final = 64 * 1024

_GZIP_HEADERS: Final = MappingProxyType({"Content-Encoding": "gzip"})


@dataclass(frozen=True, slots=True)
class ScrapeRequest:
    accept: str
    accept_encoding: str
    metric_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ScrapeResponse:
    content_type: str
    body: bytes
    gzipped: bool


def render_scrape(registry: CollectorRegistry, request: ScrapeRequest) -> ScrapeResponse:
    encoder, content_type = choose_encoder(request.accept)
    rendered: Final = encoder(
        registry.restricted_registry(request.metric_names) if request.metric_names else registry  # pyright: ignore[reportArgumentType]  # RestrictedRegistry is registry-shaped but not a subclass
    )
    if gzip_accepted(request.accept_encoding):
        return ScrapeResponse(content_type=content_type, body=gzip.compress(rendered), gzipped=True)
    return ScrapeResponse(content_type=content_type, body=rendered, gzipped=False)


class CoalescedScrapeRenderer:
    """Renders the registry in a worker thread, sharing one render across concurrent identical scrapes."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self._registry = registry
        self._inflight: tuple[ScrapeRequest, asyncio.Task[ScrapeResponse]] | None = None

    async def render(self, request: ScrapeRequest) -> ScrapeResponse:
        inflight: Final = self._inflight
        if inflight is not None and inflight[0] == request and not inflight[1].done():
            return await asyncio.shield(inflight[1])

        task: Final = asyncio.create_task(asyncio.to_thread(render_scrape, self._registry, request))
        self._inflight = (request, task)
        try:
            return await asyncio.shield(task)
        finally:
            if self._inflight == (request, task):
                self._inflight = None


def _chunks(body: bytes) -> Iterator[bytes]:
    return (body[start : start + RESPONSE_CHUNK_SIZE_BYTES] for start in range(0, len(body), RESPONSE_CHUNK_SIZE_BYTES))


def make_metrics_asgi_app(registry: CollectorRegistry) -> ASGIApp:
    renderer: Final = CoalescedScrapeRenderer(registry)

    async def metrics_app(scope: Scope, receive: Receive, send: Send) -> None:
        request: Final = Request(scope, receive)
        rendered: Final = await renderer.render(
            ScrapeRequest(
                accept=request.headers.get("accept", ""),
                accept_encoding=request.headers.get("accept-encoding", ""),
                metric_names=tuple(request.query_params.getlist("name[]")),
            )
        )
        response: Final = StreamingResponse(
            _chunks(rendered.body),
            media_type=rendered.content_type,
            headers=_GZIP_HEADERS if rendered.gzipped else None,
        )
        await response(scope, receive, send)

    return metrics_app
