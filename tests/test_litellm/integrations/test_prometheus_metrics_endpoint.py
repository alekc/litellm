"""The /metrics app must render off the event loop, coalesce concurrent scrapes and stream chunks."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Final

import httpx
import pytest
from prometheus_client import CollectorRegistry, Gauge
from prometheus_client.metrics_core import GaugeMetricFamily

from litellm.integrations.prometheus_metrics_endpoint import (
    RESPONSE_CHUNK_SIZE_BYTES,
    make_metrics_asgi_app,
)


class _SlowCollector:
    """Blocking collector standing in for a large registry render."""

    def __init__(self, block_seconds: float, sample_count: int = 1) -> None:
        self.block_seconds = block_seconds
        self.sample_count = sample_count
        self.collect_calls = 0

    def collect(self) -> Iterator[GaugeMetricFamily]:
        self.collect_calls += 1
        time.sleep(self.block_seconds)
        family: Final = GaugeMetricFamily("slow_metric", "slow", labels=("idx",))
        for idx in range(self.sample_count):
            family.add_metric((str(idx),), 1.0)
        yield family


def _client(registry: CollectorRegistry) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=make_metrics_asgi_app(registry)),
        base_url="http://metrics.test",
    )


def _registry_with(collector: _SlowCollector) -> CollectorRegistry:
    registry: Final = CollectorRegistry()
    registry.register(collector)
    return registry


async def _scrape(client: httpx.AsyncClient, headers: Mapping[str, str] | None = None) -> httpx.Response:
    return await client.get("/metrics", headers=headers)


@pytest.mark.asyncio
async def test_render_does_not_block_the_event_loop():
    ticks: Final[list[float]] = []  # mutable-ok: records loop wakeups while the scrape is in flight

    async def ticker() -> None:
        while True:
            await asyncio.sleep(0.01)
            ticks.append(time.monotonic())

    ticker_task: Final = asyncio.create_task(ticker())
    async with _client(_registry_with(_SlowCollector(block_seconds=0.5))) as client:
        try:
            response: Final = await _scrape(client)
        finally:
            ticker_task.cancel()

    assert b"slow_metric" in response.content
    assert len(ticks) > 5, "event loop was blocked while the registry was rendered"


@pytest.mark.asyncio
async def test_concurrent_identical_scrapes_share_one_render():
    collector: Final = _SlowCollector(block_seconds=0.2)
    async with _client(_registry_with(collector)) as client:
        responses: Final[Sequence[httpx.Response]] = await asyncio.gather(*(_scrape(client) for _ in range(5)))

    assert collector.collect_calls == 1
    for response in responses:
        assert b"slow_metric" in response.content


@pytest.mark.asyncio
async def test_sequential_scrapes_are_rendered_fresh():
    collector: Final = _SlowCollector(block_seconds=0.0)
    async with _client(_registry_with(collector)) as client:
        await _scrape(client)
        await _scrape(client)

    assert collector.collect_calls == 2


@pytest.mark.asyncio
async def test_gzip_is_used_when_the_scraper_accepts_it():
    registry: Final = CollectorRegistry()
    Gauge("plain_metric", "plain", registry=registry).set(1)

    async with _client(registry) as client:
        compressed: Final = await _scrape(client, headers={"accept-encoding": "gzip"})
        plain: Final = await _scrape(client, headers={"accept-encoding": "identity"})

    assert compressed.headers["content-encoding"] == "gzip"
    assert "content-encoding" not in plain.headers
    assert compressed.content == plain.content
    assert b"plain_metric" in plain.content


@pytest.mark.asyncio
async def test_name_filter_restricts_the_rendered_registry():
    registry: Final = CollectorRegistry()
    Gauge("wanted_metric", "wanted", registry=registry).set(1)
    Gauge("other_metric", "other", registry=registry).set(1)

    async with _client(registry) as client:
        response: Final = await client.get("/metrics", params={"name[]": "wanted_metric"})

    assert b"wanted_metric" in response.content
    assert b"other_metric" not in response.content


@pytest.mark.asyncio
async def test_large_payload_is_streamed_in_chunks():
    registry: Final = _registry_with(_SlowCollector(block_seconds=0.0, sample_count=5000))
    chunk_sizes: Final[list[int]] = []  # mutable-ok: records the ASGI body parts the app emitted

    async def send(message: Mapping[str, object]) -> None:
        if message["type"] == "http.response.body":
            body = message["body"]
            assert isinstance(body, bytes)
            chunk_sizes.append(len(body))

    incoming: Final = iter(({"type": "http.request", "body": b"", "more_body": False},))

    async def receive() -> Mapping[str, object]:
        request: Final = next(incoming, None)
        if request is not None:
            return request
        await asyncio.Event().wait()
        return {"type": "http.disconnect"}

    app: Final = make_metrics_asgi_app(registry)
    await app(
        {
            "type": "http",
            "method": "GET",
            "path": "/metrics",
            "headers": (),
            "query_string": b"",
        },
        receive,
        send,
    )

    assert sum(chunk_sizes) > RESPONSE_CHUNK_SIZE_BYTES
    assert len(chunk_sizes) > 2
    assert max(chunk_sizes) <= RESPONSE_CHUNK_SIZE_BYTES
