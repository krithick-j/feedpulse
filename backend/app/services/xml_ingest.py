from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any
from xml.etree.ElementTree import ParseError

import feedparser
import httpx
from defusedxml import DefusedXmlException
from defusedxml import ElementTree

from app.core.logging import log_event
from app.db.enums import FeedType

SMALL_RESPONSE_LIMIT_BYTES = 5 * 1024 * 1024
LARGE_RESPONSE_LIMIT_BYTES = 24 * 1024 * 1024
REQUEST_TIMEOUT = httpx.Timeout(20.0, connect=5.0, read=20.0, write=20.0, pool=5.0)
REQUEST_HEADERS = {
    "Accept": "application/atom+xml, application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.1",
    "User-Agent": "Feedpulse/0.1 (+https://localhost/feedpulse)",
}
logger = logging.getLogger(__name__)


class XmlIngestError(Exception):
    """Base class for XML fetch and parse failures."""


class FeedFetchError(XmlIngestError):
    """Network or server-side fetch failure."""


class HttpClientError(XmlIngestError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class OversizedResponseError(XmlIngestError):
    pass


class MalformedXmlError(XmlIngestError):
    pass


async def extract_feed_records(url: str, *, queue: str) -> list[dict[str, Any]]:
    body = await _download_xml(url, queue=queue)
    await asyncio.to_thread(_preflight_xml, body)
    parsed = await asyncio.to_thread(feedparser.parse, body)

    feed_type = _feed_type_from_version(parsed.version)
    feed_author = _normalize_string(parsed.feed.get("author")) if hasattr(parsed, "feed") else None
    records = [
        _normalize_entry(entry, source_url=url, feed_type=feed_type, fallback_author=feed_author)
        for entry in parsed.entries
    ]
    log_event(
        logger,
        logging.INFO,
        "xml.records.extracted",
        url=url,
        queue=queue,
        bytes_downloaded=len(body),
        record_count=len(records),
        feed_type=feed_type,
    )
    return records


async def _download_xml(url: str, *, queue: str) -> bytes:
    max_bytes = _max_response_bytes(queue)
    async with httpx.AsyncClient(
        follow_redirects=True,
        headers=REQUEST_HEADERS,
        timeout=REQUEST_TIMEOUT,
    ) as client:
        try:
            async with client.stream("GET", url) as response:
                if response.status_code == 429:
                    log_event(
                        logger,
                        logging.WARNING,
                        "xml.download.retryable_status",
                        url=url,
                        queue=queue,
                        status_code=response.status_code,
                    )
                    raise FeedFetchError("429 response while fetching feed")
                if 400 <= response.status_code < 500:
                    log_event(
                        logger,
                        logging.WARNING,
                        "xml.download.permanent_status",
                        url=url,
                        queue=queue,
                        status_code=response.status_code,
                    )
                    raise HttpClientError(
                        response.status_code,
                        f"{response.status_code} response while fetching feed",
                    )
                if response.status_code >= 500:
                    log_event(
                        logger,
                        logging.WARNING,
                        "xml.download.retryable_status",
                        url=url,
                        queue=queue,
                        status_code=response.status_code,
                    )
                    raise FeedFetchError(
                        f"{response.status_code} response while fetching feed",
                    )

                declared_length = response.headers.get("content-length")
                if declared_length is not None and int(declared_length) > max_bytes:
                    log_event(
                        logger,
                        logging.WARNING,
                        "xml.download.oversized",
                        url=url,
                        queue=queue,
                        status_code=response.status_code,
                        declared_length=int(declared_length),
                        max_bytes=max_bytes,
                    )
                    raise OversizedResponseError(
                        f"Response exceeded {max_bytes} bytes before download"
                    )

                chunks = bytearray()
                async for chunk in response.aiter_bytes():
                    chunks.extend(chunk)
                    if len(chunks) > max_bytes:
                        log_event(
                            logger,
                            logging.WARNING,
                            "xml.download.oversized",
                            url=url,
                            queue=queue,
                            status_code=response.status_code,
                            bytes_downloaded=len(chunks),
                            max_bytes=max_bytes,
                        )
                        raise OversizedResponseError(
                            f"Response exceeded {max_bytes} bytes while streaming"
                        )

                log_event(
                    logger,
                    logging.INFO,
                    "xml.download.completed",
                    url=url,
                    queue=queue,
                    status_code=response.status_code,
                    bytes_downloaded=len(chunks),
                )
                return bytes(chunks)
        except HttpClientError:
            raise
        except httpx.TimeoutException as exc:
            log_event(
                logger,
                logging.WARNING,
                "xml.download.timeout",
                url=url,
                queue=queue,
            )
            raise FeedFetchError("Timeout while fetching feed") from exc
        except httpx.HTTPError as exc:
            log_event(
                logger,
                logging.WARNING,
                "xml.download.http_error",
                url=url,
                queue=queue,
                error_message=str(exc) or "HTTP transport error while fetching feed",
            )
            raise FeedFetchError(str(exc) or "HTTP transport error while fetching feed") from exc


def _preflight_xml(body: bytes) -> None:
    try:
        ElementTree.fromstring(
            body,
            forbid_dtd=False,
            forbid_entities=True,
            forbid_external=True,
        )
    except (DefusedXmlException, ParseError) as exc:
        raise MalformedXmlError(str(exc) or "Malformed XML payload") from exc


def _normalize_entry(
    entry,
    *,
    source_url: str,
    feed_type: FeedType,
    fallback_author: str | None,
) -> dict[str, Any]:
    title = _normalize_string(entry.get("title"))
    chosen_link = _choose_link(entry)
    link = _normalize_string(chosen_link.get("href")) if chosen_link else _normalize_string(entry.get("link"))
    author = (
        _normalize_string(entry.get("author"))
        or _normalize_string(entry.get("dc_creator"))
        or fallback_author
    )
    summary = (
        _normalize_string(entry.get("summary"))
        or _normalize_string(entry.get("description"))
        or _normalize_string(entry.get("subtitle"))
    )
    published_at = _to_datetime(
        entry.get("published_parsed")
        or entry.get("updated_parsed")
        or entry.get("created_parsed")
    )

    extra: dict[str, Any] = {"source_url": source_url}
    guid = _normalize_string(entry.get("id") or entry.get("guid"))
    if guid and guid != link:
        extra["guid"] = guid

    categories = _entry_categories(entry)
    if categories:
        extra["categories"] = categories

    content_html = _entry_content_html(entry)
    if content_html:
        extra["content_html"] = content_html

    author_detail = entry.get("author_detail") or {}
    author_email = _normalize_string(author_detail.get("email")) if isinstance(author_detail, dict) else None
    if author_email:
        extra["author_email"] = author_email

    if chosen_link is not None:
        link_rel = _normalize_string(chosen_link.get("rel"))
        if link_rel:
            extra["link_rel"] = link_rel

    return {
        "title": title,
        "link": link,
        "author": author,
        "summary": summary,
        "published_at": published_at,
        "feed_type": feed_type,
        "dedupe_key": _dedupe_key(
            link=link,
            title=title,
            author=author,
            published_at=published_at,
            source_url=source_url,
        ),
        "extra": extra,
    }


def _choose_link(entry) -> dict[str, Any] | None:
    links = entry.get("links") or []
    for candidate in links:
        if not isinstance(candidate, dict):
            continue
        rel = candidate.get("rel")
        href = candidate.get("href")
        if href and (rel in (None, "", "alternate")):
            return candidate
    for candidate in links:
        if isinstance(candidate, dict) and candidate.get("href"):
            return candidate
    return None


def _entry_categories(entry) -> list[str]:
    values: list[str] = []
    for tag in entry.get("tags") or []:
        if not isinstance(tag, dict):
            continue
        term = _normalize_string(tag.get("term"))
        if term and term not in values:
            values.append(term)
    return values


def _entry_content_html(entry) -> str | None:
    content = entry.get("content") or []
    if not content:
        return None
    first = content[0]
    if not isinstance(first, dict):
        return None
    return _normalize_string(first.get("value"))


def _dedupe_key(
    *,
    link: str | None,
    title: str | None,
    author: str | None,
    published_at: datetime | None,
    source_url: str,
) -> str:
    if link:
        return link

    digest = hashlib.sha256(
        "|".join(
            [
                source_url,
                title or "",
                author or "",
                published_at.isoformat() if published_at else "",
            ]
        ).encode("utf-8")
    ).hexdigest()
    return f"generated:{digest}"


def _feed_type_from_version(version: str | None) -> FeedType:
    if not version:
        return FeedType.UNKNOWN
    if version.startswith("atom"):
        return FeedType.ATOM
    if version.startswith("rss1"):
        return FeedType.RDF
    if version.startswith("rss"):
        return FeedType.RSS
    return FeedType.UNKNOWN


def _max_response_bytes(queue: str) -> int:
    if queue == "xml-large-queue":
        return LARGE_RESPONSE_LIMIT_BYTES
    return SMALL_RESPONSE_LIMIT_BYTES


def _normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return " ".join(text.split())


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if not isinstance(value, tuple) or len(value) < 6:
        return None
    return datetime(
        value[0],
        value[1],
        value[2],
        value[3],
        value[4],
        value[5],
        tzinfo=timezone.utc,
    )
