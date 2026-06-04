from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.db.enums import FeedType


def queue_for_url(url: str) -> str:
    if "youtube.com" in url or "navair.navy.mil" in url:
        return "xml-large-queue"
    return "xml-small-queue"


def is_forbidden_url(url: str) -> bool:
    return "sony.com" in url or "hospitalitynet.org" in url


def task_delay_seconds(task_id: int, url: str) -> float:
    base = 0.08 + (task_id % 5) * 0.04
    if queue_for_url(url) == "xml-large-queue":
        return base + 0.15
    return base


def task_duration_ms(task_id: int) -> int:
    return 900 + (task_id % 9) * 140


def build_records(url: str, task_id: int) -> list[dict]:
    now = datetime.now(timezone.utc)
    feed_type = FeedType.ATOM if "youtube.com" in url else FeedType.RSS
    record_count = 2 + (task_id % 4)
    slug = url.split("//", 1)[-1].replace("/", "-").replace("?", "-").replace("&", "-").replace("=", "-")

    records: list[dict] = []
    for index in range(record_count):
        published_at = now - timedelta(minutes=index * 7 + (task_id % 3))
        link = f"https://{slug}/records/{task_id}-{index}"
        records.append(
            {
                "title": f"Feed item {index + 1} for task {task_id}",
                "link": link,
                "author": "Feedpulse Runtime",
                "summary": f"Synthetic record generated for {url}.",
                "published_at": published_at,
                "feed_type": feed_type,
                "dedupe_key": link,
                "extra": {
                    "source_url": url,
                    "simulated": True,
                    "record_index": index,
                },
            }
        )

    return records
