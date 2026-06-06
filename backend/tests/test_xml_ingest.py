from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.db.enums import FeedType
from app.services.xml_ingest import extract_feed_records


RSS_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>Example RSS</title>
    <item>
      <title>Show HN: A tiny feed parser</title>
      <link>https://example.com/show-hn-parser</link>
      <pubDate>Wed, 04 Jun 2026 10:15:00 GMT</pubDate>
      <dc:creator>jane_dev</dc:creator>
      <description>A 200-line RSS/Atom parser with bounded memory.</description>
      <guid>tag:example.com,2026:show-hn-parser</guid>
      <category>programming</category>
      <category>python</category>
    </item>
  </channel>
</rss>
"""

ATOM_FEED = b"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Sample Feed</title>
  <author><name>Feed Author</name></author>
  <entry>
    <title>Atom &amp; you</title>
    <link rel="alternate" href="https://blog.example.org/atom-and-you"/>
    <published>2026-06-03T22:00:00+02:00</published>
    <summary>Why Atom's date handling is stricter than RSS.</summary>
  </entry>
</feed>
"""


class XmlIngestTests(unittest.TestCase):
    def test_extract_feed_records_normalizes_rss_entry(self) -> None:
        async def fake_download(url: str, *, queue: str) -> bytes:
            self.assertEqual(url, "https://example.com/rss.xml")
            self.assertEqual(queue, "xml-small-queue")
            return RSS_FEED

        with patch("app.services.xml_ingest._download_xml", side_effect=fake_download):
            records = asyncio.run(
                extract_feed_records(
                    "https://example.com/rss.xml",
                    queue="xml-small-queue",
                )
            )

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["title"], "Show HN: A tiny feed parser")
        self.assertEqual(record["link"], "https://example.com/show-hn-parser")
        self.assertEqual(record["author"], "jane_dev")
        self.assertEqual(
            record["summary"],
            "A 200-line RSS/Atom parser with bounded memory.",
        )
        self.assertEqual(record["feed_type"], FeedType.RSS)
        self.assertEqual(record["dedupe_key"], "https://example.com/show-hn-parser")
        self.assertEqual(
            record["published_at"],
            datetime(2026, 6, 4, 10, 15, tzinfo=timezone.utc),
        )
        self.assertEqual(record["extra"]["guid"], "tag:example.com,2026:show-hn-parser")
        self.assertEqual(record["extra"]["categories"], ["programming", "python"])

    def test_extract_feed_records_uses_feed_author_fallback_for_atom(self) -> None:
        async def fake_download(url: str, *, queue: str) -> bytes:
            self.assertEqual(queue, "xml-large-queue")
            return ATOM_FEED

        with patch("app.services.xml_ingest._download_xml", side_effect=fake_download):
            records = asyncio.run(
                extract_feed_records(
                    "https://example.com/atom.xml",
                    queue="xml-large-queue",
                )
            )

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["title"], "Atom & you")
        self.assertEqual(record["link"], "https://blog.example.org/atom-and-you")
        self.assertEqual(record["author"], "Feed Author")
        self.assertEqual(
            record["summary"],
            "Why Atom's date handling is stricter than RSS.",
        )
        self.assertEqual(record["feed_type"], FeedType.ATOM)
        self.assertEqual(record["dedupe_key"], "https://blog.example.org/atom-and-you")
        self.assertEqual(
            record["published_at"],
            datetime(2026, 6, 3, 20, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(record["extra"]["link_rel"], "alternate")


class SummaryExcerptTests(unittest.TestCase):
    def test_strips_html_tags_and_unescapes(self) -> None:
        from app.services.xml_ingest import _summary_excerpt

        raw = '<img src="x.jpg" /><p>Hello &amp; welcome</p><h2>News</h2>'
        self.assertEqual(_summary_excerpt(raw), "Hello & welcome News")

    def test_collapses_whitespace(self) -> None:
        from app.services.xml_ingest import _summary_excerpt

        self.assertEqual(_summary_excerpt("a\n\n  b   c"), "a b c")

    def test_truncates_long_text_with_ellipsis(self) -> None:
        from app.services.xml_ingest import SUMMARY_MAX_CHARS, _summary_excerpt

        out = _summary_excerpt("word " * 200)
        self.assertLessEqual(len(out), SUMMARY_MAX_CHARS + 1)
        self.assertTrue(out.endswith("…"))

    def test_none_and_empty(self) -> None:
        from app.services.xml_ingest import _summary_excerpt

        self.assertIsNone(_summary_excerpt(None))
        self.assertIsNone(_summary_excerpt("<p></p>"))
