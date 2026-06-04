from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path


def _seed_file_path() -> Path:
    return Path(__file__).resolve().parents[3] / "data" / "seed_urls.csv"


@lru_cache(maxsize=1)
def load_seed_urls() -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    with _seed_file_path().open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            for cell in row:
                url = cell.strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                urls.append(url)

    return urls
