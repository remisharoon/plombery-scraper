"""Kochi pre-launch & new-launch property scraper pipeline.

Discovers, enriches, standardizes and indexes pre-launch and new-launch
residential projects (apartments, villas) in Kochi, Kerala from multiple
property portals and search engines, into Elasticsearch.

Two-level discovery:
  Level 1: Find listing pages via DuckDuckGo + portal homepages
  Level 2: Visit each listing page to extract individual project URLs
  Level 3: Visit each project detail page to extract structured data
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, List, Optional
from urllib.parse import quote_plus, unquote, urljoin

import numpy as np
import pandas as pd
import requests

try:
    from curl_cffi import requests as curl_requests
except Exception:
    curl_requests = None

from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
from pydantic import BaseModel
from plombery import Trigger, register_pipeline, task

from config import read_config


logger = logging.getLogger(__name__)

GEMINI_V2_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
GEMINI_V1_5_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
MAX_RETRIES_V2 = 3
MAX_RETRIES_V1_5 = 3

PRICE_MULTIPLIERS = {
    "cr": 10_000_000,
    "crore": 10_000_000,
    "lac": 100_000,
    "lakh": 100_000,
    "l": 100_000,
    "k": 1_000,
    "m": 1_000_000,
}

BLOCKED_MARKERS = (
    "captcha",
    "verify you are a human",
    "access denied",
    "unusual traffic",
    "blocked",
    "are you a robot",
    "please verify",
)

MODERN_USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.78 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.60 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.60 Safari/537.36",
)

IMPERSONATE_IDS = ("chrome110", "chrome120", "chrome124", "chrome131")

REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

MANDATORY_FIELDS = [
    "id", "project_name", "builder_name", "launch_status",
    "city", "state", "source", "discovered_at", "updated_at",
]

STANDARD_SCHEMA_FIELDS = [
    "id", "project_name", "builder_name", "builder_tier",
    "property_types", "launch_status",
    "price_min", "price_max", "price_currency", "price_per_sqft",
    "configurations", "total_units", "total_towers", "total_floors",
    "project_size_acres",
    "carpet_area_min_sqft", "carpet_area_max_sqft",
    "super_area_min_sqft", "super_area_max_sqft",
    "locality", "city", "district", "state",
    "latitude", "longitude",
    "possession_date", "possession_quarter",
    "rera_number", "rera_status",
    "amenities", "floor_plans",
    "project_description", "project_highlights",
    "images", "brochure_url",
    "project_url", "builder_url", "source_url", "source",
    "discovered_at", "updated_at",
]

KNOWN_PREMIUM_BUILDERS = {
    "prestige", "sobha", "puravankara", "godrej", "brigade", "prestige group",
    "sobha limited", "puravankara limited", "godrej properties", "brigade group",
    "asset homes", "heaven grand", "kalyan developers", "travancore foundation",
}

KNOWN_MID_SEGMENT_BUILDERS = {
    "artech", "desai homes", "eram builders", "malabar developers", "penta",
    "white rose", "sreelakam", "navkar builders", "benz developers",
}

BUILDER_TIER_PROMPT = """You are a real estate expert specializing in Kerala, India.
Classify the builder into one of these tiers based on their name, reputation, and the project details:
- "luxury": Ultra-premium, 5-star amenities, premium locations, ₹1.5 Cr+ apartments / ₹3 Cr+ villas
- "premium": Well-known national/state builders, good amenities, ₹80L-1.5 Cr apartments / ₹1.5-3 Cr villas
- "mid-segment": Established local builders, decent amenities, ₹40-80L apartments / ₹80L-1.5 Cr villas
- "affordable": Budget builders, basic amenities, under ₹40L apartments / under ₹80L villas

Return ONLY a single JSON object: {"builder_tier": "luxury"|"premium"|"mid-segment"|"affordable", "confidence": "high"|"medium"|"low"}"""

PROJECT_EXTRACT_PROMPT = """You are a real-estate data extraction engine.
Extract project details from the provided text into a JSON object with EXACTLY these keys.
If a value is missing use null. Lists must be arrays of strings.
Escape newlines inside strings as \\n. No markdown, no code fences, no extra text.

Keys:
- project_name (string)
- builder_name (string)
- property_types (list: e.g. ["apartment","villa"])
- launch_status (string: "pre-launch"|"new-launch"|"under-construction"|"ready-to-move")
- price_min (integer, INR)
- price_max (integer, INR)
- price_per_sqft (integer, INR per sqft)
- configurations (list: e.g. ["2BHK","3BHK"])
- total_units (integer)
- total_towers (integer)
- total_floors (integer)
- project_size_acres (float)
- carpet_area_min_sqft (float)
- carpet_area_max_sqft (float)
- super_area_min_sqft (float)
- super_area_max_sqft (float)
- locality (string)
- district (string)
- latitude (float)
- longitude (float)
- possession_date (string: YYYY-MM-DD or YYYY-MM or null)
- possession_quarter (string: e.g. "Q1 2026")
- rera_number (string)
- rera_status (string)
- amenities (list of strings)
- project_description (string)
- project_highlights (list of strings)
- brochure_url (string)
- builder_url (string)"""

try:
    import json_repair
except Exception:
    json_repair = None

try:
    import dirtyjson
except Exception:
    dirtyjson = None

try:
    import rapidjson
except Exception:
    rapidjson = None


config = read_config()

CONFIG_SECTION = "kochi_launches"
kochi_section = config[CONFIG_SECTION]
if not kochi_section:
    raise KeyError("Missing [kochi_launches] section in config.ini")

_es_config = config["elasticsearch"]
_gemini_config = config["GeminiPro"]

GEMINI_API_KEY = random.choice([_gemini_config["API_KEY_RH"], _gemini_config["API_KEY_RHA"]])
GEMINI_HEADERS = {"Content-Type": "application/json", "X-goog-api-key": GEMINI_API_KEY}
GEMINI_PARAMS = {"key": GEMINI_API_KEY}

DUCKDUCKGO_QUERIES = [q.strip() for q in kochi_section.get("duckduckgo_queries", "pre launch apartments kochi").split(",") if q.strip()]
SIGNATURE_DWELLINGS_URL = kochi_section.get("signature_dwellings_url", "https://signaturedwellingsprojects.com/kochi/")
PRESTIGE_PRELAUNCH_KOCHI_URL = kochi_section.get("prestige_prelaunch_kochi_url", "https://prestigeprelaunchprojects.com/kochi/")
PRESTIGE_CITYSCAPE_URL = kochi_section.get("prestige_cityscape_url", "https://prestigeprelaunchprojects.com/kochi/prestige-cityscape-kundannoor/")
REALESTATEINDIA_URL = kochi_section.get("realestateindia_url", "https://www.realestateindia.com/kochi-property/new-projects.htm")
REALESTATEINDIA_LOCALITIES = [l.strip() for l in kochi_section.get("realestateindia_localities", "").split(",") if l.strip()]
PAGES = int(kochi_section.get("pages", "10"))
DUCKDUCKGO_PAGES = int(kochi_section.get("duckduckgo_pages", "3"))
MIN_DELAY = float(kochi_section.get("min_delay_seconds", "2.0"))
MAX_DELAY = float(kochi_section.get("max_delay_seconds", "6.0"))
DETAIL_RETRY = int(kochi_section.get("detail_retry_count", "3"))
REQUEST_TIMEOUT = int(kochi_section.get("request_timeout_seconds", "30"))
ES_INDEX = kochi_section.get("es_index", "kochi_property_launches")

es_hosts = []
for h in _es_config["host"].split(","):
    h = h.strip()
    if not h:
        continue
    if not h.startswith("http"):
        h = f"http://{h}"
    if ":" not in h.split("//")[-1]:
        h += ":9200"
    es_hosts.append(h)
es_user = _es_config["username"]
es_password = _es_config["password"]


@dataclass(slots=True)
class KochiLaunchSettings:
    duckduckgo_queries: list[str]
    signature_dwellings_url: str
    prestige_prelaunch_kochi_url: str
    prestige_cityscape_url: str
    realestateindia_url: str
    realestateindia_localities: list[str]
    pages: int
    duckduckgo_pages: int
    min_delay: float
    max_delay: float
    detail_retry: int
    request_timeout: int
    es_index: str


SETTINGS = KochiLaunchSettings(
    duckduckgo_queries=DUCKDUCKGO_QUERIES,
    signature_dwellings_url=SIGNATURE_DWELLINGS_URL,
    prestige_prelaunch_kochi_url=PRESTIGE_PRELAUNCH_KOCHI_URL,
    prestige_cityscape_url=PRESTIGE_CITYSCAPE_URL,
    realestateindia_url=REALESTATEINDIA_URL,
    realestateindia_localities=REALESTATEINDIA_LOCALITIES,
    pages=PAGES,
    duckduckgo_pages=DUCKDUCKGO_PAGES,
    min_delay=MIN_DELAY,
    max_delay=MAX_DELAY,
    detail_retry=DETAIL_RETRY,
    request_timeout=REQUEST_TIMEOUT,
    es_index=ES_INDEX,
)


def _build_http_client():
    if curl_requests is not None:
        try:
            session = curl_requests.Session()
            return session, True
        except Exception:
            logger.exception("Failed to initialise curl_cffi; falling back to requests.")
    return requests.Session(), False


def _session_get(session, url, *, timeout, use_curl_cffi=False):
    headers = dict(REQUEST_HEADERS)
    headers["User-Agent"] = random.choice(MODERN_USER_AGENTS)
    if use_curl_cffi:
        impersonate = random.choice(IMPERSONATE_IDS)
        return session.get(url, headers=headers, timeout=timeout, impersonate=impersonate)
    return session.get(url, headers=headers, timeout=timeout)


def _looks_blocked(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in BLOCKED_MARKERS)


def _fetch(session, url, *, retries=1, timeout=None, use_curl_cffi=False):
    tout = timeout or SETTINGS.request_timeout
    last_exc = None
    for attempt in range(retries):
        try:
            response = _session_get(session, url, timeout=tout, use_curl_cffi=use_curl_cffi)
            response.raise_for_status()
            text = response.text
            if _looks_blocked(text):
                raise RuntimeError("Received a bot-protection response.")
            return text
        except Exception as exc:
            last_exc = exc
            sleep_for = SETTINGS.min_delay * (attempt + 1)
            logger.warning("Request failed (%s). Retrying in %.1fs", exc, sleep_for)
            time.sleep(sleep_for)
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


def _load_next_data(html_text: str) -> dict[str, Any] | None:
    match = re.search(
        r'<script[^>]+id=["\\\']__NEXT_DATA__["\\\'][^>]*>(.*?)</script>',
        html_text,
        re.S | re.I,
    )
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def _iter_jsonld_objects(html_text: str) -> Iterable[dict[str, Any]]:
    pattern = re.compile(
        r'<script[^>]+type=["\\\'\']application/ld\+json["\\\'\'][^>]*>(.*?)</script>',
        re.S | re.I,
    )
    for match in pattern.finditer(html_text):
        raw = match.group(1).strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    yield item
        elif isinstance(payload, dict):
            yield payload


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.upper() == "NULL":
            return None
        return stripped
    return value


def _as_text(value: Any) -> str | None:
    value = _normalize_scalar(value)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return str(value[0]).strip() if value else None
    if isinstance(value, dict):
        for key in ("value", "label", "name", "text"):
            if key in value and value[key] not in (None, "", []):
                return str(value[key]).strip()
    return str(value).strip()


def _maybe_int(value: Any) -> int | None:
    text = _as_text(value)
    if not text:
        return None
    match = re.search(r"-?\d+", text.replace(",", ""))
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None


def _maybe_float(value: Any) -> float | None:
    text = _as_text(value)
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _parse_price(value: Any) -> tuple[int | None, int | None, str | None]:
    if value is None:
        return None, None, None
    if isinstance(value, (int, float)):
        return int(value), None, "INR"
    text = _as_text(value)
    if not text:
        return None, None, None
    lowered = text.lower()
    if "price on request" in lowered:
        return None, None, None
    currency = "INR"
    cleaned = text.replace("\u20b9", " ").replace("Rs.", " ").replace("Rs", " ")
    matches = re.findall(r"([0-9]+(?:[\\.,][0-9]+)?)\s*([a-z]+)?", cleaned, flags=re.I)
    if not matches:
        return None, None, currency
    values: list[float] = []
    for number, unit in matches:
        try:
            val = float(number.replace(",", ""))
        except Exception:
            continue
        if unit:
            unit_l = unit.lower()
            if unit_l in PRICE_MULTIPLIERS:
                val *= PRICE_MULTIPLIERS[unit_l]
        values.append(val)
    if not values:
        return None, None, currency
    if len(values) == 1:
        return int(values[0]), None, currency
    return int(min(values)), int(max(values)), currency


def _parse_area(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _as_text(value)
    if not text:
        return None
    match = re.search(r"([0-9]+(?:[\\.,][0-9]+)?)\s*(sq\.?\s*ft|sqft|sq\.?\s*m|sqm|cents?)", text, re.I)
    if not match:
        return _maybe_float(text)
    number = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower().replace(" ", "")
    if "cent" in unit:
        return number * 435.6
    if "sqm" in unit or "sq.m" in unit:
        return number * 10.7639
    return number


def _normalize_configurations(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    text = _as_text(value)
    if not text:
        return []
    configs = []
    for part in re.split(r"[,;/|]", text):
        part = part.strip()
        if not part:
            continue
        match = re.match(r"(\d+)\s*(?:bhk|bedroom|br)?", part, re.I)
        if match:
            configs.append(f"{match.group(1)}BHK")
        elif re.match(r"\d+BHK", part, re.I):
            configs.append(part.upper())
        else:
            configs.append(part)
    return configs


def _normalize_property_types(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip().lower() for v in value if str(v).strip()]
    text = _as_text(value)
    if not text:
        return []
    types = []
    type_map = {
        "apartment": "apartment",
        "flat": "apartment",
        "villa": "villa",
        "independent house": "villa",
        "row house": "townhouse",
        "townhouse": "townhouse",
        "plotted": "plotted-development",
        "plot": "plotted-development",
        "duplex": "duplex",
        "penthouse": "penthouse",
        "studio": "studio",
    }
    for part in re.split(r"[,;/|&+]", text):
        part = part.strip().lower()
        if not part:
            continue
        matched = False
        for key, mapped in type_map.items():
            if key in part:
                types.append(mapped)
                matched = True
                break
        if not matched and part:
            types.append(part)
    return list(dict.fromkeys(types))


def _normalize_amenities(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip().title() for v in value if str(v).strip()]
    text = _as_text(value)
    if not text:
        return []
    return [a.strip().title() for a in re.split(r"[,;/|]", text) if a.strip()]


def _normalize_url(base_url: str, url: Any) -> str | None:
    text = _as_text(url)
    if not text:
        return None
    if text.startswith("http"):
        return text
    if text.startswith("//"):
        return f"https:{text}"
    if text.startswith("/"):
        return urljoin(base_url, text)
    return urljoin(base_url, text)


def _generate_project_id(project_name: str, builder_name: str) -> str:
    name_norm = re.sub(r"\s+", " ", project_name.strip().lower())
    builder_norm = re.sub(r"\s+", " ", builder_name.strip().lower())
    combined = f"{name_norm}|{builder_norm}"
    return hashlib.sha1(combined.encode("utf-8")).hexdigest()


def _to_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_images(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        images = [str(v).strip() for v in value if str(v).strip()]
        return images or None
    text = _as_text(value)
    if not text:
        return None
    return [text]


def _looks_like_project_url(url: str) -> bool:
    lower = url.lower()
    project_indicators = [
        "project", "property", "new-project", "new-launch",
        "pre-launch", "apartment", "villa", "flat", "resale",
        "listing", "detail", "overview",
    ]
    skip_extensions = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".css", ".js", ".ico", ".woff", ".ttf")
    if any(url.lower().endswith(ext) for ext in skip_extensions):
        return False
    return any(ind in lower for ind in project_indicators)


def _is_kochi_project(text: str, url: str = "") -> bool:
    """Check if a project is specifically about Kochi/Kerala."""
    lower_text = text.lower()
    lower_url = url.lower()
    combined = f"{lower_text} {lower_url}"
    kochi_indicators = ["kochi", "kerala", "ernakulam", "kakkanad", "edappally", "maradu",
                        "tripunithura", "kaloor", "panampilly", "thrikkakara", "elamkulam",
                        "vennala", "palarivattom", "vytilla", "aluv", "angamali", "kalamasery",
                        "kundannoor", "marine drive", "fort kochi", "mattancherry", "perumbavoor",
                        "nedumbassery", "eroor", "thrippunithura", "kadavanthra", "vazhakkala"]
    non_kochi_indicators = ["bangalore", "bengaluru", "mumbai", "delhi", "hyderabad", "chennai",
                            "pune", "noida", "gurgaon", "devanahalli", "whitefield", "electronic city",
                            "bannerghatta", "hebbal", "yelahanka", "sarjapur", "hosur road",
                            "magadi road", "begur road", "budigere", "jigani", "kensington road",
                            "outer ring road", "padil", "mangalore", "bidadi", "akshayanagar",
                            "somerville", "suncrest", "waterford", "waterfront", "fernvale",
                            "glenbrook", "greenmoor", "raintree park", "roshanara", "serenity shores",
                            "park grove", "park ridge", "pine forest", "nandi hills", "oakville",
                            "maple heights", "marigold", "landmark", "kings county", "eaton park",
                            "evergreen", "falcon city", "camden", "century landmark", "county dale",
                            "prosperity enclave", "sunset park"]
    kochi_score = sum(2 if ind in ("kochi", "kerala", "ernakulam") else 1 for ind in kochi_indicators if ind in combined)
    non_kochi_score = sum(1 for ind in non_kochi_indicators if ind in combined)
    return kochi_score > non_kochi_score and kochi_score >= 1


def _html_to_text(html_text: str) -> str:
    """Convert HTML to plain text for extraction."""
    text = re.sub(r"<script[^>]*>.*?</script>", "", html_text, flags=re.S)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# =============================================================================
# PARSERS - Individual project extraction from listing pages
# =============================================================================

def parse_duckduckgo_serp(html_text: str) -> list[dict[str, str]]:
    """Parse DuckDuckGo HTML search results to find listing URLs."""
    results: list[dict[str, str]] = []
    ddg_link_pattern = re.compile(r'class="result__a"[^>]+href="([^"]+)"', re.S)
    for match in ddg_link_pattern.finditer(html_text):
        raw_url = match.group(1)
        if raw_url.startswith("//duckduckgo.com/l/?uddg="):
            encoded = raw_url.split("uddg=")[1].split("&")[0]
            url = unquote(encoded)
        elif raw_url.startswith("http"):
            url = raw_url
        else:
            continue
        title_match = re.search(r'class="result__a"[^>]*>(.*?)</a>', html_text[match.start():match.start()+500], re.S)
        name = re.sub(r"<[^>]+>", "", title_match.group(1)).strip() if title_match else ""
        if _looks_like_project_url(url) and not any(
            skip in url.lower() for skip in ("youtube.com", "facebook.com", "twitter.com", "instagram.com", "play.google.com", "accounts.google.com", "wikipedia.org")
        ):
            results.append({"name": name or "", "url": url, "source": "duckduckgo"})

    if not results:
        all_hrefs = re.findall(r'href="([^"]+)"', html_text)
        for raw_url in all_hrefs:
            if "uddg=" in raw_url:
                encoded = raw_url.split("uddg=")[1].split("&")[0]
                url = unquote(encoded)
            elif raw_url.startswith("http"):
                url = raw_url
            else:
                continue
            if _looks_like_project_url(url) and not any(
                skip in url.lower() for skip in ("youtube.com", "facebook.com", "twitter.com", "instagram.com", "duckduckgo.com", "wikipedia.org")
            ):
                results.append({"name": "", "url": url, "source": "duckduckgo"})

    seen = set()
    deduped = []
    for r in results:
        if r["url"] not in seen:
            seen.add(r["url"])
            deduped.append(r)
    return deduped


def parse_signature_dwellings(html_text: str) -> list[dict[str, Any]]:
    """Parse Signature Dwellings Kochi page to extract individual project URLs."""
    projects: list[dict[str, Any]] = []
    link_pattern = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.S)
    base_url = "https://signaturedwellingsprojects.com"

    for match in link_pattern.finditer(html_text):
        url = match.group(1)
        content = match.group(2)
        name = re.sub(r"<[^>]+>", "", content).strip()
        if not name or len(name) < 5:
            continue
        if url.startswith("#") or url.startswith("javascript") or "whatsapp" in url.lower():
            continue
        if any(skip in url.lower() for skip in ("/property-type/", "/about", "/contact", "/privacy", "/terms", "/blog", "/sitemap")):
            continue
        if any(kw in name.lower() for kw in ("signature abode", "signature tropical", "signature dwelling")):
            if url.startswith("/"):
                url = base_url + url
            elif not url.startswith("http"):
                url = base_url + "/" + url
            projects.append({
                "project_name": name,
                "project_url": url,
                "source_url": url,
                "source": "signature_dwellings",
                "builder_name": "Signature Group",
            })
    return projects


def parse_prestige_kochi_projects(html_text: str) -> list[dict[str, Any]]:
    """Parse Prestige Prelaunch page to extract KOCHI-ONLY project URLs."""
    projects: list[dict[str, Any]] = []
    link_pattern = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.S)
    base_url = "https://prestigeprelaunchprojects.com"

    for match in link_pattern.finditer(html_text):
        url = match.group(1)
        content = match.group(2)
        name = re.sub(r"<[^>]+>", "", content).strip()
        if not name or len(name) < 5:
            continue
        if url.startswith("/"):
            url = base_url + url
        elif not url.startswith("http"):
            url = base_url + "/" + url
        if any(skip in url.lower() for skip in ("/about", "/contact", "/privacy", "/terms", "/blog", "/sitemap", "/shopdetail")):
            continue
        if not _is_kochi_project(name, url):
            continue
        projects.append({
            "project_name": name,
            "project_url": url,
            "source_url": url,
            "source": "prestige_prelaunch",
            "builder_name": "Prestige Group",
        })
    return projects


def parse_realestateindia_locality(html_text: str, locality: str = "") -> list[dict[str, Any]]:
    """Parse RealEstateIndia locality page to extract individual project URLs."""
    projects: list[dict[str, Any]] = []
    link_pattern = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.S)
    base_url = "https://www.realestateindia.com"

    for match in link_pattern.finditer(html_text):
        url = match.group(1)
        content = match.group(2)
        name = re.sub(r"<[^>]+>", "", content).strip()
        if not name or len(name) < 5:
            continue
        if url.startswith("/"):
            url = base_url + url
        elif not url.startswith("http"):
            url = base_url + "/" + url
        if any(skip in url.lower() for skip in ("/kochi-property/new-projects", "/property-type", "/about", "/contact")):
            continue
        if "new-projects" not in url.lower() and "project" not in name.lower():
            continue
        if locality:
            name = f"{name} - {locality.title()}"
        projects.append({
            "project_name": name,
            "project_url": url,
            "source_url": url,
            "source": "realestateindia",
        })
    return projects


def parse_project_detail_page(html_text: str, source: str = "") -> dict[str, Any]:
    """Parse a project detail page to extract structured data."""
    detail: dict[str, Any] = {}

    for obj in _iter_jsonld_objects(html_text):
        obj_type = str(obj.get("@type", "")).lower()
        if obj_type and obj_type not in ("itemlist", "breadcrumblist", "website"):
            if "name" in obj:
                detail["project_name"] = _as_text(obj.get("name"))
            if "description" in obj:
                detail["project_description"] = _as_text(obj.get("description"))
            images = _normalize_images(obj.get("image"))
            if images:
                detail["images"] = images
            geo = obj.get("geo") if isinstance(obj.get("geo"), dict) else {}
            if geo:
                detail["latitude"] = _maybe_float(geo.get("latitude"))
                detail["longitude"] = _maybe_float(geo.get("longitude"))
            brand = obj.get("brand")
            if isinstance(brand, dict) and "name" in brand:
                detail["builder_name"] = _as_text(brand.get("name"))
            break

    # Extract from <title> tag
    title_match = re.search(r'<title[^>]*>(.*?)</title>', html_text, re.I | re.S)
    if title_match and not detail.get("project_name"):
        title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()
        title = re.sub(r"\s*\|.*$", "", title).strip()
        if title:
            detail["project_name"] = title

    # Extract from meta description
    desc_match = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I | re.S)
    if not desc_match:
        desc_match = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']description["\']', html_text, re.I | re.S)
    if desc_match and not detail.get("project_description"):
        detail["project_description"] = desc_match.group(1).strip()

    # Extract from og:title
    og_title = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I | re.S)
    if not og_title:
        og_title = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']', html_text, re.I | re.S)
    if og_title and not detail.get("project_name"):
        title = og_title.group(1).strip()
        title = re.sub(r"\s*\|.*$", "", title).strip()
        if title:
            detail["project_name"] = title

    # Extract from og:description
    og_desc = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I | re.S)
    if not og_desc:
        og_desc = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:description["\']', html_text, re.I | re.S)
    if og_desc and not detail.get("project_description"):
        detail["project_description"] = og_desc.group(1).strip()

    # Extract from og:image
    og_image = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html_text, re.I | re.S)
    if not og_image:
        og_image = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', html_text, re.I | re.S)
    if og_image:
        img_url = og_image.group(1).strip()
        if img_url:
            detail["images"] = [img_url]

    text = _html_to_text(html_text)

    name_match = re.search(r'(?:Prestige|Signature|Asset|Sobha|Godrej|Puravankara|Brigade|SFS|Serene|Anta)\s+([A-Za-z][A-Za-z\s]+?)(?:\s+at|\s+in|\s+Kochi|\s+\||\s+-)', text, re.I)
    if name_match and not detail.get("project_name"):
        detail["project_name"] = name_match.group(1).strip()

    if not detail.get("builder_name"):
        builder_match = re.search(r'(?:Prestige Group|Signature Group|Signature Dwellings|Asset Homes|Sobha Limited|Godrej Properties|Puravankara Limited|Brigade Group|SFS Homes|Serene Communities|Anta Builders)', text, re.I)
        if builder_match:
            detail["builder_name"] = builder_match.group(0)

    bhk_match = re.findall(r'(\d+)\s*BHK', text, re.I)
    if bhk_match:
        detail["configurations"] = [f"{b}BHK" for b in bhk_match]

    price_match = re.search(r'(?:₹|Rs\.?)\s*([0-9,.]+)\s*(Cr|Lac|Lakh|K|M)?', text, re.I)
    if price_match:
        price_text = price_match.group(0)
        pmin, pmax, pcur = _parse_price(price_text)
        if pmin:
            detail["price_min"] = pmin
        if pmax:
            detail["price_max"] = pmax

    area_match = re.search(r'([0-9,.]+)\s*(?:sq\.?\s*ft|sqft)', text, re.I)
    if area_match:
        detail["super_area_min_sqft"] = _parse_area(area_match.group(0))

    locality_match = re.search(r'(?:at|in|located)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),?\s*Kochi', text)
    if locality_match:
        detail["locality"] = locality_match.group(1).strip()

    rera_match = re.search(r'RERA\s*(?:No\.?|#)?\s*([A-Za-z0-9/\-]+)', text, re.I)
    if rera_match:
        detail["rera_number"] = rera_match.group(1).strip()

    possession_match = re.search(r'(?:possession|completion)\s*(?:date)?\s*([A-Z][a-z]+\s+\d{4})', text, re.I)
    if possession_match:
        detail["possession_date"] = possession_match.group(1).strip()

    launch_match = re.search(r'(pre-launch|new-launch|under-construction|ready-to-move|upcoming)', text, re.I)
    if launch_match:
        detail["launch_status"] = launch_match.group(1).lower()

    type_match = re.search(r'TYPE\s*:\s*([A-Za-z\s]+?)(?:\s+Price|$)', text, re.I)
    if type_match:
        detail["property_types"] = _normalize_property_types(type_match.group(1))

    config_match = re.search(r'Configurations\s*:\s*([^\n]+)', text, re.I)
    if config_match and not detail.get("configurations"):
        detail["configurations"] = _normalize_configurations(config_match.group(1))

    if not detail.get("project_description"):
        desc_match = re.search(r'(?:about|description)[\s:]+([^.]{50,300})', text, re.I)
        if desc_match:
            detail["project_description"] = desc_match.group(1).strip()

    amenities_section = re.search(r'(?:amenities|facilities)[\s:]+([^.]{50,500})', text, re.I)
    if amenities_section:
        detail["amenities"] = _normalize_amenities(amenities_section.group(1))

    return {key: value for key, value in detail.items() if value not in (None, [], "")}


# =============================================================================
# AI EXTRACTION
# =============================================================================

def _strip_bom_and_fences(s: str) -> str:
    s = s.lstrip("\ufeff").strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z0-9_-]*\s*\n?", "", s, count=1)
        if s.endswith("```"):
            s = s[:-3].rstrip()
    s = re.sub(r"^\)\]\}',?\s*\n", "", s)
    return s


def _find_first_json_block(s: str) -> str | None:
    s = s.strip()
    start_idx = None
    depth = 0
    in_str = False
    esc = False
    quote = None

    for i, ch in enumerate(s):
        if start_idx is None:
            if ch in "{[":
                start_idx = i
                depth = 1
                continue
        else:
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == quote:
                    in_str = False
            else:
                if ch in ('"', "'"):
                    in_str = True
                    quote = ch
                elif ch in "{[":
                    depth += 1
                elif ch in "}]":
                    depth -= 1
                    if depth == 0:
                        return s[start_idx : i + 1]
    return None


def parse_llm_json(text: str) -> Any:
    t = _strip_bom_and_fences(text)
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        pass
    if json_repair is not None:
        try:
            return json_repair.loads(t)
        except Exception:
            pass
    if dirtyjson is not None:
        try:
            return dirtyjson.loads(t, search_for_first_object=True)
        except Exception:
            pass
    block = _find_first_json_block(t)
    if block:
        try:
            return parse_llm_json(block)
        except Exception:
            pass
    t2 = re.sub(r"\bNone\b", "null", t)
    t2 = re.sub(r"\bTrue\b", "true", t2)
    t2 = re.sub(r"\bFalse\b", "false", t2)
    t2 = "".join(ch for ch in t2 if (ord(ch) >= 32 or ch in "\n\r\t"))
    try:
        return json.loads(t2)
    except json.JSONDecodeError as e:
        snippet = t[:500].replace("\n", "\\n")
        raise ValueError(f"Could not parse/repair JSON. Starts with: {snippet}") from e


def call_gemini(payload: dict) -> requests.Response:
    for attempt in range(1, MAX_RETRIES_V2 + 1):
        resp = requests.post(GEMINI_V2_URL, json=payload, headers=GEMINI_HEADERS, params=GEMINI_PARAMS, timeout=30)
        if resp.status_code == 200:
            return resp
        if resp.status_code != 429:
            resp.raise_for_status()
        time.sleep(2 ** attempt)
    for attempt in range(1, MAX_RETRIES_V1_5 + 1):
        resp = requests.post(GEMINI_V1_5_URL, json=payload, headers=GEMINI_HEADERS, params=GEMINI_PARAMS, timeout=30)
        if resp.status_code == 200:
            return resp
        if resp.status_code != 429:
            resp.raise_for_status()
        time.sleep(2 ** attempt)
    raise RuntimeError("Gemini API: exhausted retries on both 2.0-flash and 1.5-flash")


def ai_extract_project(text: str) -> dict[str, Any]:
    payload = {
        "system_instruction": {
            "parts": [{"text": "You are an extraction engine. Return ONE compact JSON object with exactly the keys I list. Strings must NOT contain literal line-breaks - escape them as \\n. No markdown, no code fences, no explanatory text. If a value is missing use null."}]
        },
        "contents": [{"parts": [{"text": f"{PROJECT_EXTRACT_PROMPT}\n\nText to extract from:\n{text[:8000]}"}]}],
        "generation_config": {"response_mime_type": "application/json", "temperature": 0.0},
    }
    try:
        response = call_gemini(payload)
        if response.status_code == 200:
            result = response.json()
            result_text = result["candidates"][0]["content"]["parts"][0]["text"]
            return parse_llm_json(result_text)
    except Exception as exc:
        logger.warning("AI extraction failed: %s", exc)
    return {}


def ai_classify_builder(builder_name: str, project_details: dict[str, Any]) -> str:
    builder_lower = builder_name.lower().strip()
    for known in KNOWN_PREMIUM_BUILDERS:
        if known in builder_lower:
            return "premium"
    for known in KNOWN_MID_SEGMENT_BUILDERS:
        if known in builder_lower:
            return "mid-segment"

    price_min = project_details.get("price_min") or 0
    property_types = project_details.get("property_types") or []
    is_villa = "villa" in property_types

    if price_min > 0:
        if is_villa:
            if price_min >= 3_00_00_000:
                return "luxury"
            if price_min >= 1_50_00_000:
                return "premium"
            if price_min >= 80_00_000:
                return "mid-segment"
            return "affordable"
        else:
            if price_min >= 1_50_00_000:
                return "luxury"
            if price_min >= 80_00_000:
                return "premium"
            if price_min >= 40_00_000:
                return "mid-segment"
            return "affordable"

    context = f"Builder: {builder_name}"
    if price_min:
        context += f", Price range starts: ₹{price_min}"
    if property_types:
        context += f", Types: {', '.join(property_types)}"
    if project_details.get("locality"):
        context += f", Locality: {project_details['locality']}"
    if project_details.get("amenities"):
        context += f", Amenities: {', '.join(project_details['amenities'][:5])}"

    payload = {
        "system_instruction": {"parts": [{"text": BUILDER_TIER_PROMPT}]},
        "contents": [{"parts": [{"text": context}]}],
        "generation_config": {"response_mime_type": "application/json", "temperature": 0.0},
    }
    try:
        response = call_gemini(payload)
        if response.status_code == 200:
            result = response.json()
            result_text = result["candidates"][0]["content"]["parts"][0]["text"]
            parsed = parse_llm_json(result_text)
            tier = parsed.get("builder_tier", "").strip().lower()
            if tier in ("luxury", "premium", "mid-segment", "affordable"):
                return tier
    except Exception as exc:
        logger.warning("AI builder classification failed for %s: %s", builder_name, exc)
    return "mid-segment"


# =============================================================================
# NORMALIZATION & DEDUPLICATION
# =============================================================================

def normalize_project_record(raw: dict[str, Any], discovered_at: str | None = None) -> dict[str, Any]:
    project_name = _as_text(raw.get("project_name", ""))
    builder_name = _as_text(raw.get("builder_name", "")) or "Unknown"
    if not project_name:
        return {}
    record: dict[str, Any] = {}
    for key in STANDARD_SCHEMA_FIELDS:
        if key in raw:
            record[key] = raw[key]
        else:
            record[key] = None
    record["project_name"] = project_name
    record["builder_name"] = builder_name
    record["city"] = raw.get("city") or "Kochi"
    record["state"] = raw.get("state") or "Kerala"
    record["price_currency"] = raw.get("price_currency") or "INR"
    if not record.get("launch_status"):
        record["launch_status"] = "new-launch"
    if record.get("price_min") is not None:
        record["price_min"] = int(record["price_min"])
    if record.get("price_max") is not None:
        record["price_max"] = int(record["price_max"])
    if record.get("price_per_sqft") is not None:
        record["price_per_sqft"] = int(record["price_per_sqft"])
    record["property_types"] = record.get("property_types") or []
    record["configurations"] = record.get("configurations") or []
    record["amenities"] = record.get("amenities") or []
    record["project_highlights"] = record.get("project_highlights") or []
    record["images"] = record.get("images") or None
    record["floor_plans"] = record.get("floor_plans") or None
    record["source"] = record.get("source") or "unknown"
    now_iso = discovered_at or _to_iso_now()
    if not record.get("discovered_at"):
        record["discovered_at"] = now_iso
    record["updated_at"] = _to_iso_now()
    record["id"] = _generate_project_id(project_name, builder_name)
    list_keys = {"property_types", "configurations", "amenities", "project_highlights"}
    mandatory_keys = set(MANDATORY_FIELDS)
    cleaned: dict[str, Any] = {}
    for k, v in record.items():
        if k in list_keys:
            cleaned[k] = v
        elif k in mandatory_keys:
            cleaned[k] = v
        elif v is not None and v != [] and v != "":
            cleaned[k] = v
    return cleaned


def deduplicate_projects(projects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for project in projects:
        pid = project.get("id")
        if not pid:
            continue
        existing = by_id.get(pid)
        if existing is None:
            by_id[pid] = project
        else:
            merged = existing.copy()
            for key, value in project.items():
                if value is None or value == [] or value == "":
                    continue
                if key == "source":
                    merged_sources = set()
                    for s in (existing.get("source"), project.get("source")):
                        if s:
                            merged_sources.add(s)
                    merged["source"] = ",".join(sorted(merged_sources))
                elif key in ("price_min", "price_max"):
                    existing_val = existing.get(key)
                    if existing_val is None:
                        merged[key] = value
                    elif value is not None:
                        if key == "price_min":
                            merged[key] = min(existing_val, value)
                        else:
                            merged[key] = max(existing_val, value)
                elif key in ("amenities", "property_types", "configurations", "project_highlights"):
                    existing_list = existing.get(key) or []
                    merged[key] = list(dict.fromkeys(existing_list + value))
                elif key not in merged or merged[key] is None or merged[key] == [] or merged[key] == "":
                    merged[key] = value
            by_id[pid] = merged
    return list(by_id.values())


# =============================================================================
# ELASTICSEARCH
# =============================================================================

ES_INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s",
    },
    "mappings": {
        "dynamic": True,
        "dynamic_templates": [
            {"dates_iso": {"match": "*_at", "mapping": {"type": "date", "format": "strict_date_optional_time||epoch_millis"}}},
            {"dates": {"match": "*_date", "mapping": {"type": "keyword"}}},
            {"epochs": {"match": "*_epoch", "mapping": {"type": "long"}}},
            {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword", "ignore_above": 512}}},
            {"doubleNums": {"match_mapping_type": "double", "mapping": {"type": "double"}}},
            {"longNums": {"match_mapping_type": "long", "mapping": {"type": "long"}}},
        ],
        "properties": {
            "id": {"type": "keyword"},
            "project_name": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "builder_name": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "builder_tier": {"type": "keyword"},
            "launch_status": {"type": "keyword"},
            "property_types": {"type": "keyword"},
            "configurations": {"type": "keyword"},
            "locality": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
            "district": {"type": "keyword"},
            "price_min": {"type": "long"},
            "price_max": {"type": "long"},
            "price_per_sqft": {"type": "long"},
            "rera_number": {"type": "keyword"},
            "source": {"type": "keyword"},
            "project_url": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "project_description": {"type": "text"},
            "amenities": {"type": "keyword"},
            "project_highlights": {"type": "keyword"},
        },
    },
}


def es_client() -> Elasticsearch:
    es = Elasticsearch(
        hosts=es_hosts,
        http_auth=(es_user, es_password),
        timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )
    try:
        info = es.info()
        logger.info("Connected to ES cluster=%s", info.get("cluster_name", "unknown"))
    except Exception as exc:
        logger.warning("Failed to fetch ES info: %s", exc)
    return es


def ensure_index(es: Elasticsearch, index: str) -> None:
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=ES_INDEX_MAPPING)


def es_doc_exists(es: Elasticsearch, index: str, doc_id: str) -> bool:
    try:
        return bool(es.exists(index=index, id=doc_id))
    except Exception:
        return False


def df_to_actions(df: pd.DataFrame, index: str) -> Iterable[dict[str, Any]]:
    clean = df.replace({np.nan: None})
    for record in clean.to_dict(orient="records"):
        doc_id = record.get("id")
        if not doc_id:
            continue
        yield {
            "_index": index,
            "_id": doc_id,
            "_op_type": "index",
            "_source": record,
        }


def index_projects_to_es(projects: list[dict[str, Any]], es: Elasticsearch | None = None) -> int:
    if not projects:
        logger.info("No projects to index into Elasticsearch (%s)", SETTINGS.es_index)
        return 0

    if es is None:
        es = es_client()
    ensure_index(es, SETTINGS.es_index)

    df = pd.DataFrame(projects)
    df = df.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)

    actions = list(df_to_actions(df, SETTINGS.es_index))
    if not actions:
        logger.info("No serializable project records for Elasticsearch")
        return 0

    try:
        indexed, errors = helpers.bulk(
            es,
            actions,
            chunk_size=500,
            request_timeout=120,
            raise_on_error=False,
            raise_on_exception=False,
        )
    except Exception:
        logger.exception("Elasticsearch bulk indexing failed")
        raise

    if errors:
        logger.warning("ES bulk indexing completed with %d errors (index=%s)", len(errors), SETTINGS.es_index)
    logger.info("Indexed %d project records into ES index %s", indexed, SETTINGS.es_index)
    return indexed


# =============================================================================
# PIPELINE TASKS
# =============================================================================

async def _fetch_detail_with_retry(session, url: str | None, use_curl_cffi: bool) -> dict[str, Any]:
    if not url:
        return {}
    for attempt in range(SETTINGS.detail_retry):
        try:
            html_text = _fetch(session, url, retries=1, use_curl_cffi=use_curl_cffi)
            detail = parse_project_detail_page(html_text, "")
            if detail:
                return detail
        except Exception as exc:
            logger.warning("Detail fetch failed for %s (attempt %s/%s): %s", url, attempt + 1, SETTINGS.detail_retry, exc)
        await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))
    return {}


class InputParams(BaseModel):
    pass


@task
async def discover_kochi_projects(params: InputParams = None) -> list[dict[str, Any]]:
    """Two-level discovery:
    Level 1: Find listing pages via DuckDuckGo + portal homepages
    Level 2: Visit listing pages to extract individual project URLs
    """
    session, use_curl = _build_http_client()
    discovered: list[dict[str, Any]] = []
    now = _to_iso_now()
    seen_urls: set[str] = set()
    listing_urls_to_visit: list[str] = []

    def add_project(proj: dict[str, Any]):
        url = proj.get("project_url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            proj["discovered_at"] = now
            discovered.append(proj)

    def add_listing_url(url: str):
        if url and url not in seen_urls and url not in listing_urls_to_visit:
            listing_urls_to_visit.append(url)
            seen_urls.add(url)

    # --- Level 1: DuckDuckGo search for listing pages (NOT individual projects) ---
    for query in SETTINGS.duckduckgo_queries:
        for page in range(SETTINGS.duckduckgo_pages):
            offset = page * 10
            url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}&s={offset}"
            logger.info("DuckDuckGo search: %s (page %d)", query, page + 1)
            try:
                html_text = _fetch(session, url, retries=2, use_curl_cffi=use_curl)
                results = parse_duckduckgo_serp(html_text)
                for r in results:
                    add_listing_url(r["url"])
                logger.info("Found %d DuckDuckGo listing URLs for '%s' page %d", len(results), query, page + 1)
            except Exception as exc:
                logger.warning("DuckDuckGo search failed for '%s' page %d: %s", query, page + 1, exc)
            await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))

    # --- Level 1: Visit portal homepages for individual project URLs ---
    portal_configs = [
        ("signature_dwellings", SETTINGS.signature_dwellings_url, parse_signature_dwellings),
        ("prestige_prelaunch", SETTINGS.prestige_prelaunch_kochi_url, parse_prestige_kochi_projects),
    ]
    for portal_name, base_url, parser in portal_configs:
        if not base_url:
            continue
        logger.info("Scraping portal homepage: %s", portal_name)
        try:
            html_text = _fetch(session, base_url, retries=2, use_curl_cffi=use_curl)
            projects = parser(html_text)
            for p in projects:
                add_project(p)
            logger.info("Found %d projects from %s homepage", len(projects), portal_name)
        except Exception as exc:
            logger.warning("Failed to scrape %s homepage: %s", portal_name, exc)

    # --- Level 1: Visit RealEstateIndia locality pages for individual project URLs ---
    if SETTINGS.realestateindia_url:
        logger.info("Scraping RealEstateIndia main page")
        try:
            html_text = _fetch(session, SETTINGS.realestateindia_url, retries=2, use_curl_cffi=use_curl)
            locality_links = re.findall(r'href=["\'](/kochi-property/new-projects-in-[^"\']+)["\']', html_text)
            for locality_path in locality_links[:10]:
                locality_url = f"https://www.realestateindia.com{locality_path}"
                locality_name = locality_path.split("-")[-1].replace(".htm", "")
                logger.info("Scraping locality: %s", locality_name)
                try:
                    html_text = _fetch(session, locality_url, retries=1, use_curl_cffi=use_curl)
                    projects = parse_realestateindia_locality(html_text, locality=locality_name)
                    for p in projects:
                        add_project(p)
                    logger.info("Found %d projects from %s", len(projects), locality_name)
                except Exception as exc:
                    logger.warning("Failed to scrape locality %s: %s", locality_name, exc)
                await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))
        except Exception as exc:
            logger.warning("Failed to scrape RealEstateIndia: %s", exc)

    # --- Level 2: Visit DuckDuckGo-discovered listing pages to extract individual projects ---
    for listing_url in listing_urls_to_visit[:15]:
        logger.info("Visiting listing page: %s", listing_url)
        try:
            html_text = _fetch(session, listing_url, retries=1, use_curl_cffi=use_curl)
            text = _html_to_text(html_text)
            links = re.findall(r'href=["\']([^"\']+)["\']', html_text)
            for link in links:
                if link.startswith("http") and _looks_like_project_url(link) and link not in seen_urls:
                    if _is_kochi_project(text, link):
                        add_project({
                            "project_name": "",
                            "project_url": link,
                            "source_url": link,
                            "source": "discovered_listing",
                        })
        except Exception as exc:
            logger.warning("Failed to visit listing page %s: %s", listing_url, exc)
        await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))

    logger.info("Total discovered: %d individual project entries", len(discovered))
    out_dir = Path("saved_data/kochi_launches")
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(discovered)
    df.to_json(out_dir / "discovered_raw.json", orient="records", force_ascii=False, indent=2)
    return discovered


@task
async def enrich_project_details(discovered: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Visit each project detail page, extract structured data, normalize to schema."""
    session, use_curl = _build_http_client()
    enriched: list[dict[str, Any]] = []

    for i, project in enumerate(discovered):
        project_name = project.get("project_name", "")
        detail_url = project.get("project_url") or project.get("source_url")
        source = project.get("source", "unknown")
        logger.info("Enriching project %d/%d: %s (%s)", i + 1, len(discovered), project_name or "(unnamed)", source)

        if detail_url:
            try:
                html_text = _fetch(session, detail_url, retries=1, use_curl_cffi=use_curl)
                detail = parse_project_detail_page(html_text, source)
                for key, value in detail.items():
                    if value is not None and (key not in project or project.get(key) is None or project.get(key) == "" or project.get(key) == []):
                        project[key] = value

                needs_ai = not project.get("builder_name") or not project.get("configurations") or not project.get("property_types")
                if needs_ai or not project.get("project_description"):
                    try:
                        page_text = _html_to_text(html_text)
                        if page_text:
                            ai_result = ai_extract_project(page_text)
                            for key, value in ai_result.items():
                                if value is not None and (key not in project or project.get(key) is None or project.get(key) == "" or project.get(key) == []):
                                    project[key] = value
                    except Exception as exc:
                        logger.warning("AI extraction failed for %s: %s", project_name, exc)
            except Exception as exc:
                logger.warning("Detail fetch failed for %s: %s", detail_url, exc)

        normalized = normalize_project_record(project)
        if normalized:
            enriched.append(normalized)

        await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))

    logger.info("Enriched %d projects out of %d discovered", len(enriched), len(discovered))
    out_dir = Path("saved_data/kochi_launches")
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(enriched)
    df.to_json(out_dir / "enriched_projects.json", orient="records", force_ascii=False, indent=2)
    return enriched


@task
async def standardize_and_index(enriched: list[dict[str, Any]]) -> int:
    if not enriched:
        logger.info("No enriched projects to standardize and index")
        return 0

    deduped = deduplicate_projects(enriched)
    logger.info("After deduplication: %d unique projects (from %d)", len(deduped), len(enriched))

    indexed = index_projects_to_es(deduped)

    out_dir = Path("saved_data/kochi_launches")
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(deduped)
    df.to_json(out_dir / "standardized_projects.json", orient="records", force_ascii=False, indent=2)
    return indexed


@task
async def ai_classify_builders() -> int:
    es = es_client()
    ensure_index(es, SETTINGS.es_index)

    query = {
        "query": {
            "bool": {
                "must_not": {"exists": {"field": "builder_tier"}}
            }
        },
        "_source": ["id", "project_name", "builder_name", "price_min", "property_types", "locality", "amenities"],
        "size": 10000,
    }

    docs = list(helpers.scan(es, index=SETTINGS.es_index, query=query, preserve_order=False, size=1000))
    if not docs:
        logger.info("No projects without builder_tier found")
        return 0

    logger.info("Found %d projects needing builder tier classification", len(docs))
    updated = 0

    for doc in docs:
        source = doc.get("_source", {})
        doc_id = doc.get("_id")
        builder_name = source.get("builder_name", "Unknown")
        if not builder_name or builder_name == "Unknown":
            continue

        tier = ai_classify_builder(builder_name, source)
        try:
            es.update(
                index=SETTINGS.es_index,
                id=doc_id,
                body={"doc": {"builder_tier": tier, "updated_at": _to_iso_now()}},
            )
            updated += 1
        except Exception as exc:
            logger.warning("Failed to update builder_tier for doc %s: %s", doc_id, exc)

        await asyncio.sleep(0.5)

    logger.info("Classified builder tier for %d projects", updated)
    return updated


register_pipeline(
    id="kochi_launches_pipeline",
    description="Discover, enrich and index pre-launch & new-launch property projects in Kochi, Kerala into Elasticsearch.",
    tasks=[discover_kochi_projects, enrich_project_details, standardize_and_index],
    triggers=[
        Trigger(
            id="kochi_launches_daily",
            name="Kochi Launches Daily",
            description="Run Kochi property launches pipeline daily at 06:30 IST",
            params=InputParams(),
            schedule=CronTrigger(hour="1", minute="0", timezone="Asia/Kolkata"),
        )
    ],
    params=InputParams,
)
