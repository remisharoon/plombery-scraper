"""Kochi pre-launch & new-launch property scraper pipeline.

Discovers, enriches, standardizes and indexes pre-launch and new-launch
residential projects (apartments, villas) in Kochi, Kerala from multiple
property portals and Google search, into Elasticsearch.
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
from urllib.parse import quote_plus, urljoin

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

IMPERSONATE_IDS = ("chrome110", "chrome120", "chrome124", "edge110")

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

MAGICBRICKS_URL = kochi_section.get("magicbricks_url", "https://www.magicbricks.com/new-projects-kochi-pppfs")
HOUSING_URL = kochi_section.get("housing_url", "https://www.housing.com/new-projects/kochi")
ACRES99_URL = kochi_section.get("acres99_url", "https://www.99acres.com/new-projects-in-kochi")
COMMONFLOOR_URL = kochi_section.get("commonfloor_url", "https://www.commonfloor.com/kochi-property/new-projects")
GOOGLE_QUERIES = [q.strip() for q in kochi_section.get("google_search_queries", "pre launch apartments kochi").split(",") if q.strip()]
PAGES = int(kochi_section.get("pages", "20"))
GOOGLE_PAGES = int(kochi_section.get("google_pages", "5"))
MIN_DELAY = float(kochi_section.get("min_delay_seconds", "2.0"))
MAX_DELAY = float(kochi_section.get("max_delay_seconds", "6.0"))
DETAIL_RETRY = int(kochi_section.get("detail_retry_count", "3"))
REQUEST_TIMEOUT = int(kochi_section.get("request_timeout_seconds", "30"))
ES_INDEX = kochi_section.get("es_index", "kochi_property_launches")

es_hosts = _es_config["host"].split(",")
es_user = _es_config["username"]
es_password = _es_config["password"]


@dataclass(slots=True)
class KochiLaunchSettings:
    magicbricks_url: str
    housing_url: str
    acres99_url: str
    commonfloor_url: str
    google_queries: list[str]
    pages: int
    google_pages: int
    min_delay: float
    max_delay: float
    detail_retry: int
    request_timeout: int
    es_index: str


SETTINGS = KochiLaunchSettings(
    magicbricks_url=MAGICBRICKS_URL,
    housing_url=HOUSING_URL,
    acres99_url=ACRES99_URL,
    commonfloor_url=COMMONFLOOR_URL,
    google_queries=GOOGLE_QUERIES,
    pages=PAGES,
    google_pages=GOOGLE_PAGES,
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


def parse_google_serp(html_text: str) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for obj in _iter_jsonld_objects(html_text):
        if str(obj.get("@type", "")).lower() == "itemlist":
            for item in obj.get("itemListElement", []) or []:
                if not isinstance(item, dict):
                    continue
                url = item.get("url", "")
                name = _as_text(item.get("name", ""))
                if url and _looks_like_project_url(url):
                    results.append({"name": name or "", "url": url, "source": "google"})
        elif str(obj.get("@type", "")).lower() in ("webpage", "searchresultspage"):
            pass
        else:
            url = obj.get("url", "")
            name = _as_text(obj.get("name", ""))
            if url and _looks_like_project_url(url):
                results.append({"name": name or "", "url": url, "source": "google"})

    if not results:
        href_pattern = re.compile(r'<a[^>]+href=["\'](https?://[^"\']+)["\'][^>]*>(.*?)</a>', re.S)
        for match in href_pattern.finditer(html_text):
            url = match.group(1)
            name = re.sub(r"<[^>]+>", "", match.group(2)).strip()
            if _looks_like_project_url(url) and not any(
                skip in url.lower() for skip in ("google.com", "youtube.com", "facebook.com", "twitter.com", "instagram.com", "play.google.com", "accounts.google.com")
            ):
                results.append({"name": name or "", "url": url, "source": "google"})

    seen = set()
    deduped = []
    for r in results:
        if r["url"] not in seen:
            seen.add(r["url"])
            deduped.append(r)
    return deduped


def parse_magicbricks_listing(html_text: str) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []
    next_data = _load_next_data(html_text)
    if next_data:
        try:
            listings = (
                next_data.get("props", {})
                .get("pageProps", {})
                .get("projectData", {})
                .get("projects", [])
            )
            for listing in listings:
                project = _extract_magicbricks_project(listing)
                if project:
                    projects.append(project)
        except Exception as exc:
            logger.warning("Error parsing MagicBricks __NEXT_DATA__: %s", exc)

    if not projects:
        try:
            json_pattern = re.compile(r'"projects"\s*:\s*(\[.*?\])\s*,\s*"(?:totalProjects|pageData)"', re.S)
            match = json_pattern.search(html_text)
            if match:
                listings = json.loads(match.group(1))
                for listing in listings:
                    project = _extract_magicbricks_project(listing)
                    if project:
                        projects.append(project)
        except Exception as exc:
            logger.warning("Error parsing MagicBricks inline JSON: %s", exc)

    if not projects:
        project_blocks = re.findall(
            r'<div[^>]+class="[^"]*projdis[^"]*"[^>]*>(.*?)</div>',
            html_text,
            re.S,
        )
        for block in project_blocks[:50]:
            name_match = re.search(r'title="([^"]+)"', block)
            url_match = re.search(r'href="([^"]+)"', block)
            if name_match:
                projects.append({
                    "project_name": name_match.group(1).strip(),
                    "project_url": _normalize_url("https://www.magicbricks.com", url_match.group(1)) if url_match else None,
                    "source": "magicbricks",
                })

    return projects


def _extract_magicbricks_project(data: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        return None
    project_name = _as_text(data.get("projectName") or data.get("name") or data.get("title"))
    if not project_name:
        return None
    builder_name = _as_text(data.get("builderName") or data.get("developerName") or data.get("builder"))
    price_text = data.get("priceRange") or data.get("price") or data.get("minPrice")
    price_min, price_max, currency = _parse_price(price_text)
    locality = _as_text(data.get("localityName") or data.get("locality") or data.get("location"))
    prop_type_raw = data.get("propertyType") or data.get("projectType") or data.get("propertyTypes")
    configs_raw = data.get("configuration") or data.get("configurations") or data.get("bhk")
    area_raw = data.get("area") or data.get("superArea") or data.get("carpetArea") or data.get("size")
    detail_path = data.get("projectURL") or data.get("url") or data.get("detailUrl")
    detail_url = _normalize_url("https://www.magicbricks.com", detail_path)
    images = _normalize_images(data.get("images") or data.get("projectImages") or data.get("image"))
    amenities = _normalize_amenities(data.get("amenities") or data.get("projectAmenities"))
    total_units = _maybe_int(data.get("totalUnits") or data.get("noOfUnits"))
    possession = _as_text(data.get("possessionDate") or data.get("possession") or data.get("expectedPossession"))
    rera = _as_text(data.get("reraNumber") or data.get("reraId") or data.get("rera"))
    lat = _maybe_float(data.get("latitude") or data.get("lat"))
    lng = _maybe_float(data.get("longitude") or data.get("lng"))

    area_sqft = _parse_area(area_raw)
    super_min = area_sqft if area_sqft else None

    return {
        "project_name": project_name,
        "builder_name": builder_name,
        "property_types": _normalize_property_types(prop_type_raw),
        "launch_status": _classify_launch_status(data),
        "price_min": price_min,
        "price_max": price_max,
        "price_currency": currency,
        "configurations": _normalize_configurations(configs_raw),
        "locality": locality,
        "city": "Kochi",
        "state": "Kerala",
        "latitude": lat,
        "longitude": lng,
        "super_area_min_sqft": super_min,
        "possession_date": possession,
        "rera_number": rera,
        "amenities": amenities,
        "total_units": total_units,
        "images": images,
        "project_url": detail_url,
        "source_url": detail_url,
        "source": "magicbricks",
    }


def _classify_launch_status(data: dict[str, Any]) -> str:
    status_raw = str(
        data.get("projectStatus") or data.get("status") or data.get("launchStatus") or ""
    ).lower()
    if "pre" in status_raw and "launch" in status_raw:
        return "pre-launch"
    if "new" in status_raw and "launch" in status_raw:
        return "new-launch"
    if "under" in status_raw and "construct" in status_raw:
        return "under-construction"
    if "ready" in status_raw or "complete" in status_raw:
        return "ready-to-move"
    possession = str(data.get("possessionDate") or data.get("possession") or "")
    if possession:
        try:
            poss_year = int(re.search(r"20\d{2}", possession).group(0))
            current_year = datetime.now().year
            if poss_year > current_year + 1:
                return "pre-launch"
            if poss_year > current_year:
                return "under-construction"
        except Exception:
            pass
    return "new-launch"


def parse_housing_listing(html_text: str) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []
    next_data = _load_next_data(html_text)
    if next_data:
        try:
            listings = (
                next_data.get("props", {})
                .get("pageProps", {})
                .get("searchResults", {})
                .get("listings", [])
            )
            if not listings:
                listings = (
                    next_data.get("props", {})
                    .get("pageProps", {})
                    .get("projects", [])
                )
            for listing in listings:
                project = _extract_housing_project(listing)
                if project:
                    projects.append(project)
        except Exception as exc:
            logger.warning("Error parsing Housing __NEXT_DATA__: %s", exc)

    if not projects:
        try:
            json_pattern = re.compile(r'"projects"\s*:\s*(\[.*?\])\s*[,}]', re.S)
            match = json_pattern.search(html_text)
            if match:
                listings = json.loads(match.group(1))
                for listing in listings:
                    project = _extract_housing_project(listing)
                    if project:
                        projects.append(project)
        except Exception as exc:
            logger.warning("Error parsing Housing inline JSON: %s", exc)

    return projects


def _extract_housing_project(data: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        return None
    project_name = _as_text(data.get("projectName") or data.get("name") or data.get("title"))
    if not project_name:
        return None
    builder_name = _as_text(data.get("builderName") or data.get("developerName") or data.get("builder"))
    price_text = data.get("priceRange") or data.get("price") or data.get("minPrice")
    price_min, price_max, currency = _parse_price(price_text)
    locality = _as_text(data.get("localityName") or data.get("locality") or data.get("address"))
    prop_type_raw = data.get("propertyType") or data.get("projectType")
    configs_raw = data.get("configuration") or data.get("bhk")
    area_raw = data.get("area") or data.get("size")
    detail_path = data.get("url") or data.get("detailUrl") or data.get("projectUrl")
    detail_url = _normalize_url("https://www.housing.com", detail_path)
    images = _normalize_images(data.get("images") or data.get("image"))
    amenities = _normalize_amenities(data.get("amenities"))
    total_units = _maybe_int(data.get("totalUnits"))
    possession = _as_text(data.get("possessionDate") or data.get("possession"))
    rera = _as_text(data.get("reraId") or data.get("reraNumber"))
    lat = _maybe_float(data.get("lat") or data.get("latitude"))
    lng = _maybe_float(data.get("lng") or data.get("longitude"))
    area_sqft = _parse_area(area_raw)

    return {
        "project_name": project_name,
        "builder_name": builder_name,
        "property_types": _normalize_property_types(prop_type_raw),
        "launch_status": _classify_launch_status(data),
        "price_min": price_min,
        "price_max": price_max,
        "price_currency": currency,
        "configurations": _normalize_configurations(configs_raw),
        "locality": locality,
        "city": "Kochi",
        "state": "Kerala",
        "latitude": lat,
        "longitude": lng,
        "super_area_min_sqft": area_sqft,
        "possession_date": possession,
        "rera_number": rera,
        "amenities": amenities,
        "total_units": total_units,
        "images": images,
        "project_url": detail_url,
        "source_url": detail_url,
        "source": "housing",
    }


def parse_99acres_listing(html_text: str) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []
    next_data = _load_next_data(html_text)
    if next_data:
        try:
            for obj in _iter_objects(next_data):
                if not isinstance(obj, list):
                    continue
                if len(obj) < 3:
                    continue
                if not all(isinstance(item, dict) for item in obj):
                    continue
                scores = [_score_project_dict(item) for item in obj]
                if not scores or max(scores) < 3.0:
                    continue
                for item in obj:
                    project = _extract_99acres_project(item)
                    if project:
                        projects.append(project)
                break
        except Exception as exc:
            logger.warning("Error parsing 99acres __NEXT_DATA__: %s", exc)

    if not projects:
        for obj in _iter_jsonld_objects(html_text):
            if str(obj.get("@type", "")).lower() == "itemlist":
                for item in obj.get("itemListElement", []) or []:
                    if isinstance(item, dict):
                        project = _extract_99acres_project(item.get("item", item))
                        if project:
                            projects.append(project)

    return projects


def _score_project_dict(item: dict[str, Any]) -> float:
    if not isinstance(item, dict):
        return 0.0
    keyset = set(item.keys())
    score = 0.0
    if keyset & {"projectName", "name", "title"}:
        score += 3.0
    if keyset & {"builderName", "developerName"}:
        score += 2.0
    if keyset & {"price", "priceRange", "minPrice"}:
        score += 2.0
    if keyset & {"propertyType", "projectType"}:
        score += 1.0
    if keyset & {"locality", "localityName"}:
        score += 1.0
    return score


def _iter_objects(obj: Any) -> Iterable[Any]:
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _iter_objects(value)
    elif isinstance(obj, list):
        yield obj
        for item in obj:
            yield from _iter_objects(item)


def _extract_99acres_project(data: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        return None
    project_name = _as_text(
        data.get("projectName") or data.get("name") or data.get("title") or data.get("societyName")
    )
    if not project_name:
        return None
    builder_name = _as_text(data.get("builderName") or data.get("developerName") or data.get("builder"))
    price_text = data.get("priceRange") or data.get("price") or data.get("minPrice")
    price_min, price_max, currency = _parse_price(price_text)
    locality = _as_text(data.get("localityName") or data.get("locality") or data.get("location"))
    prop_type_raw = data.get("propertyType") or data.get("projectType") or data.get("propType")
    configs_raw = data.get("configuration") or data.get("bedrooms") or data.get("bhk")
    area_raw = data.get("area") or data.get("size") or data.get("builtUpArea") or data.get("plotArea")
    detail_path = data.get("url") or data.get("detailUrl") or data.get("pdUrl") or data.get("seoUrl")
    detail_url = _normalize_url("https://www.99acres.com", detail_path)
    images = _normalize_images(data.get("images") or data.get("image") or data.get("photos"))
    amenities = _normalize_amenities(data.get("amenities"))
    total_units = _maybe_int(data.get("totalUnits") or data.get("noOfUnits"))
    possession = _as_text(data.get("possessionDate") or data.get("possession") or data.get("possessionStatus"))
    rera = _as_text(data.get("reraId") or data.get("reraNumber") or data.get("rera"))
    lat = _maybe_float(data.get("latitude") or data.get("lat"))
    lng = _maybe_float(data.get("longitude") or data.get("lng") or data.get("lon"))
    area_sqft = _parse_area(area_raw)

    return {
        "project_name": project_name,
        "builder_name": builder_name,
        "property_types": _normalize_property_types(prop_type_raw),
        "launch_status": _classify_launch_status(data),
        "price_min": price_min,
        "price_max": price_max,
        "price_currency": currency,
        "configurations": _normalize_configurations(configs_raw),
        "locality": locality,
        "city": "Kochi",
        "state": "Kerala",
        "latitude": lat,
        "longitude": lng,
        "super_area_min_sqft": area_sqft,
        "possession_date": possession,
        "rera_number": rera,
        "amenities": amenities,
        "total_units": total_units,
        "images": images,
        "project_url": detail_url,
        "source_url": detail_url,
        "source": "99acres",
    }


def parse_commonfloor_listing(html_text: str) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []
    next_data = _load_next_data(html_text)
    if next_data:
        try:
            listings = (
                next_data.get("props", {})
                .get("pageProps", {})
                .get("projects", [])
            )
            if not listings:
                listings = (
                    next_data.get("props", {})
                    .get("pageProps", {})
                    .get("projectList", [])
                )
            for listing in listings:
                project = _extract_commonfloor_project(listing)
                if project:
                    projects.append(project)
        except Exception as exc:
            logger.warning("Error parsing CommonFloor __NEXT_DATA__: %s", exc)

    if not projects:
        try:
            json_pattern = re.compile(r'"projects"\s*:\s*(\[.*?\])\s*[,}]', re.S)
            match = json_pattern.search(html_text)
            if match:
                listings = json.loads(match.group(1))
                for listing in listings:
                    project = _extract_commonfloor_project(listing)
                    if project:
                        projects.append(project)
        except Exception as exc:
            logger.warning("Error parsing CommonFloor inline JSON: %s", exc)

    return projects


def _extract_commonfloor_project(data: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        return None
    project_name = _as_text(data.get("projectName") or data.get("name") or data.get("title"))
    if not project_name:
        return None
    builder_name = _as_text(data.get("builderName") or data.get("developerName") or data.get("builder"))
    price_text = data.get("priceRange") or data.get("price") or data.get("minPrice")
    price_min, price_max, currency = _parse_price(price_text)
    locality = _as_text(data.get("localityName") or data.get("locality") or data.get("location"))
    prop_type_raw = data.get("propertyType") or data.get("projectType")
    configs_raw = data.get("configuration") or data.get("bhk")
    area_raw = data.get("area") or data.get("size")
    detail_path = data.get("url") or data.get("detailUrl") or data.get("projectUrl")
    detail_url = _normalize_url("https://www.commonfloor.com", detail_path)
    images = _normalize_images(data.get("images") or data.get("image"))
    amenities = _normalize_amenities(data.get("amenities"))
    total_units = _maybe_int(data.get("totalUnits"))
    possession = _as_text(data.get("possessionDate") or data.get("possession"))
    rera = _as_text(data.get("reraId") or data.get("reraNumber"))
    lat = _maybe_float(data.get("lat") or data.get("latitude"))
    lng = _maybe_float(data.get("lng") or data.get("longitude"))
    area_sqft = _parse_area(area_raw)

    return {
        "project_name": project_name,
        "builder_name": builder_name,
        "property_types": _normalize_property_types(prop_type_raw),
        "launch_status": _classify_launch_status(data),
        "price_min": price_min,
        "price_max": price_max,
        "price_currency": currency,
        "configurations": _normalize_configurations(configs_raw),
        "locality": locality,
        "city": "Kochi",
        "state": "Kerala",
        "latitude": lat,
        "longitude": lng,
        "super_area_min_sqft": area_sqft,
        "possession_date": possession,
        "rera_number": rera,
        "amenities": amenities,
        "total_units": total_units,
        "images": images,
        "project_url": detail_url,
        "source_url": detail_url,
        "source": "commonfloor",
    }


def parse_project_detail_page(html_text: str, source: str) -> dict[str, Any]:
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
            break

    next_data = _load_next_data(html_text)
    if next_data:
        best = _find_best_project_detail(next_data)
        if best:
            detail.update(_extract_detail_from_dict(best, source))

    return {key: value for key, value in detail.items() if value not in (None, [], "")}


def _find_best_project_detail(payload: dict[str, Any]) -> dict[str, Any] | None:
    best = None
    best_score = 0.0
    for obj in _iter_objects(payload):
        if not isinstance(obj, dict):
            continue
        score = 0.0
        for key in ("description", "amenities", "features", "projectName", "builderName"):
            if key in obj:
                score += 1.0
        for key in ("price", "priceRange", "images", "gallery"):
            if key in obj:
                score += 1.0
        for key in ("bedrooms", "bathrooms", "area", "possessionDate"):
            if key in obj:
                score += 0.5
        if score > best_score:
            best_score = score
            best = obj
    return best


def _extract_detail_from_dict(data: dict[str, Any], source: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if data.get("projectName") or data.get("name"):
        result["project_name"] = _as_text(data.get("projectName") or data.get("name"))
    if data.get("builderName") or data.get("developerName") or data.get("builder"):
        result["builder_name"] = _as_text(data.get("builderName") or data.get("developerName") or data.get("builder"))
    if data.get("description"):
        result["project_description"] = _as_text(data.get("description"))
    if data.get("amenities"):
        result["amenities"] = _normalize_amenities(data.get("amenities"))
    if data.get("highlights"):
        result["project_highlights"] = _normalize_amenities(data.get("highlights"))
    price_text = data.get("priceRange") or data.get("price")
    if price_text:
        pmin, pmax, pcur = _parse_price(price_text)
        if pmin:
            result["price_min"] = pmin
        if pmax:
            result["price_max"] = pmax
        if pcur:
            result["price_currency"] = pcur
    if data.get("possessionDate") or data.get("possession"):
        result["possession_date"] = _as_text(data.get("possessionDate") or data.get("possession"))
    if data.get("reraId") or data.get("reraNumber") or data.get("rera"):
        result["rera_number"] = _as_text(data.get("reraId") or data.get("reraNumber") or data.get("rera"))
    if data.get("totalUnits"):
        result["total_units"] = _maybe_int(data.get("totalUnits"))
    if data.get("propertyType") or data.get("projectType"):
        result["property_types"] = _normalize_property_types(data.get("propertyType") or data.get("projectType"))
    if data.get("configuration") or data.get("bhk"):
        result["configurations"] = _normalize_configurations(data.get("configuration") or data.get("bhk"))
    if data.get("area") or data.get("size"):
        area = _parse_area(data.get("area") or data.get("size"))
        if area:
            result["super_area_min_sqft"] = area
    if data.get("latitude") or data.get("lat"):
        result["latitude"] = _maybe_float(data.get("latitude") or data.get("lat"))
    if data.get("longitude") or data.get("lng") or data.get("lon"):
        result["longitude"] = _maybe_float(data.get("longitude") or data.get("lng") or data.get("lon"))
    if data.get("localityName") or data.get("locality"):
        result["locality"] = _as_text(data.get("localityName") or data.get("locality"))
    if data.get("images") or data.get("image"):
        images = _normalize_images(data.get("images") or data.get("image"))
        if images:
            result["images"] = images
    return result


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


def _page_text_to_ai_input(html_text: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", "", html_text, flags=re.S)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:8000]


class InputParams(BaseModel):
    pass


@task
async def discover_kochi_projects(params: InputParams = None) -> list[dict[str, Any]]:
    session, use_curl = _build_http_client()
    discovered: list[dict[str, Any]] = []
    now = _to_iso_now()

    for query in SETTINGS.google_queries:
        for page in range(SETTINGS.google_pages):
            start = page * 10
            url = f"https://www.google.com/search?q={quote_plus(query)}&start={start}"
            logger.info("Google search: %s (page %d)", query, page + 1)
            try:
                html_text = _fetch(session, url, retries=2, use_curl_cffi=use_curl)
                results = parse_google_serp(html_text)
                for r in results:
                    discovered.append({
                        "project_name": r.get("name", ""),
                        "project_url": r["url"],
                        "source_url": r["url"],
                        "source": "google",
                        "discovered_at": now,
                    })
                logger.info("Found %d Google results for '%s' page %d", len(results), query, page + 1)
            except Exception as exc:
                logger.warning("Google search failed for '%s' page %d: %s", query, page + 1, exc)
            await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))

    portal_configs = [
        ("magicbricks", SETTINGS.magicbricks_url, parse_magicbricks_listing),
        ("housing", SETTINGS.housing_url, parse_housing_listing),
        ("99acres", SETTINGS.acres99_url, parse_99acres_listing),
        ("commonfloor", SETTINGS.commonfloor_url, parse_commonfloor_listing),
    ]
    for portal_name, base_url, parser in portal_configs:
        logger.info("Scraping portal: %s", portal_name)
        try:
            html_text = _fetch(session, base_url, retries=2, use_curl_cffi=use_curl)
            projects = parser(html_text)
            for project in projects:
                project["discovered_at"] = now
            discovered.extend(projects)
            logger.info("Found %d projects from %s", len(projects), portal_name)

            for page in range(2, SETTINGS.pages + 1):
                page_url = f"{base_url}?page={page}" if "?" not in base_url else f"{base_url}&page={page}"
                try:
                    html_text = _fetch(session, page_url, retries=1, use_curl_cffi=use_curl)
                    projects = parser(html_text)
                    if not projects:
                        logger.info("No more projects from %s page %d, stopping.", portal_name, page)
                        break
                    for project in projects:
                        project["discovered_at"] = now
                    discovered.extend(projects)
                    logger.info("Found %d projects from %s page %d", len(projects), portal_name, page)
                except Exception as exc:
                    logger.warning("Failed to fetch %s page %d: %s", portal_name, page, exc)
                    break
                await asyncio.sleep(random.uniform(SETTINGS.min_delay, SETTINGS.max_delay))
        except Exception as exc:
            logger.warning("Failed to scrape %s: %s", portal_name, exc)

    logger.info("Total discovered: %d raw project entries", len(discovered))
    out_dir = Path("saved_data/kochi_launches")
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(discovered)
    df.to_json(out_dir / "discovered_raw.json", orient="records", force_ascii=False, indent=2)
    return discovered


@task
async def enrich_project_details(discovered: list[dict[str, Any]]) -> list[dict[str, Any]]:
    session, use_curl = _build_http_client()
    enriched: list[dict[str, Any]] = []

    for i, project in enumerate(discovered):
        project_name = project.get("project_name", "")
        detail_url = project.get("project_url") or project.get("source_url")
        source = project.get("source", "unknown")
        logger.info("Enriching project %d/%d: %s (%s)", i + 1, len(discovered), project_name, source)

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
                        page_text = _page_text_to_ai_input(html_text)
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
    description="Discover, enrich, classify and index pre-launch & new-launch property projects in Kochi, Kerala into Elasticsearch.",
    tasks=[discover_kochi_projects, enrich_project_details, standardize_and_index, ai_classify_builders],
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
