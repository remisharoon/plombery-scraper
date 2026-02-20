"""99acres property scraper and pipeline.

This module mirrors the Allsopp pipeline but targets 99acres search pages.
It scrapes paginated listing pages, enriches records with detail-page data,
stores results in Elasticsearch, and exports a JSON snapshot to Cloudflare R2.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
import requests

try:  # pragma: no cover - optional dependency
    from curl_cffi import requests as curl_requests
except Exception:  # pragma: no cover - gracefully degrade
    curl_requests = None

from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
import boto3
from pydantic import BaseModel

from plombery import Trigger, register_pipeline, task
from .config import read_config


logger = logging.getLogger(__name__)


ID_KEYS = (
    "id",
    "propId",
    "propertyId",
    "property_id",
    "listingId",
    "pid",
    "PropID",
    "PROP_ID",
)
URL_KEYS = (
    "url",
    "propertyUrl",
    "detailUrl",
    "detail_url",
    "permalink",
    "landingUrl",
    "pdUrl",
    "seoUrl",
    "absolute_url",
    "propertyLink",
)
TITLE_KEYS = (
    "title",
    "propertyTitle",
    "heading",
    "name",
    "projectName",
    "societyName",
    "displayName",
)
PRICE_KEYS = (
    "price",
    "priceValue",
    "minPrice",
    "maxPrice",
    "listingPrice",
    "cost",
    "amount",
    "priceText",
    "priceLabel",
    "priceStr",
)
PRICE_PER_SQFT_KEYS = (
    "pricePerUnitArea",
    "pricePerSqft",
    "pricePerSqFt",
    "price_per_sqft",
    "ratePerSqft",
)
AREA_KEYS = (
    "area",
    "builtUpArea",
    "superBuiltupArea",
    "carpetArea",
    "plotArea",
    "size",
    "areaSqft",
    "area_sqft",
    "coveredArea",
)
BHK_KEYS = (
    "bhk",
    "bedrooms",
    "bedroom",
    "bedroomCount",
    "noOfBedrooms",
    "bedroom_num",
    "numBedrooms",
)
BATH_KEYS = (
    "bathrooms",
    "bathroom",
    "bathroomCount",
    "noOfBathrooms",
    "numBathrooms",
)
TYPE_KEYS = (
    "propertyType",
    "propType",
    "property_type",
    "type",
    "subType",
)
STATUS_KEYS = (
    "status",
    "constructionStatus",
    "possessionStatus",
    "listingStatus",
)
CITY_KEYS = (
    "city",
    "cityName",
)
LOCALITY_KEYS = (
    "locality",
    "localityName",
    "areaName",
    "location",
    "address",
)
IMAGE_KEYS = (
    "images",
    "image",
    "photos",
    "gallery",
    "imageList",
)


PRICE_MULTIPLIERS = {
    "cr": 10_000_000,
    "crore": 10_000_000,
    "lac": 100_000,
    "lakh": 100_000,
    "l": 100_000,
    "k": 1_000,
    "m": 1_000_000,
}


REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

MODERN_USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.78 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.60 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.6312.122 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.6422.60 Safari/537.36",
)

IMPERSONATE_IDS = (
    "chrome110",
    "chrome120",
    "chrome124",
    "edge110",
)

BLOCKED_MARKERS = (
    "captcha",
    "verify you are a human",
    "access denied",
    "unusual traffic",
    "blocked",
)


@dataclass(slots=True)
class Acres99BaseSettings:
    min_delay_seconds: float
    max_delay_seconds: float
    detail_retry_count: int
    request_timeout: int
    stop_on_existing: bool


@dataclass(slots=True)
class Acres99JobSettings:
    name: str
    listing_url_template: str
    pages: int
    es_index: str


@dataclass(slots=True)
class HttpClient:
    session: Any
    use_curl_cffi: bool


config = read_config()

CONFIG_SECTION = "acres99"
acres_section = config[CONFIG_SECTION]

if not acres_section:
    raise KeyError("Missing [acres99] section in config.ini")

_es_config = config["elasticsearch"]

min_delay = float(acres_section.get("min_delay_seconds", "1.5"))
max_delay = float(acres_section.get("max_delay_seconds", "4.0"))
detail_retry_count = int(acres_section.get("detail_retry_count", "3"))
default_pages = int(acres_section.get("pages", "1"))
request_timeout = int(acres_section.get("request_timeout_seconds", "25"))
stop_on_existing = acres_section.get("stop_on_existing", "true").strip().lower() != "false"
default_es_index = acres_section.get("es_index") or _es_config.get("properties_index", "acres99_properties")

BASE_SETTINGS = Acres99BaseSettings(
    min_delay_seconds=min_delay,
    max_delay_seconds=max_delay,
    detail_retry_count=detail_retry_count,
    request_timeout=request_timeout,
    stop_on_existing=stop_on_existing,
)


def _build_job_settings(
    section: Mapping[str, str],
    *,
    name: str,
    url_key: str,
    pages_key: str,
    es_index_key: str,
    default_pages_value: int,
    default_index: str,
) -> Acres99JobSettings | None:
    listing_url = section.get(url_key)
    if not listing_url:
        return None
    pages_val = int(section.get(pages_key, str(default_pages_value)))
    index_val = section.get(es_index_key) or default_index
    return Acres99JobSettings(
        name=name,
        listing_url_template=listing_url,
        pages=pages_val,
        es_index=index_val,
    )


JOB_SETTINGS: list[Acres99JobSettings] = []

buy_job = _build_job_settings(
    acres_section,
    name="buy",
    url_key="listing_url",
    pages_key="pages",
    es_index_key="es_index",
    default_pages_value=default_pages,
    default_index=default_es_index,
)
if buy_job:
    JOB_SETTINGS.append(buy_job)

rent_job = _build_job_settings(
    acres_section,
    name="rent",
    url_key="rent_listing_url",
    pages_key="rent_pages",
    es_index_key="rent_es_index",
    default_pages_value=default_pages,
    default_index=default_es_index,
)
if rent_job:
    JOB_SETTINGS.append(rent_job)

if not JOB_SETTINGS:
    raise KeyError("No 99acres listing URLs configured (expected listing_url or rent_listing_url)")


es_hosts = _es_config["host"].split(",")
es_user = _es_config["username"]
es_password = _es_config["password"]


def _build_http_client() -> HttpClient:
    if curl_requests is not None:
        try:
            session = curl_requests.Session()
            return HttpClient(session=session, use_curl_cffi=True)
        except Exception:  # pragma: no cover - fallback to requests
            logger.exception("Failed to initialise curl_cffi session; falling back to requests.")
    return HttpClient(session=requests.Session(), use_curl_cffi=False)


def _session_get(client: HttpClient, url: str, *, timeout: int) -> requests.Response:
    headers = dict(REQUEST_HEADERS)
    headers["User-Agent"] = random.choice(MODERN_USER_AGENTS)
    if client.use_curl_cffi:
        impersonate = random.choice(IMPERSONATE_IDS)
        return client.session.get(url, headers=headers, timeout=timeout, impersonate=impersonate)
    return client.session.get(url, headers=headers, timeout=timeout)


def _looks_blocked(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in BLOCKED_MARKERS)


def _fetch(
    client: HttpClient,
    url: str,
    *,
    retries: int = 1,
    timeout: int | None = None,
    base_settings: Acres99BaseSettings | None = None,
) -> str:
    settings = base_settings or BASE_SETTINGS
    last_exc: Exception | None = None
    timeout = timeout or settings.request_timeout
    for attempt in range(retries):
        try:
            response = _session_get(client, url, timeout=timeout)
            response.raise_for_status()
            text = response.text
            if _looks_blocked(text):
                raise RuntimeError("Received a bot-protection response.")
            return text
        except Exception as exc:  # pragma: no cover - networking best effort
            last_exc = exc
            sleep_for = settings.min_delay_seconds * (attempt + 1)
            logger.warning("Request failed (%s). Retrying in %.1fs", exc, sleep_for)
            time.sleep(sleep_for)
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


def _load_next_data(html_text: str) -> dict[str, Any]:
    match = re.search(
        r'<script[^>]+id=["\\\']__NEXT_DATA__["\\\'][^>]*>(.*?)</script>',
        html_text,
        re.S | re.I,
    )
    if not match:
        raise ValueError("Unable to locate __NEXT_DATA__ script")
    payload = match.group(1)
    return json.loads(payload)


def _iter_jsonld_objects(html_text: str) -> Iterable[dict[str, Any]]:
    pattern = re.compile(r'<script[^>]+type=["\\\'\']application/ld\+json["\\\'\'][^>]*>(.*?)</script>', re.S | re.I)
    for match in pattern.finditer(html_text):
        raw = match.group(1).strip()
        if not raw:
            continue
        try:
            payload = json.loads(html.unescape(raw))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    yield item
        elif isinstance(payload, dict):
            yield payload


def _iter_objects(obj: Any) -> Iterable[Any]:
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _iter_objects(value)
    elif isinstance(obj, list):
        yield obj
        for item in obj:
            yield from _iter_objects(item)


def _score_listing_dict(item: dict[str, Any]) -> float:
    if not isinstance(item, dict):
        return 0.0
    keyset = set(item.keys())
    score = 0.0
    if keyset & set(ID_KEYS):
        score += 3.0
    if keyset & set(URL_KEYS):
        score += 3.0
    if keyset & set(PRICE_KEYS):
        score += 2.0
    if keyset & set(TITLE_KEYS):
        score += 1.5
    if keyset & set(AREA_KEYS):
        score += 1.0
    if keyset & set(BHK_KEYS):
        score += 1.0
    if keyset & set(BATH_KEYS):
        score += 1.0
    return score


def _find_best_listing_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    best_list: list[dict[str, Any]] = []
    best_score = 0.0
    for obj in _iter_objects(payload):
        if not isinstance(obj, list) or len(obj) < 3:
            continue
        if not all(isinstance(item, dict) for item in obj):
            continue
        scores = [_score_listing_dict(item) for item in obj]
        if not scores:
            continue
        avg_score = sum(scores) / len(scores)
        if avg_score > best_score:
            best_list = list(obj)
            best_score = avg_score
    return best_list


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.upper() == "NULL":
            return None
        return stripped
    return value


def _unwrap_value(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    if isinstance(value, dict):
        for key in ("value", "label", "name", "text"):
            if key in value and value[key] not in (None, "", []):
                return value[key]
    return value


def _as_text(value: Any) -> str | None:
    value = _unwrap_value(value)
    value = _normalize_scalar(value)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(value)
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
        return int(value), None, None
    text = _as_text(value)
    if not text:
        return None, None, None
    lowered = text.lower()
    if "price on request" in lowered:
        return None, None, None
    currency = None
    if "\u20b9" in text or "inr" in lowered or "rs" in lowered:
        currency = "INR"
    cleaned = text.replace("\u20b9", " ")
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


def _parse_area(value: Any) -> tuple[float | None, float | None]:
    if value is None:
        return None, None
    if isinstance(value, (int, float)):
        return float(value), None
    text = _as_text(value)
    if not text:
        return None, None
    match = re.search(r"([0-9]+(?:[\\.,][0-9]+)?)\s*(sq\.?\s*ft|sqft|sq\.?\s*m|sqm)", text, re.I)
    if not match:
        return _maybe_float(text), None
    number = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower().replace(" ", "")
    if "sqft" in unit:
        return number, None
    if "sqm" in unit or "sq.m" in unit:
        return None, number
    return number, None


def _normalize_url(value: Any) -> str | None:
    text = _as_text(value)
    if not text:
        return None
    if text.startswith("//"):
        return f"https:{text}"
    if text.startswith("/"):
        return f"https://www.99acres.com{text}"
    if text.startswith("http"):
        return text
    return f"https://www.99acres.com/{text.lstrip('/')}"


def _extract_images(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        images = [img for img in (str(v).strip() for v in value) if img]
        return images or None
    text = _as_text(value)
    if not text:
        return None
    return [text]


def _pick(data: Mapping[str, Any] | None, keys: Iterable[str]) -> Any:
    if not isinstance(data, Mapping):
        return None
    for key in keys:
        if key in data and data[key] not in (None, "", []):
            return data[key]
    return None


def _stable_id(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _listing_from_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    raw_id = _pick(candidate, ID_KEYS)
    raw_url = _pick(candidate, URL_KEYS)
    raw_title = _pick(candidate, TITLE_KEYS)

    offers = candidate.get("offers") if isinstance(candidate.get("offers"), dict) else {}
    if not offers:
        offers = {}

    price_value = _pick(candidate, PRICE_KEYS)
    if price_value is None and offers:
        price_value = _pick(offers, ("price", "lowPrice", "highPrice", "priceValue"))

    price_min, price_max, currency = _parse_price(price_value or candidate.get("priceRange"))

    price_per_sqft = _pick(candidate, PRICE_PER_SQFT_KEYS)
    price_per_sqft_val = _maybe_float(price_per_sqft)

    area_value = _pick(candidate, AREA_KEYS)
    area_sqft, area_sqm = _parse_area(area_value)

    bedrooms = _maybe_int(_pick(candidate, BHK_KEYS))
    bathrooms = _maybe_int(_pick(candidate, BATH_KEYS))
    property_type = _as_text(_pick(candidate, TYPE_KEYS))
    listing_status = _as_text(_pick(candidate, STATUS_KEYS))

    city = _as_text(_pick(candidate, CITY_KEYS))
    locality = _as_text(_pick(candidate, LOCALITY_KEYS))

    latitude = _maybe_float(candidate.get("latitude") or candidate.get("lat"))
    longitude = _maybe_float(candidate.get("longitude") or candidate.get("lng") or candidate.get("lon"))

    images = _extract_images(_pick(candidate, IMAGE_KEYS))
    if not images and offers:
        images = _extract_images(offers.get("image"))

    detail_url = _normalize_url(raw_url)

    record_id = _as_text(raw_id)
    if not record_id and detail_url:
        record_id = _stable_id(detail_url)
    if not record_id and raw_title:
        record_id = _stable_id(str(raw_title))

    listing: dict[str, Any] = {
        "id": record_id,
        "title": _as_text(raw_title),
        "price_min": price_min,
        "price_max": price_max,
        "price_currency": currency,
        "price_per_sqft": price_per_sqft_val,
        "area_sqft": area_sqft,
        "area_sqm": area_sqm,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "property_type": property_type,
        "listing_status": listing_status,
        "city": city,
        "locality": locality,
        "latitude": latitude,
        "longitude": longitude,
        "detail_url": detail_url,
        "images": images,
        "raw_listing": candidate,
    }
    return {key: value for key, value in listing.items() if value not in (None, [], "")}


def _listing_from_jsonld(item: dict[str, Any]) -> dict[str, Any]:
    candidate = dict(item)
    if "item" in candidate and isinstance(candidate["item"], dict):
        inner = candidate["item"]
        candidate.update(inner)
    return _listing_from_candidate(candidate)


def parse_99acres_listing_page(html_text: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []

    # Strategy 1: JSON bootstrap payloads
    try:
        next_data = _load_next_data(html_text)
        candidates = _find_best_listing_list(next_data)
        for candidate in candidates:
            listing = _listing_from_candidate(candidate)
            if listing:
                records.append(listing)
    except Exception:  # pragma: no cover - handled by fallback parsers
        pass

    # Strategy 2: JSON-LD ItemList
    if not records:
        for obj in _iter_jsonld_objects(html_text):
            if str(obj.get("@type")).lower() == "itemlist":
                for item in obj.get("itemListElement", []) or []:
                    if isinstance(item, dict):
                        listing = _listing_from_jsonld(item)
                        if listing:
                            records.append(listing)

    if not records:
        return pd.DataFrame()

    deduped: dict[str, dict[str, Any]] = {}
    for record in records:
        rec_id = record.get("id") or record.get("detail_url")
        if rec_id:
            deduped[str(rec_id)] = record
    return pd.DataFrame.from_records(list(deduped.values()))


def _to_epoch_and_iso(value: Any) -> tuple[int | None, str | None]:
    if value is None:
        return None, None
    if isinstance(value, (int, float)):
        epoch = int(value)
        try:
            iso = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
        except Exception:
            iso = None
        return epoch, iso
    text = _as_text(value)
    if not text:
        return None, None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp()), dt.isoformat()
    except Exception:
        return None, None


def _score_detail_dict(item: dict[str, Any]) -> float:
    if not isinstance(item, dict):
        return 0.0
    keyset = set(item.keys())
    score = 0.0
    for key in ("description", "amenities", "features", "address", "location"):
        if key in keyset:
            score += 1.0
    for key in ("price", "priceValue", "offers", "images", "gallery"):
        if key in keyset:
            score += 1.0
    for key in ("bedrooms", "bathrooms", "area", "areaSqft", "propertyType"):
        if key in keyset:
            score += 0.5
    return score


def _find_best_detail(payload: dict[str, Any]) -> dict[str, Any] | None:
    best = None
    best_score = 0.0
    for obj in _iter_objects(payload):
        if not isinstance(obj, dict):
            continue
        score = _score_detail_dict(obj)
        if score > best_score:
            best_score = score
            best = obj
    return best


def _detail_from_jsonld(obj: dict[str, Any]) -> dict[str, Any]:
    offers = obj.get("offers") if isinstance(obj.get("offers"), dict) else {}
    price_value = _pick(offers, ("price", "lowPrice", "highPrice"))
    price_min, price_max, currency = _parse_price(price_value)

    address = obj.get("address") if isinstance(obj.get("address"), dict) else {}
    geo = obj.get("geo") if isinstance(obj.get("geo"), dict) else {}

    detail: dict[str, Any] = {
        "detail_name": _as_text(obj.get("name")),
        "detail_description": _as_text(obj.get("description")),
        "detail_price_min": price_min,
        "detail_price_max": price_max,
        "detail_price_currency": currency or _as_text(offers.get("priceCurrency")),
        "detail_images": _extract_images(obj.get("image")),
        "detail_address": _as_text(address.get("streetAddress")),
        "detail_locality": _as_text(address.get("addressLocality")),
        "detail_region": _as_text(address.get("addressRegion")),
        "detail_postal_code": _as_text(address.get("postalCode")),
        "detail_latitude": _maybe_float(geo.get("latitude")),
        "detail_longitude": _maybe_float(geo.get("longitude")),
    }
    return {key: value for key, value in detail.items() if value not in (None, [], "")}


def parse_99acres_detail_page(html_text: str) -> dict[str, Any]:
    detail: dict[str, Any] = {}

    for obj in _iter_jsonld_objects(html_text):
        obj_type = str(obj.get("@type", "")).lower()
        if obj_type and obj_type != "itemlist":
            detail.update(_detail_from_jsonld(obj))
            break

    try:
        next_data = _load_next_data(html_text)
    except Exception:
        next_data = None

    if next_data:
        best = _find_best_detail(next_data)
        if isinstance(best, dict):
            detail.update(
                {
                    "detail_bedrooms": _maybe_int(_pick(best, BHK_KEYS)),
                    "detail_bathrooms": _maybe_int(_pick(best, BATH_KEYS)),
                    "detail_property_type": _as_text(_pick(best, TYPE_KEYS)),
                    "detail_status": _as_text(_pick(best, STATUS_KEYS)),
                    "detail_city": _as_text(_pick(best, CITY_KEYS)),
                    "detail_locality": _as_text(_pick(best, LOCALITY_KEYS)),
                    "detail_images": _extract_images(_pick(best, IMAGE_KEYS)) or detail.get("detail_images"),
                    "detail_raw": best,
                }
            )
            area_sqft, area_sqm = _parse_area(_pick(best, AREA_KEYS))
            if area_sqft is not None:
                detail["detail_area_sqft"] = area_sqft
            if area_sqm is not None:
                detail["detail_area_sqm"] = area_sqm

            for key in ("postedAt", "createdAt", "updatedAt", "postedDate", "creationDate", "updateDate"):
                if key in best:
                    epoch, iso = _to_epoch_and_iso(best.get(key))
                    detail["detail_updated_epoch"] = epoch
                    detail["detail_updated_iso"] = iso
                    break

    return {key: value for key, value in detail.items() if value not in (None, [], "")}


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
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to fetch ES info: %s", exc)
    return es


def ensure_index(es: Elasticsearch, index: str) -> None:
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "5s",
        },
        "mappings": {
            "dynamic": True,
            "dynamic_templates": [
                {"dates_iso": {"match": "*_iso", "mapping": {"type": "date", "format": "strict_date_time"}}},
                {"epochs": {"match": "*_epoch", "mapping": {"type": "long"}}},
                {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword", "ignore_above": 256}}},
                {"doubleNums": {"match_mapping_type": "double", "mapping": {"type": "double"}}},
                {"longNums": {"match_mapping_type": "long", "mapping": {"type": "long"}}},
            ],
            "properties": {
                "id": {"type": "keyword"},
                "detail_url": {"type": "keyword"},
                "title": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
                "locality": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
                "city": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
                "source_page": {"type": "integer"},
                "source": {"type": "keyword"},
            },
        },
    }
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=mapping)


def es_doc_exists(es: Elasticsearch, index: str, doc_id: str) -> bool:
    try:
        return bool(es.exists(index=index, id=doc_id))
    except Exception:
        return False


def df_to_actions(df: pd.DataFrame, default_index: str | None) -> Iterable[dict[str, Any]]:
    clean = df.replace({np.nan: None})
    for record in clean.to_dict(orient="records"):
        target_index = record.pop("_target_index", None) or default_index
        if not target_index:
            raise ValueError("Missing target index for 99acres document")
        doc_id = record.get("id")
        if not doc_id:
            continue
        yield {
            "_index": target_index,
            "_id": doc_id,
            "_op_type": "index",
            "_source": record,
        }


async def _fetch_detail_with_retry(
    client: HttpClient,
    url: str | None,
    base_settings: Acres99BaseSettings,
) -> dict[str, Any]:
    if not url:
        return {}
    for attempt in range(base_settings.detail_retry_count):
        try:
            html_text = _fetch(client, url, retries=1, base_settings=base_settings)
            detail = parse_99acres_detail_page(html_text)
            if detail:
                return detail
        except Exception as exc:  # pragma: no cover - logged for observability
            logger.warning(
                "Detail fetch failed for %s (attempt %s/%s): %s",
                url,
                attempt + 1,
                base_settings.detail_retry_count,
                exc,
            )
        await asyncio.sleep(random.uniform(base_settings.min_delay_seconds, base_settings.max_delay_seconds))
    return {}


async def _scrape_job(
    client: HttpClient,
    es: Elasticsearch,
    job: Acres99JobSettings,
) -> list[dict[str, Any]]:
    ensure_index(es, job.es_index)

    job_rows: list[dict[str, Any]] = []
    done = False

    for page in range(1, job.pages + 1):
        url = job.listing_url_template.format(page=page)
        logger.info("Fetching 99acres %s listing page %s", job.name, url)
        listing_html = _fetch(client, url, retries=2, base_settings=BASE_SETTINGS)

        df = parse_99acres_listing_page(listing_html)
        if df.empty:
            logger.info("No %s listings parsed from page %s, stopping job.", job.name, page)
            break
        logger.info("Parsed %d 99acres %s listings from page %d", len(df), job.name, page)

        for record in df.to_dict(orient="records"):
            record.setdefault("listing_category", job.name)
            record["listing_category"] = str(record.get("listing_category") or job.name)
            record["source_page"] = page
            rec_id = str(record.get("id")) if record.get("id") is not None else None
            if rec_id and BASE_SETTINGS.stop_on_existing and es_doc_exists(es, job.es_index, rec_id):
                logger.info("Encountered existing listing id=%s in job=%s; stopping pagination.", rec_id, job.name)
                done = True
                break

            detail_payload = await _fetch_detail_with_retry(client, record.get("detail_url"), BASE_SETTINGS)
            for key, value in detail_payload.items():
                if value is not None:
                    record[key] = value
            record["source"] = "99acres"
            record["_target_index"] = job.es_index
            job_rows.append(record)

            await asyncio.sleep(random.uniform(BASE_SETTINGS.min_delay_seconds, BASE_SETTINGS.max_delay_seconds))

        out_dir = Path("saved_data/99acres") / job.name
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"page_{page}.csv"
        df.to_csv(csv_path, index=False)
        logger.info("Saved raw %s listings to %s", job.name, csv_path)

        if done:
            break

    return job_rows


@task
async def acres99_property_data() -> None:
    client = _build_http_client()
    es = es_client()

    all_rows: list[dict[str, Any]] = []

    for job in JOB_SETTINGS:
        job_rows = await _scrape_job(client, es, job)
        if not job_rows:
            logger.info("No rows collected for 99acres job %s", job.name)
            continue

        job_df = pd.DataFrame(job_rows)
        job_df = job_df.drop_duplicates(subset=["id", "listing_category"], keep="last").reset_index(drop=True)

        logger.info(
            "Indexing %d 99acres %s documents into ES index %s",
            len(job_df),
            job.name,
            job.es_index,
        )
        bulk_resp = helpers.bulk(
            es,
            df_to_actions(job_df, default_index=None),
            chunk_size=500,
            request_timeout=120,
            raise_on_error=False,
            raise_on_exception=False,
        )
        logger.info("ES bulk response for job %s: %s", job.name, bulk_resp)

        all_rows.extend(job_rows)

    final_df = pd.DataFrame(all_rows)
    if final_df.empty:
        raise ValueError("No 99acres records collected across configured jobs")

    final_df = final_df.drop_duplicates(subset=["id", "listing_category"], keep="last").reset_index(drop=True)

    out_json = Path("acres99_listings.json")
    json_df = final_df.drop(columns=["_target_index"], errors="ignore")
    json_df.to_json(out_json, orient="records", force_ascii=True)
    logger.info("Wrote %s with %d rows", out_json, len(json_df))


class InputParams(BaseModel):
    """Pipeline parameters placeholder (no external inputs required)."""


def fetch_all_docs(es: Elasticsearch, index: str, fields: list[str] | None = None):
    query: dict[str, Any] = {"query": {"match_all": {}}}
    if fields:
        query["_source"] = fields
    for doc in helpers.scan(es, index=index, query=query, preserve_order=False, size=1000):
        yield doc.get("_source", {})


@task
async def export_acres99_json_to_r2():
    cloudflare_config = config["cloudflare"]

    es = es_client()

    rows: list[dict[str, Any]] = []
    indices = {job.es_index for job in JOB_SETTINGS}
    for index in indices:
        if not es.indices.exists(index=index):
            logger.warning("Index not found: %s - skipping export for this index.", index)
            continue
        rows.extend(fetch_all_docs(es, index))

    if not rows:
        raise SystemExit("No documents returned from ES.")

    df = pd.DataFrame.from_records(rows)
    out_path = Path("acres99_listings.json")
    df.to_json(out_path, orient="records", force_ascii=True)
    logger.info("Wrote %s with %d rows and %d columns", out_path, len(df), len(df.columns))

    session = boto3.session.Session()
    s3 = session.client(
        service_name="s3",
        endpoint_url=cloudflare_config["R2_ENDPOINT"],
        aws_access_key_id=cloudflare_config["ACCESS_KEY_ID"],
        aws_secret_access_key=cloudflare_config["SECRET_ACCESS_KEY"],
    )

    bucket = cloudflare_config.get("PROP_BUCKET") or cloudflare_config.get("BUCKET")
    if not bucket:
        raise KeyError("Missing PROP_BUCKET/BUCKET configuration for Cloudflare export")
    key = "data/acres99_listings.json"
    s3.upload_file(
        Filename=str(out_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs={
            "ContentType": "application/json",
            "ACL": "public-read",
            "CacheControl": "public, max-age=60",
        },
    )
    logger.info("Uploaded to r2://%s/%s", bucket, key)


register_pipeline(
    id="acres99_pipeline",
    description="Scrape 99acres listings, enrich with detail data, and export snapshots.",
    tasks=[acres99_property_data, export_acres99_json_to_r2],
    triggers=[
        Trigger(
            id="acres99_daily",
            name="99acres Daily",
            description="Run 99acres scraper daily at 05:30 India time",
            params=InputParams(),
            schedule=CronTrigger(hour="5", minute="30", timezone="Asia/Kolkata"),
        )
    ],
    params=InputParams,
)
