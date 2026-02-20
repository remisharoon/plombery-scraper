"""Allsopp & Allsopp property scraper and pipeline.

This module mirrors the structure used by the Carswitch scraper, but targets
residential sales listings on allsoppandallsopp.com. It scrapes paginated
listing pages, enriches every new record with detail-page data, stores the
results in Elasticsearch, and ships a trimmed JSON snapshot to Cloudflare R2
for downstream dashboards.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import requests
from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
import boto3
from pydantic import BaseModel

from plombery import Trigger, register_pipeline, task
from config import read_config


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AllsoppSettings:
    listing_url_template: str
    pages: int
    min_delay_seconds: float
    max_delay_seconds: float
    detail_retry_count: int
    es_index: str


config = read_config()

CONFIG_SECTION = "allsopp"
allsopp_section = config[CONFIG_SECTION]

if not allsopp_section:
    raise KeyError("Missing [allsopp] section in config.ini")

listing_url_template = allsopp_section["listing_url"]
pages = int(allsopp_section.get("pages", "1"))
min_delay = float(allsopp_section.get("min_delay_seconds", "1.5"))
max_delay = float(allsopp_section.get("max_delay_seconds", "4.0"))
detail_retry_count = int(allsopp_section.get("detail_retry_count", "3"))

_es_config = config["elasticsearch"]
es_index = allsopp_section.get("es_index") or _es_config.get("properties_index", "allsopp_properties")

SETTINGS = AllsoppSettings(
    listing_url_template=listing_url_template,
    pages=pages,
    min_delay_seconds=min_delay,
    max_delay_seconds=max_delay,
    detail_retry_count=detail_retry_count,
    es_index=es_index,
)

es_hosts = _es_config["host"].split(",")
es_user = _es_config["username"]
es_password = _es_config["password"]


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}


KEEP_LIST_FIELDS = {
    "images",
    "pba_uaefields__private_amenities__c",
    "pba_uaefields__commercial_amenities__c",
}


def _load_next_data(html_text: str) -> dict[str, Any]:
    marker = '<script id="__NEXT_DATA__"'
    idx = html_text.find(marker)
    if idx == -1:
        raise ValueError("Unable to locate __NEXT_DATA__ script")
    start = html_text.find(">", idx)
    if start == -1:
        raise ValueError("Malformed __NEXT_DATA__ script tag")
    start += 1
    end = html_text.find("</script>", start)
    if end == -1:
        raise ValueError("Unable to locate end of __NEXT_DATA__ script")
    payload = html_text[start:end]
    return json.loads(payload)


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.upper() == "NULL":
            return None
        return stripped
    return value


def _normalize_value(value: Any, *, keep_list: bool) -> Any:
    if isinstance(value, list):
        normalized_items = [_normalize_scalar(item) for item in value]
        normalized_items = [item for item in normalized_items if item is not None]
        if keep_list:
            return normalized_items or None
        return normalized_items[0] if normalized_items else None
    return _normalize_scalar(value)


def _flatten_fields(data: dict[str, Any], *, keep_list_keys: set[str] | None = None) -> dict[str, Any]:
    keep_list_keys = keep_list_keys or set()
    flattened: dict[str, Any] = {}
    for key, value in data.items():
        keep_list = key in keep_list_keys
        flattened[key] = _normalize_value(value, keep_list=keep_list)
    return flattened


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return None


def _maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _clean_location(value: Any) -> str | None:
    text = value[0] if isinstance(value, list) and value else value
    text = _normalize_scalar(text)
    if not text:
        return None
    return text.lstrip(", ").strip()


def _build_detail_url(reference: str | None) -> str | None:
    if not reference:
        return None
    return f"https://www.allsoppandallsopp.com/dubai/property/sales/{reference}"


def parse_allsopp_listing_page(html_text: str) -> pd.DataFrame:
    try:
        next_data = _load_next_data(html_text)
    except Exception as exc:  # pragma: no cover - handled in caller
        logger.warning("Failed to decode listing page: %s", exc)
        return pd.DataFrame()

    hits = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("data", {})
        .get("data", {})
        .get("hits", [])
    )
    if not hits:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for hit in hits:
        fields = _flatten_fields(hit.get("fields") or {}, keep_list_keys=KEEP_LIST_FIELDS)
        record_id = fields.get("id") or hit.get("_id") or fields.get("pba__broker_s_listing_id__c") or fields.get("pba__property__c")
        if not record_id:
            continue
        reference_number = fields.get("pba__broker_s_listing_id__c")
        listing = {
            "id": str(record_id),
            "reference_number": reference_number,
            "price": _maybe_int(fields.get("pba__listingprice_pb__c")),
            "bedrooms": _maybe_int(fields.get("pba__bedrooms_pb__c")),
            "bathrooms": _maybe_int(fields.get("pba__fullbathrooms_pb__c")),
            "total_area_sqft": _maybe_float(fields.get("pba__totalarea_pb__c")),
            "listing_area": _clean_location(fields.get("listing_area")),
            "property_type": fields.get("property_type_website__c"),
            "listing_status": fields.get("pba__status__c"),
            "listing_type": fields.get("pba__listingtype__c"),
            "business_type": fields.get("business_type_aa__c"),
            "latitude": _maybe_float(fields.get("pba__latitude_pb__c")),
            "longitude": _maybe_float(fields.get("pba__longitude_pb__c")),
            "listing_agent_name": fields.get("listing_agent_name"),
            "listing_agent_mobile": fields.get("listing_agent_mobile"),
            "listing_agent_email": fields.get("listing_agent_Email"),
            "property_video": fields.get("property_video"),
            "images": fields.get("images"),
            "detail_url": _build_detail_url(reference_number),
            "name": fields.get("name"),
            "property_id": fields.get("pba__property__c"),
        }
        cleaned = {key: value for key, value in listing.items() if value not in (None, [], "")}
        records.append(cleaned)

    return pd.DataFrame.from_records(records)


def _to_epoch_and_iso(value: Any) -> tuple[int | None, str | None]:
    text = _normalize_scalar(value)
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


def parse_allsopp_detail_page(html_text: str) -> dict[str, Any]:
    try:
        next_data = _load_next_data(html_text)
    except Exception as exc:  # pragma: no cover - handled in caller
        logger.warning("Failed to decode detail page: %s", exc)
        return {}

    listing_hits = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("data", {})
        .get("data", {})
        .get("listingDetails", {})
        .get("hits", {})
        .get("hits", [])
    )
    if not listing_hits:
        return {}

    fields = _flatten_fields(listing_hits[0].get("fields") or {}, keep_list_keys=KEEP_LIST_FIELDS)
    detail: dict[str, Any] = {
        "detail_id": fields.get("id"),
        "detail_reference_number": fields.get("pba__broker_s_listing_id__c"),
        "detail_name": fields.get("name"),
        "detail_price": _maybe_int(fields.get("pba__listingprice_pb__c")),
        "detail_bedrooms": _maybe_int(fields.get("pba__bedrooms_pb__c")),
        "detail_bathrooms": _maybe_int(fields.get("pba__fullbathrooms_pb__c")),
        "detail_total_area_sqft": _maybe_float(fields.get("pba__totalarea_pb__c")),
        "detail_property_type": fields.get("property_type_website__c"),
        "detail_listing_status": fields.get("pba__status__c"),
        "detail_listing_type": fields.get("pba__listingtype__c"),
        "detail_listing_area": _clean_location(fields.get("listing_area")),
        "detail_description": fields.get("pba__description_pb__c"),
        "detail_brief_description": fields.get("pba_brief_description__c"),
        "detail_images": fields.get("images"),
        "detail_video_url": fields.get("property_video"),
        "detail_image_count": _maybe_int(fields.get("image_count")),
        "detail_private_amenities": fields.get("pba_uaefields__private_amenities__c"),
        "detail_commercial_amenities": fields.get("pba_uaefields__commercial_amenities__c"),
        "detail_latitude": _maybe_float(fields.get("pba__latitude_pb__c")),
        "detail_longitude": _maybe_float(fields.get("pba__longitude_pb__c")),
        "detail_country": fields.get("pba__country_pb__c"),
        "detail_city": fields.get("pba__city_pb__c"),
        "detail_address": fields.get("pba__address_pb__c"),
        "detail_agent_name": fields.get("listing_agent_name"),
        "detail_agent_mobile": fields.get("listing_agent_mobile"),
        "detail_agent_email": fields.get("listing_agent_Email"),
        "detail_agent_whatsapp": fields.get("listing_agent_Whatsapp"),
    }

    transferred_date = fields.get("transferred_date__c")
    if transferred_date:
        epoch, iso = _to_epoch_and_iso(transferred_date)
        detail["detail_transferred_date_epoch"] = epoch
        detail["detail_transferred_date_iso"] = iso

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
    except Exception as exc:  # pragma: no cover - connection best effort
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
                "reference_number": {"type": "keyword"},
                "price": {"type": "long"},
                "bedrooms": {"type": "integer"},
                "bathrooms": {"type": "integer"},
                "detail_url": {"type": "keyword"},
                "detail_name": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
                "listing_area": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
                "detail_listing_area": {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}},
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


def _fetch(session: requests.Session, url: str, *, retries: int = 1, timeout: int = 20) -> str:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            response = session.get(url, headers=REQUEST_HEADERS, timeout=timeout)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # pragma: no cover - networking best effort
            last_exc = exc
            sleep_for = SETTINGS.min_delay_seconds * (attempt + 1)
            logger.warning("Request failed (%s). Retrying in %.1fs", exc, sleep_for)
            time.sleep(sleep_for)
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


async def _fetch_detail_with_retry(session: requests.Session, url: str | None) -> dict[str, Any]:
    if not url:
        return {}
    for attempt in range(SETTINGS.detail_retry_count):
        try:
            html_text = _fetch(session, url, retries=1)
            detail = parse_allsopp_detail_page(html_text)
            if detail:
                return detail
        except Exception as exc:  # pragma: no cover - logged for observability
            logger.warning("Detail fetch failed for %s (attempt %s/%s): %s", url, attempt + 1, SETTINGS.detail_retry_count, exc)
        await asyncio.sleep(random.uniform(SETTINGS.min_delay_seconds, SETTINGS.max_delay_seconds))
    return {}


@task
async def allsopp_property_data() -> None:
    session = requests.Session()
    es = es_client()
    ensure_index(es, SETTINGS.es_index)

    all_rows: list[dict[str, Any]] = []
    done = False

    for page in range(1, SETTINGS.pages + 1):
        curr_rows: list[dict[str, Any]] = []
        url = SETTINGS.listing_url_template.format(page=page)
        logger.info("Fetching Allsopp listing page %s", url)
        listing_html = _fetch(session, url, retries=2)

        df = parse_allsopp_listing_page(listing_html)
        if df.empty:
            logger.info("No listings parsed from page %s, stopping.", page)
            break
        logger.info("Parsed %d Allsopp listings from page %d", len(df), page)

        for record in df.to_dict(orient="records"):
            record["source_page"] = page
            rec_id = str(record.get("id")) if record.get("id") is not None else None
            if rec_id and es_doc_exists(es, SETTINGS.es_index, rec_id):
                logger.info("Encountered existing listing id=%s, stopping pagination.", rec_id)
                done = True
                break

            detail_payload = await _fetch_detail_with_retry(session, record.get("detail_url"))
            for key, value in detail_payload.items():
                if value is not None:
                    record[key] = value
            record["source"] = "allsopp"
            all_rows.append(record)
            curr_rows.append(record)
            await asyncio.sleep(random.uniform(SETTINGS.min_delay_seconds, SETTINGS.max_delay_seconds))

        out_dir = Path("saved_data/allsopp")
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"listings_page_{page}.csv"
        df.to_csv(csv_path, index=False)
        logger.info("Saved raw listings to %s", csv_path)

        if done:
            break

        curr_df = pd.DataFrame(curr_rows)
        if curr_df.empty:
            raise ValueError("No Allsopp records collected")

        curr_df = curr_df.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)

        logger.info("Indexing %d Allsopp documents into ES index %s", len(curr_df), SETTINGS.es_index)
        bulk_resp = helpers.bulk(
            es,
            df_to_actions(curr_df, SETTINGS.es_index),
            chunk_size=500,
            request_timeout=120,
            raise_on_error=False,
            raise_on_exception=False,
        )
        logger.info("ES bulk response: %s", bulk_resp)

        await asyncio.sleep(random.uniform(SETTINGS.min_delay_seconds, SETTINGS.max_delay_seconds))

    final_df = pd.DataFrame(all_rows)
    final_df = final_df.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)
    out_json = Path("allsopp_listings.json")
    final_df.to_json(out_json, orient="records", force_ascii=False)
    logger.info("Wrote %s with %d rows", out_json, len(final_df))


class InputParams(BaseModel):
    """Pipeline parameters placeholder (no external inputs required)."""


def fetch_all_docs(es: Elasticsearch, index: str, fields: list[str]):
    query = {"query": {"match_all": {}}, "_source": fields}
    for doc in helpers.scan(es, index=index, query=query, preserve_order=False, size=1000):
        yield doc.get("_source", {})


@task
async def export_allsopp_json_to_r2():
    cloudflare_config = config["cloudflare"]

    fields = [
        "id",
        "reference_number",
        "price",
        "bedrooms",
        "bathrooms",
        "total_area_sqft",
        "property_type",
        "listing_status",
        "listing_area",
        "detail_listing_area",
        "detail_description",
        "detail_private_amenities",
        "detail_images",
        "detail_url",
        "detail_name",
        "detail_transferred_date_iso",
        "source",
    ]

    es = es_client()

    if not es.indices.exists(index=SETTINGS.es_index):
        raise SystemExit(f"Index not found: {SETTINGS.es_index}")

    rows = list(fetch_all_docs(es, SETTINGS.es_index, fields))
    if not rows:
        raise SystemExit("No documents returned from ES.")

    df = pd.DataFrame.from_records(rows)
    present_cols = [col for col in fields if col in df.columns]
    df = df[present_cols].copy()

    out_path = Path("allsopp_listings.json")
    df.to_json(out_path, orient="records", force_ascii=False)
    logger.info("Wrote %s with %d rows and %d columns", out_path, len(df), len(df.columns))

    session = boto3.session.Session()
    s3 = session.client(
        service_name="s3",
        endpoint_url=cloudflare_config["R2_ENDPOINT"],
        aws_access_key_id=cloudflare_config["ACCESS_KEY_ID"],
        aws_secret_access_key=cloudflare_config["SECRET_ACCESS_KEY"],
    )

    key = "data/allsopp_listings.json"
    s3.upload_file(
        Filename=str(out_path),
        Bucket=cloudflare_config["PROP_BUCKET"],
        Key=key,
        ExtraArgs={
            "ContentType": "application/json",
            "ACL": "public-read",
            "CacheControl": "public, max-age=60",
        },
    )
    logger.info("Uploaded to r2://%s/%s", cloudflare_config["PROP_BUCKET"], key)


register_pipeline(
    id="allsopp_pipeline",
    description="Scrape Allsopp & Allsopp residential sale listings and index enriched data into Elasticsearch.",
    tasks=[allsopp_property_data, export_allsopp_json_to_r2],
    triggers=[
        Trigger(
            id="allsopp_daily",
            name="Allsopp Daily",
            description="Run Allsopp scraper daily at 05:30 Dubai time",
            params=InputParams(),
            schedule=CronTrigger(hour="4", minute="30", timezone="Asia/Dubai"),
        )
    ],
    params=InputParams,
)
