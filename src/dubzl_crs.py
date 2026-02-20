import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import json
# from elasticsearch import Elasticsearch
import pandas as pd
from sqlalchemy import create_engine, text
from plombery import register_pipeline, task, Trigger, get_logger
from pydantic import BaseModel, Field
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from pathlib import Path

import re, json, math
from pathlib import Path
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timezone
import numpy as np
from elasticsearch import Elasticsearch, helpers
from config import read_config
import asyncio
import random
import boto3, os
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)

core_config = read_config()['core']

es_config = read_config()['elasticsearch']
es_hosts = es_config['host'].split(",")
es_user = es_config['username']
es_password = es_config['password']
ES_INDEX = es_config['index']

cloudflare_config = read_config()['cloudflare']

def es_client():
    es = Elasticsearch(
        hosts=es_hosts,
        http_auth=(es_user, es_password),
        timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )
    print(es.info())
    return es

es = es_client()



def es_doc_exists(es: Elasticsearch, index: str, doc_id: str) -> bool:
    try:
        return bool(es.exists(index=index, id=doc_id))
    except Exception:
        return False

def parse_dubizzle_listings_full(html_text: str) -> pd.DataFrame:
    soup = BeautifulSoup(html_text, "html.parser")

    def _snake(s: str) -> str:
        s = re.sub(r"[^\w]+", "_", s.strip().lower())
        return re.sub(r"_+", "_", s).strip("_")

    def _get_next_payload(soup: BeautifulSoup):
        for sc in soup.find_all("script"):
            txt = (sc.string or sc.get_text() or "").strip()
            if not txt or "pageProps" not in txt or "reduxWrapperActionsGIPP" not in txt:
                continue
            try:
                data = json.loads(txt)
            except Exception:
                continue
            pp = data.get("props", {}).get("pageProps", {})
            actions = (pp.get("reduxWrapperActionsGIPP") or []) + (pp.get("reduxWrapperActionsGIAP") or [])
            for a in actions:
                if a.get("type") == "listings/fetchListingDataForQuery/fulfilled":
                    return a.get("payload", {})
        return {}

    def _norm_path(url: str) -> str:
        if not url:
            return ""
        if url.startswith("http"):
            p = urlparse(url)
            path = p.path
        else:
            path = url
        return path.rstrip("/").lower()

    def _is_detail_href(href: str) -> bool:
        return "/motors/used-cars/" in href and href.count("/") > 5 and "?" not in href

    def _build_card_map(soup: BeautifulSoup):
        path_to_card = {}
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not _is_detail_href(href):
                continue
            node = a
            for _ in range(6):
                parent = node.parent
                if not parent or parent.name == "body":
                    break
                node = parent
                if len(node.find_all(["span","div"])) > 5:
                    break
            path_to_card[_norm_path(href)] = node
        return path_to_card

    def _infer_city_from_url(url: str) -> str | None:
        try:
            host = urlparse(url).hostname or ""
            sub = host.split(".")[0]
            if sub and sub.lower() not in ("uae","www"):
                return sub.replace("-", " ").title()
        except Exception:
            pass
        return None

    def _epoch_to_iso(ts):
        if ts is None or (isinstance(ts, float) and math.isnan(ts)):
            return None
        try:
            return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
        except Exception:
            return None

    payload = _get_next_payload(soup)
    hits = payload.get("hits", []) if isinstance(payload, dict) else []

    # union of all detail labels to define columns
    detail_labels = set()
    for h in hits:
        det = h.get("details")
        if isinstance(det, dict):
            for label, obj in det.items():
                if isinstance(obj, dict) and isinstance(obj.get("en"), dict):
                    detail_labels.add(label)

    card_map = _build_card_map(soup)
    records = []

    for h in hits:
        abs_url = h.get("absolute_url")
        url = abs_url.get("en") if isinstance(abs_url, dict) else (abs_url or "")

        row = {
            # Identifiers & URLs
            "id": h.get("id"),
            "uuid": h.get("uuid"),
            "object_id": h.get("objectID"),
            "url": url,
            "permalink": h.get("permalink"),
            "site": h.get("site"),

            # Title & pricing
            "title_en": (h.get("name") or {}).get("en") if isinstance(h.get("name"), dict) else h.get("name"),
            "title_ar": (h.get("name") or {}).get("ar") if isinstance(h.get("name"), dict) else None,
            "price": h.get("price"),
            "pre_discount_price": h.get("pre_discount_price"),
            "discount_percentage": h.get("discount_percentage"),
            "currency": "AED",

            # Seller / Contact / Verification
            "seller_type_code": h.get("seller_type"),
            "has_phone_number": h.get("has_phone_number"),
            "has_whatsapp_number": h.get("has_whatsapp_number"),
            "can_chat": h.get("can_chat"),
            "is_verified_user": h.get("is_verified_user"),
            "is_verified_business": h.get("is_verified_business"),
            "is_premium": h.get("is_premium"),
            "is_super_ad": h.get("is_super_ad"),
            "is_reserved": h.get("is_reserved"),
            "is_coming_soon": h.get("is_coming_soon"),
            "has_vin": h.get("has_vin"),
            "has_variants": h.get("has_variants"),
            "auto_agent_id": h.get("auto_agent_id"),
            "agent_logo": h.get("agent_logo"),
            "user_id": (h.get("user") or {}).get("id") if isinstance(h.get("user"), dict) else None,
            "business": h.get("business"),

            # Media
            "photos_count": h.get("photos_count"),
            "first_photo_url": (h.get("photos") or [None])[0] if isinstance(h.get("photos"), list) and h.get("photos") else None,
            "first_thumbnail_url": (h.get("photo_thumbnails") or [None])[0] if isinstance(h.get("photo_thumbnails"), list) and h.get("photo_thumbnails") else None,

            # Location
            "neighbourhood_en": (h.get("neighbourhood") or {}).get("en") if isinstance(h.get("neighbourhood"), dict) else None,
            "neighbourhood_id": (h.get("neighbourhood") or {}).get("id") if isinstance(h.get("neighbourhood"), dict) else None,
            "location_path_en": " > ".join((h.get("location_list") or {}).get("en", []) or []) if isinstance(h.get("location_list"), dict) else None,
            "location_path_ar": " > ".join((h.get("location_list") or {}).get("ar", []) or []) if isinstance(h.get("location_list"), dict) else None,
            "location_path_fr": " > ".join((h.get("location_list") or {}).get("fr", []) or []) if isinstance(h.get("location_list"), dict) else None,

            # Timestamps
            "added_epoch": h.get("added"),
            "created_at_epoch": h.get("created_at"),
        }

        # City inferred
        row["city_inferred"] = _infer_city_from_url(url)
        if not row["city_inferred"] and isinstance(h.get("location_list"), dict):
            for lang in ("en","ar","fr"):
                arr = h["location_list"].get(lang)
                if isinstance(arr, list) and len(arr) >= 2:
                    row["city_inferred"] = arr[1]
                    break

        # Taxonomy
        row["category_id"] = h.get("category_id")
        if isinstance(h.get("category"), dict):
            row["category_en_path"] = " > ".join(h["category"].get("en", []) or [])
            row["category_ar_path"] = " > ".join(h["category"].get("ar", []) or [])
            row["category_slug_path"] = " > ".join(h["category"].get("slug", []) or [])
        else:
            row["category_en_path"] = row["category_ar_path"] = row["category_slug_path"] = None

        if isinstance(h.get("category_v2"), dict):
            row["category_v2_slug_paths"] = " > ".join(h["category_v2"].get("slug_paths", []) or [])
        else:
            row["category_v2_slug_paths"] = None

        # Flatten details.* (English values)
        det = h.get("details") if isinstance(h.get("details"), dict) else {}
        for label in sorted(detail_labels):
            key = f"details_{_snake(label)}"
            val = None
            obj = det.get(label)
            if isinstance(obj, dict):
                en = obj.get("en")
                if isinstance(en, dict):
                    v = en.get("value")
                    if isinstance(v, list):
                        val = "; ".join(map(str, v))
                    else:
                        val = v
            row[key] = val

        # HTML-derived hints (fallbacks)
        card = card_map.get(_norm_path(url), None) if url else None
        if card:
            txt = re.sub(r"\s+", " ", card.get_text(" ", strip=True))
            m_year = re.search(r"\b(20\d{2}|19\d{2})\b", txt)
            m_mileage = re.search(r"([0-9][\d,]*)\s*(km)\b", txt, flags=re.I)
            html_trans = "Automatic" if re.search(r"\bAutomatic\b", txt, re.I) else ("Manual" if re.search(r"\bManual\b", txt, re.I) else None)
            m_nb = re.search(r"\(([^)]+)\),\s*(Dubai|Abu Dhabi|Sharjah|Ajman|Ras Al Khaimah|Umm Al Quwain|Fujairah)", txt)

            row["html_year_guess"] = int(m_year.group(1)) if m_year else None
            row["html_mileage"] = int(m_mileage.group(1).replace(",", "")) if m_mileage else None
            row["html_mileage_unit"] = "km" if m_mileage else None
            row["html_transmission"] = html_trans
            row["html_neighbourhood"] = m_nb.group(1) if m_nb else None
        else:
            row["html_year_guess"] = row["html_mileage"] = row["html_mileage_unit"] = row["html_transmission"] = row["html_neighbourhood"] = None

        if row["id"] and es_doc_exists(es, ES_INDEX, row["id"]):
            logger.info("Skip existing listing id=%s in index=%s", row["id"], ES_INDEX)
            continue
        records.append(row)

    df = pd.DataFrame.from_records(records)

    # add ISO times
    def _epoch_to_iso(ts):
        if ts is None or (isinstance(ts, float) and pd.isna(ts)):
            return None
        try:
            return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
        except Exception:
            return None
    for col in ["added_epoch", "created_at_epoch"]:
        df[col + "_iso"] = df[col].apply(_epoch_to_iso)

    return df

def df_to_actions(df: pd.DataFrame, index: str):
    # Replace NaN/NaT with None so ES gets nulls, not NaN strings
    clean = df.replace({np.nan: None})
    # Convert numpy scalar types to native Python types
    records = clean.to_dict(orient="records")

    for r in records:
        # Use 'id' as document _id when present; ES will upsert on re-index
        doc_id = r.get("id")
        yield {
            "_index": index,
            "_id": doc_id,
            "_op_type": "index",   # use "create" if you want to fail on duplicate IDs
            "_source": r
        }


def fetch_all_docs(es, index: str, fields: list[str]):
    """
    Use helpers.scan to stream all docs without the 10k limit.
    """
    query = {"query": {"match_all": {}}, "_source": fields}
    for doc in helpers.scan(es, index=index, query=query, preserve_order=False, size=1000):
        src = doc.get("_source", {})
        yield src


mapping = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "5s"
    },
    "mappings": {
        "dynamic": True,
        "dynamic_templates": [
            {"dates_iso":  {"match": "*_iso",  "mapping": {"type": "date", "format": "strict_date_time"}}},
            {"epochs":     {"match": "*_epoch","mapping": {"type": "long"}}},
            {"strings":    {"match_mapping_type": "string", "mapping": {"type": "keyword", "ignore_above": 256}}},
            {"doubleNums": {"match_mapping_type": "double", "mapping": {"type": "double"}}},
            {"longNums":   {"match_mapping_type": "long",   "mapping": {"type": "long"}}}
        ],
        "properties": {
            "id":            {"type": "long"},
            "price":         {"type": "integer"},
            "photos_count":  {"type": "integer"},
            "url":           {"type": "keyword"},
            "permalink":     {"type": "keyword"},
            "title_en":      {"type": "text", "fields": {"kw": {"type":"keyword","ignore_above":256}}},
        }
    }
}

if not es.indices.exists(index=ES_INDEX):
    es.indices.create(index=ES_INDEX, body=mapping)


class ScrapingUtils:
    header = {
        "Authority": "www.propertyfinder.ae",
        "Method": "GET",
        "Path": "/en/search?c=1&ob=mr&page=1",
        "Scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-GB,en;q=0.9,en-US;q=0.8,ml;q=0.7",
        "Cache-Control": "no-cache",
        "Cookie": "_hjSessionUser_17954=eyJpZCI6IjdjYTliM2Y4LTM0NzAtNTZkYy05MzY5LTdlZGM4NDlhMG",
        "Dnt": "1",
        "Pragma": "no-cache",
        "Sec-Ch-Ua": "\"Not.A/Brand\";v=\"8\", \"Chromium\";v=\"114\", \"Google Chrome\";v=\"114\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"macOS\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }

class InputParams(BaseModel):
    """Showcase all the available input types in Plombery"""

base_url = core_config['base_url']


def merge_two_dicts(x, y):
    z = x.copy()   # start with keys and values of x
    z.update(y)    # modifies z with keys and values of y
    return z

def build_detail_url(listing_id):
    base_url = "https://www.allsoppandallsopp.com/dubai/property/sales/"
    return base_url + listing_id

def merge_details(listing_flat):
    # listing_id = listing_flat['pba__broker_s_listing_id__c']
    # detail_url = build_detail_url(listing_id)
    detail_url = listing_flat['share_url']
    # propFndrDetailParser = PropFndrDetailParser(detail_url,listing_flat["prop_id"])
    # details = propFndrDetailParser.parse()
    # listing_flat = merge_two_dicts(listing_flat, details)
    return listing_flat

def save_listings(listings):
    for listing in listings:
        pass
        # mapped_docs =  PFMapper.map_doc(listing)
        # property_info = mapped_docs.get("property_info")
        # agent_info = mapped_docs.get("agent_info")
        # broker_info = mapped_docs.get("broker_info")
        #
        # property_info['offering_type_id'] = offering_type_id
        # property_info['source'] = "PROPFNDR"
        # property_info['details_added'] = False
        # prop_doc_id = property_info["source"] + "-" + str(property_info["offering_type_id"]) + "-" + property_info["property_id"]

        # if not is_listing_saved(prop_doc_id):
        #
        #     print("saving -- ", property_info, "prop_doc_id: ", prop_doc_id)
        #     pa_es.index(index=target_index, body=property_info, id=prop_doc_id)
        #     try:
        #         if not pa_es.exists(index='broker_info', id=broker_info["broker_id"]):
        #             pa_es.index(index='broker_info', body=broker_info, id=broker_info["broker_id"])
        #     except Exception as e:
        #         print(e)
        #     try:
        #         if not pa_es.exists(index='agent_info', id=agent_info["agent_id"]):
        #             pa_es.index(index='agent_info', body=agent_info, id=agent_info["agent_id"])
        #     except Exception as e:
        #         print(e)
        # else:
        #     print(prop_doc_id, " is already saved")

def build_url(page_n):
    # Get the datetime 2 days before now
    two_days_before = datetime.now() - timedelta(days=1)

    # Convert to epoch timestamp (int)
    two_days_epoch_timestamp = int(two_days_before.timestamp())
    return base_url.format(two_days_epoch_timestamp, page_n)


def save_html(resp, filepath: str):
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    Path(filepath).write_bytes(resp.content)  # exact bytes

def load_soup(filepath: str) -> BeautifulSoup:
    data = Path(filepath).read_bytes()
    return BeautifulSoup(data, "html.parser")

@task
async def dbzl_car_data():
    for page_n in range(1,20):
        url = build_url(page_n)
        # url = base_url
        try:
            print('****** LOOOADINGGGGG PAGE No = ' + str(page_n) + "  **********")
            hdr = ScrapingUtils.header

            page = response = requests.get(url, headers = hdr, timeout=10)

            # save
            save_html(response, "saved_pages/page_{0}.html".format(page_n))
            # later: read
            soup = load_soup("saved_pages/page_{0}.html".format(page_n))
            # soup = BeautifulSoup(page.text, 'html.parser')

            html_file = "saved_pages/page_{0}.html".format(page_n)
            html_text = Path(html_file).read_text(encoding="utf-8", errors="ignore")
            df = parse_dubizzle_listings_full(html_text)

            # QA: row count and key columns present
            assert len(df) > 0, "No rows parsed"
            expected_cols = ["id", "title_en", "price", "url", "has_phone_number", "has_whatsapp_number",
                             "details_body_type", "details_year"]
            for c in expected_cols:
                assert c in df.columns, f"Missing expected column: {c}"

            out_csv = "saved_data/dubizzle_listings_page_{0}_full.csv".format(page_n)
            # Define the full file path
            filepath = Path(out_csv)
            # Create parent directories if they don't exist
            filepath.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(out_csv, index=False)
            print(f"OK: {len(df)} listings parsed → {out_csv}")

            # Do the bulk ingest in chunks; capture failures
            resp = helpers.bulk(
                es,
                df_to_actions(df, ES_INDEX),
                chunk_size=1000,
                request_timeout=120,
                raise_on_error=False,  # don't explode on a single bad doc
                raise_on_exception=False
            )
            print("Bulk result:", resp)

            # 👇 human-style pause: random 2–5 seconds
            await asyncio.sleep(random.uniform(55, 155))
        except Exception as e:
            print('************ ERROR *****************', e)
            break


@task
async def export_json_to_r2():

    # Fields to expose in the dashboard JSON
    FIELDS = [
        "id", "title_en", "price", "city_inferred", "details_body_type", "details_year",
        "details_kilometers", "details_transmission_type",
        "permalink", "url", "added_epoch_iso", "created_at_epoch_iso", "details_make", "details_model", "neighbourhood_en",
        "details_regional_specs","details_seller_type"
    ]

    es = es_client()

    # Optional: check index exists
    if not es.indices.exists(index=ES_INDEX):
        raise SystemExit(f"Index not found: {ES_INDEX}")

    rows = list(fetch_all_docs(es, ES_INDEX, FIELDS))
    if not rows:
        raise SystemExit("No documents returned from ES.")

    # Build DataFrame; keep only expected columns (missing become NaN -> will be written as null)
    df = pd.DataFrame.from_records(rows)
    present_cols = [c for c in FIELDS if c in df.columns]
    df = df[present_cols].copy()

    # Write compact JSON (array of objects)
    out_path = Path("dbzl_listings.json")
    df.to_json(out_path, orient="records", force_ascii=False)

    print(f"Wrote {out_path.resolve()} with {len(df)} rows and {len(df.columns)} columns")
    # Quick peek
    if len(df):
        print("Columns:", ", ".join(df.columns.tolist()))

    # upload_to_r2.py

    ACCOUNT_ID = cloudflare_config["ACCOUNT_ID"]
    # R2_ENDPOINT = f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com"
    R2_ENDPOINT = cloudflare_config["R2_ENDPOINT"]
    BUCKET = cloudflare_config["BUCKET"]
    ACCESS_KEY_ID = cloudflare_config["ACCESS_KEY_ID"]
    SECRET_ACCESS_KEY = cloudflare_config["SECRET_ACCESS_KEY"]

    session = boto3.session.Session()
    s3 = session.client(
        service_name="s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
    )

    local = Path("dbzl_listings.json")
    key = "data/dbzl_listings.json"  # path inside the bucket

    s3.upload_file(
        Filename=str(local),
        Bucket=BUCKET,
        Key=key,
        ExtraArgs={
            "ContentType": "application/json",
            "ACL": "public-read",  # for buckets that still honor ACLs; for new-style public buckets this is ignored
            "CacheControl": "public, max-age=60"  # 1 minute cache; tune later
        },
    )

    print(f"Uploaded to r2://{BUCKET}/{key}")
    print("Public URL (example): https://pub-xxxxxxxxxxxxxxxxxxxx.r2.dev/data/listings.json")


register_pipeline(
    id="crs_pipeline",
    description="""This is a very dbzlly pipeline""",
    # tasks=[get_jobs_data, ai_infer_raw_data, load_jobs_analyzer_site],
    tasks= [dbzl_car_data, export_json_to_r2],
    # tasks= [export_json_to_r2],
    triggers=[
        Trigger(
            id="daily1",
            name="Daily1",
            description="Run the pipeline 1 times daily",
            params=InputParams(),
            schedule=CronTrigger(
                hour="5", minute="45", timezone="Asia/Dubai"
            ),
        )
    ],
    params=InputParams,
)
