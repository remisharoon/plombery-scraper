import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def magicbricks_html():
    return (FIXTURES_DIR / "magicbricks_kochi_listing.html").read_text()


@pytest.fixture
def housing_html():
    return (FIXTURES_DIR / "housing_kochi_listing.html").read_text()


@pytest.fixture
def commonfloor_html():
    return (FIXTURES_DIR / "commonfloor_kochi_listing.html").read_text()


@pytest.fixture
def google_serp_html():
    return (FIXTURES_DIR / "google_serp_kochi_launches.html").read_text()


@pytest.fixture
def prestige_prelaunch_html():
    return (FIXTURES_DIR / "prestige_prelaunch_listing.html").read_text()


@pytest.fixture
def signature_dwellings_html():
    return (FIXTURES_DIR / "signature_dwellings_listing.html").read_text()


@pytest.fixture
def project_detail_html():
    return (FIXTURES_DIR / "project_detail_page.html").read_text()


@pytest.fixture
def blocked_page_html():
    return (FIXTURES_DIR / "blocked_page.html").read_text()


@pytest.fixture
def empty_html():
    return "<html><body></body></html>"


@pytest.fixture
def malformed_html():
    return "<html><body><div>broken<div></body>"


@pytest.fixture
def sample_project_record():
    return {
        "project_name": "Prestige Dolphins Court",
        "builder_name": "Prestige Group",
        "builder_tier": "premium",
        "property_types": ["apartment"],
        "launch_status": "under-construction",
        "price_min": 7500000,
        "price_max": 12000000,
        "price_currency": "INR",
        "price_per_sqft": 6000,
        "configurations": ["2BHK", "3BHK"],
        "total_units": 250,
        "total_towers": 4,
        "total_floors": 18,
        "project_size_acres": 3.5,
        "carpet_area_min_sqft": 950.0,
        "carpet_area_max_sqft": 1400.0,
        "super_area_min_sqft": 1250.0,
        "super_area_max_sqft": 1850.0,
        "locality": "Kakkanad",
        "city": "Kochi",
        "district": "Ernakulam",
        "state": "Kerala",
        "latitude": 9.9816,
        "longitude": 76.2999,
        "possession_date": "2026-12",
        "possession_quarter": "Q4 2026",
        "rera_number": "K-RERA/123/2025",
        "rera_status": "Registered",
        "amenities": ["Swimming Pool", "Gym", "Club House", "Jogging Track", "Children Play Area"],
        "floor_plans": [
            {"config": "2BHK", "area_sqft": 1250.0, "price_min": 7500000, "price_max": 8500000},
            {"config": "3BHK", "area_sqft": 1850.0, "price_min": 9500000, "price_max": 12000000},
        ],
        "project_description": "Premium apartments in Kakkanad",
        "project_highlights": ["Riverside Living", "Near Metro", "IT Hub Proximity"],
        "images": ["https://img.magicbricks.com/proj1-1.jpg", "https://img.magicbricks.com/proj1-2.jpg"],
        "brochure_url": "https://magicbricks.com/brochure/prestige-dolphins.pdf",
        "project_url": "https://www.magicbricks.com/new-projects-kochi/prestige-dolphins-court-pppfsid",
        "builder_url": "https://www.prestigeconstructions.com",
        "source_url": "https://www.magicbricks.com/new-projects-kochi/prestige-dolphins-court-pppfsid",
        "source": "magicbricks",
    }


@pytest.fixture
def sample_project_record_2():
    return {
        "project_name": "Prestige Dolphins Court",
        "builder_name": "Prestige Group",
        "builder_tier": "premium",
        "property_types": ["apartment", "villa"],
        "launch_status": "under-construction",
        "price_min": 7500000,
        "price_max": 15000000,
        "price_currency": "INR",
        "configurations": ["2BHK", "3BHK", "4BHK"],
        "locality": "Kakkanad",
        "city": "Kochi",
        "state": "Kerala",
        "rera_number": "K-RERA/123/2025",
        "amenities": ["Swimming Pool", "Gym", "Club House", "Tennis Court"],
        "project_url": "https://www.housing.com/new-projects/kochi/prestige-dolphins-court",
        "source": "housing",
    }


@pytest.fixture
def mock_es_client():
    es = MagicMock()
    es.indices.exists.return_value = False
    es.indices.create.return_value = {"acknowledged": True}
    es.exists.return_value = False
    es.info.return_value = {"cluster_name": "test-cluster"}
    es.update.return_value = {"result": "updated"}
    return es


@pytest.fixture
def mock_gemini_response():
    def _make_response(data: dict, status_code: int = 200):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": json.dumps(data)}]
                    }
                }
            ]
        }
        return resp
    return _make_response


@pytest.fixture
def mock_config():
    config_dict = {
        "kochi_launches": {
            "magicbricks_url": "https://www.magicbricks.com/new-projects-kochi-pppfs",
            "housing_url": "https://www.housing.com/new-projects/kochi",
            "acres99_url": "https://www.99acres.com/new-projects-in-kochi",
            "commonfloor_url": "https://www.commonfloor.com/kochi-property/new-projects",
            "google_search_queries": "pre launch apartments kochi,new launch villas kochi",
            "pages": "5",
            "google_pages": "2",
            "min_delay_seconds": "0.1",
            "max_delay_seconds": "0.2",
            "detail_retry_count": "1",
            "request_timeout_seconds": "10",
            "es_index": "kochi_property_launches",
        },
        "elasticsearch": {
            "host": "localhost:9200",
            "username": "elastic",
            "password": "changeme",
        },
        "GeminiPro": {
            "API_KEY_RH": "fake-key-1",
            "API_KEY_RHA": "fake-key-2",
        },
    }

    class FakeConfig:
        def __getitem__(self, key):
            return config_dict.get(key, {})

    return FakeConfig()
