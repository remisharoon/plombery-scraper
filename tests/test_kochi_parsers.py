import pytest

from kochi_launches_pipeline import (
    parse_magicbricks_listing,
    parse_housing_listing,
    parse_99acres_listing,
    parse_commonfloor_listing,
    parse_google_serp,
    parse_project_detail_page,
    _looks_blocked,
    _load_next_data,
)


class TestParseMagicbricks:
    def test_parse_listing_page(self, magicbricks_html):
        projects = parse_magicbricks_listing(magicbricks_html)
        assert len(projects) >= 1
        project = projects[0]
        assert project["project_name"] == "Prestige Dolphins Court"
        assert project["builder_name"] == "Prestige Group"
        assert project["locality"] == "Kakkanad"
        assert project["price_min"] == 7500000
        assert project["price_max"] == 12000000
        assert project["price_currency"] == "INR"
        assert project["source"] == "magicbricks"
        assert project["city"] == "Kochi"
        assert project["state"] == "Kerala"

    def test_parse_listing_multiple_projects(self, magicbricks_html):
        projects = parse_magicbricks_listing(magicbricks_html)
        assert len(projects) == 2
        names = [p["project_name"] for p in projects]
        assert "Prestige Dolphins Court" in names
        assert "Sobha Crystal Meadows" in names

    def test_parse_sobha_project(self, magicbricks_html):
        projects = parse_magicbricks_listing(magicbricks_html)
        sobha = [p for p in projects if p["builder_name"] == "Sobha Limited"][0]
        assert sobha["price_min"] == 15000000
        assert sobha["price_max"] == 30000000
        assert sobha["launch_status"] == "pre-launch"

    def test_parse_empty_page(self, empty_html):
        projects = parse_magicbricks_listing(empty_html)
        assert projects == []

    def test_parse_malformed_html(self, malformed_html):
        projects = parse_magicbricks_listing(malformed_html)
        assert isinstance(projects, list)

    def test_project_has_configurations(self, magicbricks_html):
        projects = parse_magicbricks_listing(magicbricks_html)
        project = projects[0]
        assert isinstance(project.get("configurations"), list)
        assert "2BHK" in project["configurations"] or "3BHK" in project["configurations"]

    def test_project_has_amenities(self, magicbricks_html):
        projects = parse_magicbricks_listing(magicbricks_html)
        project = projects[0]
        assert isinstance(project.get("amenities"), list)
        assert len(project["amenities"]) > 0

    def test_project_has_rera(self, magicbricks_html):
        projects = parse_magicbricks_listing(magicbricks_html)
        project = projects[0]
        assert project.get("rera_number") == "K-RERA/123/2025"

    def test_project_has_area(self, magicbricks_html):
        projects = parse_magicbricks_listing(magicbricks_html)
        project = projects[0]
        assert project.get("super_area_min_sqft") == 1250.0


class TestParseHousing:
    def test_parse_listing_page(self, housing_html):
        projects = parse_housing_listing(housing_html)
        assert len(projects) >= 1
        project = projects[0]
        assert project["project_name"] == "Godrej Kochi Riverside"
        assert project["builder_name"] == "Godrej Properties"
        assert project["locality"] == "Tripunithura"
        assert project["source"] == "housing"

    def test_parse_empty_page(self, empty_html):
        projects = parse_housing_listing(empty_html)
        assert projects == []


class TestParse99acres:
    def test_parse_empty_page(self, empty_html):
        projects = parse_99acres_listing(empty_html)
        assert projects == []

    def test_parse_malformed_html(self, malformed_html):
        projects = parse_99acres_listing(malformed_html)
        assert isinstance(projects, list)


class TestParseCommonFloor:
    def test_parse_listing_page(self, commonfloor_html):
        projects = parse_commonfloor_listing(commonfloor_html)
        assert len(projects) >= 1
        project = projects[0]
        assert project["project_name"] == "Asset Grandeur"
        assert project["builder_name"] == "Asset Homes"
        assert project["source"] == "commonfloor"

    def test_parse_empty_page(self, empty_html):
        projects = parse_commonfloor_listing(empty_html)
        assert projects == []


class TestParseGoogleSerp:
    def test_parse_serp(self, google_serp_html):
        results = parse_google_serp(google_serp_html)
        assert len(results) >= 1
        urls = [r["url"] for r in results]
        assert any("prestige" in u.lower() for u in urls)

    def test_no_youtube_links(self, google_serp_html):
        results = parse_google_serp(google_serp_html)
        urls = [r["url"] for r in results]
        assert not any("youtube.com" in u for u in urls)

    def test_no_google_links(self, google_serp_html):
        results = parse_google_serp(google_serp_html)
        urls = [r["url"] for r in results]
        assert not any("google.com" in u for u in urls)

    def test_serp_deduplication(self, google_serp_html):
        results = parse_google_serp(google_serp_html)
        urls = [r["url"] for r in results]
        assert len(urls) == len(set(urls))

    def test_empty_page(self, empty_html):
        results = parse_google_serp(empty_html)
        assert results == []


class TestParseProjectDetailPage:
    def test_parse_detail(self, project_detail_html):
        detail = parse_project_detail_page(project_detail_html, "magicbricks")
        assert "project_name" in detail
        assert detail["project_name"] == "Prestige Dolphins Court"
        assert "project_description" in detail
        assert "amenities" in detail

    def test_parse_empty_page(self, empty_html):
        detail = parse_project_detail_page(empty_html, "unknown")
        assert isinstance(detail, dict)

    def test_parse_malformed_html(self, malformed_html):
        detail = parse_project_detail_page(malformed_html, "unknown")
        assert isinstance(detail, dict)


class TestLoadNextData:
    def test_load_valid(self, magicbricks_html):
        result = _load_next_data(magicbricks_html)
        assert result is not None
        assert "props" in result

    def test_load_missing(self, empty_html):
        result = _load_next_data(empty_html)
        assert result is None


class TestLooksBlocked:
    def test_blocked_captcha(self, blocked_page_html):
        assert _looks_blocked(blocked_page_html) is True

    def test_not_blocked(self, magicbricks_html):
        assert _looks_blocked(magicbricks_html) is False

    def test_empty_page(self, empty_html):
        assert _looks_blocked(empty_html) is False
