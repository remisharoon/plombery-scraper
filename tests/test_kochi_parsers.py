import pytest

from kochi_launches_pipeline import (
    parse_duckduckgo_serp,
    parse_prestige_kochi_projects,
    parse_signature_dwellings,
    parse_realestateindia_locality,
    parse_project_detail_page,
    _looks_blocked,
    _load_next_data,
    _is_kochi_project,
    _html_to_text,
)


class TestParseDuckDuckGo:
    def test_parse_serp(self, google_serp_html):
        results = parse_duckduckgo_serp(google_serp_html)
        assert len(results) >= 3
        urls = [r["url"] for r in results]
        assert any("puravankara" in u.lower() for u in urls)
        assert any("magicbricks" in u.lower() for u in urls)
        assert any("prestige" in u.lower() for u in urls)

    def test_no_youtube_links(self, google_serp_html):
        results = parse_duckduckgo_serp(google_serp_html)
        urls = [r["url"] for r in results]
        assert not any("youtube.com" in u for u in urls)

    def test_no_facebook_links(self, google_serp_html):
        results = parse_duckduckgo_serp(google_serp_html)
        urls = [r["url"] for r in results]
        assert not any("facebook.com" in u for u in urls)

    def test_serp_deduplication(self, google_serp_html):
        results = parse_duckduckgo_serp(google_serp_html)
        urls = [r["url"] for r in results]
        assert len(urls) == len(set(urls))

    def test_urls_decoded(self, google_serp_html):
        results = parse_duckduckgo_serp(google_serp_html)
        for r in results:
            assert "%" not in r["url"]
            assert r["url"].startswith("http")

    def test_empty_page(self, empty_html):
        results = parse_duckduckgo_serp(empty_html)
        assert results == []


class TestParsePrestigeKochiProjects:
    def test_parse_listing_page(self, prestige_prelaunch_html):
        projects = parse_prestige_kochi_projects(prestige_prelaunch_html)
        assert len(projects) >= 1
        names = [p["project_name"] for p in projects]
        assert any("Prestige" in n for n in names)

    def test_filters_non_kochi_projects(self, prestige_prelaunch_html):
        projects = parse_prestige_kochi_projects(prestige_prelaunch_html)
        names = [p["project_name"] for p in projects]
        non_kochi = [n for n in names if "bangalore" in n.lower() or "mumbai" in n.lower() or "devanahalli" in n.lower()]
        assert len(non_kochi) == 0

    def test_project_urls_absolute(self, prestige_prelaunch_html):
        projects = parse_prestige_kochi_projects(prestige_prelaunch_html)
        for p in projects:
            assert p["project_url"].startswith("http")

    def test_project_source(self, prestige_prelaunch_html):
        projects = parse_prestige_kochi_projects(prestige_prelaunch_html)
        for p in projects:
            assert p["source"] == "prestige_prelaunch"
            assert p["builder_name"] == "Prestige Group"

    def test_parse_empty_page(self, empty_html):
        projects = parse_prestige_kochi_projects(empty_html)
        assert projects == []


class TestParseSignatureDwellings:
    def test_parse_listing_page(self, signature_dwellings_html):
        projects = parse_signature_dwellings(signature_dwellings_html)
        assert len(projects) >= 2
        names = [p["project_name"] for p in projects]
        assert any("Signature Abode" in n for n in names)
        assert any("Signature Tropical" in n for n in names)

    def test_skips_non_project_links(self, signature_dwellings_html):
        projects = parse_signature_dwellings(signature_dwellings_html)
        names = [p["project_name"] for p in projects]
        assert "About Us" not in names
        assert "Apartments" not in names

    def test_project_urls_absolute(self, signature_dwellings_html):
        projects = parse_signature_dwellings(signature_dwellings_html)
        for p in projects:
            assert p["project_url"].startswith("http")

    def test_project_source(self, signature_dwellings_html):
        projects = parse_signature_dwellings(signature_dwellings_html)
        for p in projects:
            assert p["source"] == "signature_dwellings"
            assert p["builder_name"] == "Signature Group"

    def test_parse_empty_page(self, empty_html):
        projects = parse_signature_dwellings(empty_html)
        assert projects == []


class TestParseRealEstateIndia:
    def test_parse_empty_page(self, empty_html):
        projects = parse_realestateindia_locality(empty_html)
        assert projects == []

    def test_parse_malformed_html(self, malformed_html):
        projects = parse_realestateindia_locality(malformed_html)
        assert isinstance(projects, list)


class TestParseProjectDetailPage:
    def test_parse_detail(self, project_detail_html):
        detail = parse_project_detail_page(project_detail_html, "magicbricks")
        assert "project_name" in detail
        assert detail["project_name"] == "Prestige Dolphins Court"
        assert "project_description" in detail

    def test_parse_empty_page(self, empty_html):
        detail = parse_project_detail_page(empty_html, "unknown")
        assert isinstance(detail, dict)

    def test_parse_malformed_html(self, malformed_html):
        detail = parse_project_detail_page(malformed_html, "unknown")
        assert isinstance(detail, dict)


class TestIsKochiProject:
    def test_kochi_in_text(self):
        assert _is_kochi_project("New Projects in Kochi") is True

    def test_kochi_in_url(self):
        assert _is_kochi_project("Prestige Project", "https://example.com/kochi/prestige") is True

    def test_kakkanad_in_text(self):
        assert _is_kochi_project("Project in Kakkanad") is True

    def test_bangalore_project(self):
        assert _is_kochi_project("Prestige Project in Bangalore") is False

    def test_devanahalli_project(self):
        assert _is_kochi_project("Prestige Gardenia Estates at Devanahalli") is False

    def test_mixed_score(self):
        assert _is_kochi_project("Project in Kochi near Bangalore") is True


class TestHtmlToText:
    def test_removes_scripts(self):
        html = "<html><script>alert('x')</script><body>Hello</body></html>"
        text = _html_to_text(html)
        assert "alert" not in text
        assert "Hello" in text

    def test_removes_styles(self):
        html = "<html><style>body{color:red}</style><p>Text</p></html>"
        text = _html_to_text(html)
        assert "color" not in text
        assert "Text" in text

    def test_collapses_whitespace(self):
        html = "<p>  Hello   World  </p>"
        text = _html_to_text(html)
        assert "  " not in text


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
