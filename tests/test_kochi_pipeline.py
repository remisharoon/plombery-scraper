import asyncio
import json
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from kochi_launches_pipeline import (
    discover_kochi_projects,
    enrich_project_details,
    standardize_and_index,
    ai_classify_builders,
    normalize_project_record,
    parse_duckduckgo_serp,
    parse_prestige_kochi_projects,
    parse_signature_dwellings,
    parse_project_detail_page,
    InputParams,
)


def run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestDiscoverTask:
    @patch("kochi_launches_pipeline._fetch")
    @patch("kochi_launches_pipeline._build_http_client")
    def test_discovers_from_duckduckgo(self, mock_client_builder, mock_fetch, google_serp_html):
        mock_session = MagicMock()
        mock_client_builder.return_value = (mock_session, False)

        def side_effect(session, url, **kwargs):
            if "duckduckgo" in url:
                return google_serp_html
            # Level 2: listing page with project links
            if "squareyards" in url or "magicbricks" in url or "housing" in url:
                return '<html><body><a href="https://example.com/project-kochi-1">Kochi Project</a></body></html>'
            return "<html><body></body></html>"

        mock_fetch.side_effect = side_effect

        with patch("kochi_launches_pipeline.SETTINGS") as mock_settings:
            mock_settings.duckduckgo_queries = ["pre launch apartments kochi"]
            mock_settings.duckduckgo_pages = 1
            mock_settings.signature_dwellings_url = ""
            mock_settings.prestige_prelaunch_kochi_url = ""
            mock_settings.realestateindia_url = ""
            mock_settings.realestateindia_localities = []
            mock_settings.pages = 1
            mock_settings.min_delay = 0.01
            mock_settings.max_delay = 0.02

            result = run_async(discover_kochi_projects.run(params=InputParams()))
            assert isinstance(result, list)
            assert len(result) > 0

    @patch("kochi_launches_pipeline._fetch")
    @patch("kochi_launches_pipeline._build_http_client")
    def test_discovers_from_prestige_prelaunch(self, mock_client_builder, mock_fetch, prestige_prelaunch_html):
        mock_session = MagicMock()
        mock_client_builder.return_value = (mock_session, False)
        mock_fetch.return_value = prestige_prelaunch_html

        with patch("kochi_launches_pipeline.SETTINGS") as mock_settings:
            mock_settings.duckduckgo_queries = []
            mock_settings.duckduckgo_pages = 0
            mock_settings.signature_dwellings_url = ""
            mock_settings.prestige_prelaunch_kochi_url = "https://prestigeprelaunchprojects.com/kochi/"
            mock_settings.realestateindia_url = ""
            mock_settings.realestateindia_localities = []
            mock_settings.pages = 1
            mock_settings.min_delay = 0.01
            mock_settings.max_delay = 0.02

            result = run_async(discover_kochi_projects.run(params=InputParams()))
            assert isinstance(result, list)
            assert len(result) >= 1
            names = [p.get("project_name", "") for p in result]
            assert any("Prestige" in n for n in names)

    @patch("kochi_launches_pipeline._fetch")
    @patch("kochi_launches_pipeline._build_http_client")
    def test_discovers_from_signature_dwellings(self, mock_client_builder, mock_fetch, signature_dwellings_html):
        mock_session = MagicMock()
        mock_client_builder.return_value = (mock_session, False)
        mock_fetch.return_value = signature_dwellings_html

        with patch("kochi_launches_pipeline.SETTINGS") as mock_settings:
            mock_settings.duckduckgo_queries = []
            mock_settings.duckduckgo_pages = 0
            mock_settings.signature_dwellings_url = "https://signaturedwellingsprojects.com/kochi/"
            mock_settings.prestige_prelaunch_kochi_url = ""
            mock_settings.realestateindia_url = ""
            mock_settings.realestateindia_localities = []
            mock_settings.pages = 1
            mock_settings.min_delay = 0.01
            mock_settings.max_delay = 0.02

            result = run_async(discover_kochi_projects.run(params=InputParams()))
            assert isinstance(result, list)
            assert len(result) >= 2
            names = [p.get("project_name", "") for p in result]
            assert any("Signature" in n for n in names)

    @patch("kochi_launches_pipeline._fetch")
    @patch("kochi_launches_pipeline._build_http_client")
    def test_handles_empty_results(self, mock_client_builder, mock_fetch, empty_html):
        mock_session = MagicMock()
        mock_client_builder.return_value = (mock_session, False)
        mock_fetch.return_value = empty_html

        with patch("kochi_launches_pipeline.SETTINGS") as mock_settings:
            mock_settings.duckduckgo_queries = []
            mock_settings.duckduckgo_pages = 0
            mock_settings.signature_dwellings_url = ""
            mock_settings.prestige_prelaunch_kochi_url = ""
            mock_settings.realestateindia_url = ""
            mock_settings.realestateindia_localities = []
            mock_settings.pages = 0
            mock_settings.min_delay = 0.01
            mock_settings.max_delay = 0.02

            result = run_async(discover_kochi_projects.run(params=InputParams()))
            assert isinstance(result, list)


class TestEnrichTask:
    @patch("kochi_launches_pipeline._fetch")
    @patch("kochi_launches_pipeline._build_http_client")
    def test_enriches_project_details(self, mock_client_builder, mock_fetch, project_detail_html):
        mock_session = MagicMock()
        mock_client_builder.return_value = (mock_session, False)
        mock_fetch.return_value = project_detail_html

        discovered = [
            {
                "project_name": "Prestige Dolphins Court",
                "builder_name": "Prestige Group",
                "project_url": "https://www.magicbricks.com/project1",
                "source": "magicbricks",
            }
        ]

        with patch("kochi_launches_pipeline.SETTINGS") as mock_settings:
            mock_settings.min_delay = 0.01
            mock_settings.max_delay = 0.02
            mock_settings.detail_retry = 1

            result = run_async(enrich_project_details.run(discovered=discovered))
            assert isinstance(result, list)
            assert len(result) >= 1
            record = result[0]
            assert record.get("project_name") == "Prestige Dolphins Court"
            assert "id" in record

    @patch("kochi_launches_pipeline._build_http_client")
    def test_enrich_empty_list(self, mock_client_builder):
        mock_session = MagicMock()
        mock_client_builder.return_value = (mock_session, False)

        with patch("kochi_launches_pipeline.SETTINGS") as mock_settings:
            mock_settings.min_delay = 0.01
            mock_settings.max_delay = 0.02

            result = run_async(enrich_project_details.run(discovered=[]))
            assert result == []


class TestStandardizeAndIndexTask:
    def test_standardize_and_index(self, mock_es_client, sample_project_record):
        record = normalize_project_record(sample_project_record)

        with patch("kochi_launches_pipeline.es_client", return_value=mock_es_client):
            with patch("kochi_launches_pipeline.helpers") as mock_helpers:
                mock_helpers.bulk.return_value = (1, [])
                result = run_async(standardize_and_index.run(enriched=[record]))
                assert result >= 0

    def test_empty_input(self, mock_es_client):
        with patch("kochi_launches_pipeline.es_client", return_value=mock_es_client):
            result = run_async(standardize_and_index.run(enriched=[]))
            assert result == 0


class TestClassifyBuildersTask:
    def test_classify_builders(self, mock_es_client):
        mock_es_client.indices.exists.return_value = True
        docs = [
            {
                "_id": "doc1",
                "_source": {
                    "builder_name": "Prestige Group",
                    "price_min": 7500000,
                    "property_types": ["apartment"],
                },
            }
        ]

        with patch("kochi_launches_pipeline.es_client", return_value=mock_es_client):
            with patch("kochi_launches_pipeline.helpers") as mock_helpers:
                mock_helpers.scan.return_value = docs
                result = run_async(ai_classify_builders.run())
                assert result >= 0
                mock_es_client.update.assert_called()

    def test_no_docs_needing_classification(self, mock_es_client):
        mock_es_client.indices.exists.return_value = True

        with patch("kochi_launches_pipeline.es_client", return_value=mock_es_client):
            with patch("kochi_launches_pipeline.helpers") as mock_helpers:
                mock_helpers.scan.return_value = []
                result = run_async(ai_classify_builders.run())
                assert result == 0


class TestFullPipelineIntegration:
    @patch("kochi_launches_pipeline.ai_extract_project")
    @patch("kochi_launches_pipeline._fetch")
    @patch("kochi_launches_pipeline._build_http_client")
    def test_discover_to_index_chain(self, mock_client_builder, mock_fetch, mock_ai_extract, signature_dwellings_html, project_detail_html, mock_es_client):
        mock_session = MagicMock()
        mock_client_builder.return_value = (mock_session, False)
        mock_ai_extract.return_value = {}

        def side_effect(session, url, **kwargs):
            if "signature" in url.lower():
                return signature_dwellings_html
            return project_detail_html

        mock_fetch.side_effect = side_effect

        with patch("kochi_launches_pipeline.SETTINGS") as mock_settings:
            mock_settings.duckduckgo_queries = []
            mock_settings.duckduckgo_pages = 0
            mock_settings.signature_dwellings_url = "https://signaturedwellingsprojects.com/kochi/"
            mock_settings.prestige_prelaunch_kochi_url = ""
            mock_settings.realestateindia_url = ""
            mock_settings.realestateindia_localities = []
            mock_settings.pages = 1
            mock_settings.min_delay = 0.01
            mock_settings.max_delay = 0.02
            mock_settings.detail_retry = 1
            mock_settings.es_index = "kochi_property_launches"

            discovered = run_async(discover_kochi_projects.run(params=InputParams()))
            assert len(discovered) > 0

            enriched = run_async(enrich_project_details.run(discovered=discovered))
            assert len(enriched) > 0

            with patch("kochi_launches_pipeline.es_client", return_value=mock_es_client):
                with patch("kochi_launches_pipeline.helpers") as mock_helpers:
                    mock_helpers.bulk.return_value = (len(enriched), [])
                    indexed = run_async(standardize_and_index.run(enriched=enriched))
                    assert indexed >= 0

    @patch("kochi_launches_pipeline._build_http_client")
    def test_dedup_across_sources(self, mock_client_builder, mock_es_client):
        mock_session = MagicMock()
        mock_client_builder.return_value = (mock_session, False)

        record1 = normalize_project_record({
            "project_name": "Same Project",
            "builder_name": "Same Builder",
            "price_min": 5000000,
            "amenities": ["Pool"],
            "source": "duckduckgo",
        })
        record2 = normalize_project_record({
            "project_name": "Same Project",
            "builder_name": "Same Builder",
            "price_min": 5000000,
            "price_max": 8000000,
            "amenities": ["Pool", "Gym"],
            "source": "prestige_prelaunch",
        })

        with patch("kochi_launches_pipeline.es_client", return_value=mock_es_client):
            with patch("kochi_launches_pipeline.helpers") as mock_helpers:
                mock_helpers.bulk.return_value = (1, [])
                indexed = run_async(standardize_and_index.run(enriched=[record1, record2]))
                call_args = mock_helpers.bulk.call_args
                actions = list(call_args[0][1])
                assert len(actions) == 1
                doc = actions[0]["_source"]
                assert doc["price_max"] == 8000000
