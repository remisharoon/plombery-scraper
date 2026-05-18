import pytest

from kochi_launches_pipeline import (
    _parse_price,
    _parse_area,
    _normalize_configurations,
    _normalize_property_types,
    _normalize_amenities,
    _generate_project_id,
    _normalize_scalar,
    _as_text,
    _maybe_int,
    _maybe_float,
    normalize_project_record,
    deduplicate_projects,
    STANDARD_SCHEMA_FIELDS,
    MANDATORY_FIELDS,
    ai_classify_builder,
    KNOWN_PREMIUM_BUILDERS,
)


class TestParsePrice:
    def test_lakh(self):
        pmin, pmax, cur = _parse_price("₹75 Lac")
        assert pmin == 7500000
        assert cur == "INR"

    def test_crore(self):
        pmin, pmax, cur = _parse_price("1.2 Cr")
        assert pmin == 12000000
        assert cur == "INR"

    def test_range(self):
        pmin, pmax, cur = _parse_price("₹75 Lac - 1.2 Cr")
        assert pmin == 7500000
        assert pmax == 12000000
        assert cur == "INR"

    def test_numeric(self):
        pmin, pmax, cur = _parse_price(5000000)
        assert pmin == 5000000
        assert cur == "INR"

    def test_price_on_request(self):
        pmin, pmax, cur = _parse_price("Price on Request")
        assert pmin is None
        assert pmax is None

    def test_none(self):
        pmin, pmax, cur = _parse_price(None)
        assert pmin is None
        assert pmax is None

    def test_rs_prefix(self):
        pmin, pmax, cur = _parse_price("Rs. 45 Lakh")
        assert pmin == 4500000
        assert cur == "INR"

    def test_crore_range(self):
        pmin, pmax, cur = _parse_price("1.5 Cr - 3 Cr")
        assert pmin == 15000000
        assert pmax == 30000000
        assert cur == "INR"

    def test_k_format(self):
        pmin, pmax, cur = _parse_price("25K")
        assert pmin == 25000
        assert cur == "INR"


class TestParseArea:
    def test_sqft(self):
        assert _parse_area("1250 sq.ft") == 1250.0

    def test_sqft_no_dot(self):
        assert _parse_area("2400 sqft") == 2400.0

    def test_numeric(self):
        assert _parse_area(1500) == 1500.0

    def test_none(self):
        assert _parse_area(None) is None

    def test_cents(self):
        result = _parse_area("10 cents")
        assert result == pytest.approx(4356.0)

    def test_sqm(self):
        result = _parse_area("100 sq.m")
        assert result == pytest.approx(1076.39, rel=0.01)


class TestNormalizeConfigurations:
    def test_comma_separated(self):
        result = _normalize_configurations("2,3 BHK")
        assert "2BHK" in result
        assert "3BHK" in result

    def test_list_input(self):
        result = _normalize_configurations(["2BHK", "3BHK", "4BHK"])
        assert len(result) == 3

    def test_none(self):
        assert _normalize_configurations(None) == []

    def test_already_bhk(self):
        result = _normalize_configurations("3BHK, 4BHK")
        assert "3BHK" in result
        assert "4BHK" in result

    def test_single(self):
        result = _normalize_configurations("3 BHK")
        assert "3BHK" in result

    def test_empty_string(self):
        assert _normalize_configurations("") == []


class TestNormalizePropertyTypes:
    def test_apartment(self):
        assert _normalize_property_types("Apartment") == ["apartment"]

    def test_flat_maps_to_apartment(self):
        assert _normalize_property_types("Flat") == ["apartment"]

    def test_villa(self):
        assert _normalize_property_types("Villa") == ["villa"]

    def test_list(self):
        result = _normalize_property_types(["Apartment", "Villa"])
        assert "apartment" in result
        assert "villa" in result

    def test_none(self):
        assert _normalize_property_types(None) == []

    def test_plotted(self):
        result = _normalize_property_types("Plotted Development")
        assert "plotted-development" in result

    def test_dedup(self):
        result = _normalize_property_types("Apartment & Flat")
        assert result.count("apartment") == 1


class TestNormalizeAmenities:
    def test_comma_separated(self):
        result = _normalize_amenities("Swimming Pool, Gym, Club House")
        assert len(result) == 3
        assert "Swimming Pool" in result

    def test_list(self):
        result = _normalize_amenities(["swimming pool", "gym"])
        assert len(result) == 2
        assert result[0] == "Swimming Pool"

    def test_none(self):
        assert _normalize_amenities(None) == []

    def test_empty(self):
        assert _normalize_amenities("") == []


class TestGenerateProjectId:
    def test_deterministic(self):
        id1 = _generate_project_id("Prestige Dolphins Court", "Prestige Group")
        id2 = _generate_project_id("Prestige Dolphins Court", "Prestige Group")
        assert id1 == id2

    def test_different_projects(self):
        id1 = _generate_project_id("Prestige Dolphins Court", "Prestige Group")
        id2 = _generate_project_id("Sobha Crystal Meadows", "Sobha Limited")
        assert id1 != id2

    def test_case_insensitive(self):
        id1 = _generate_project_id("Prestige Dolphins Court", "Prestige Group")
        id2 = _generate_project_id("prestige dolphins court", "prestige group")
        assert id1 == id2

    def test_whitespace_normalized(self):
        id1 = _generate_project_id("  Prestige  Dolphins  Court  ", "  Prestige  Group  ")
        id2 = _generate_project_id("Prestige Dolphins Court", "Prestige Group")
        assert id1 == id2


class TestNormalizeProjectRecord:
    def test_full_record(self, sample_project_record):
        record = normalize_project_record(sample_project_record)
        assert record["project_name"] == "Prestige Dolphins Court"
        assert record["builder_name"] == "Prestige Group"
        assert record["city"] == "Kochi"
        assert record["state"] == "Kerala"
        assert record["id"] is not None
        assert record["discovered_at"] is not None
        assert record["updated_at"] is not None

    def test_minimal_record(self):
        record = normalize_project_record({"project_name": "Test Project"})
        assert record["project_name"] == "Test Project"
        assert record["builder_name"] == "Unknown"
        assert record["city"] == "Kochi"
        assert record["state"] == "Kerala"
        assert record["launch_status"] == "new-launch"
        assert record["price_currency"] == "INR"

    def test_empty_name_returns_empty(self):
        record = normalize_project_record({"project_name": ""})
        assert record == {}

    def test_none_name_returns_empty(self):
        record = normalize_project_record({"project_name": None})
        assert record == {}

    def test_id_generated(self, sample_project_record):
        record = normalize_project_record(sample_project_record)
        assert record["id"] == _generate_project_id(
            sample_project_record["project_name"],
            sample_project_record["builder_name"],
        )

    def test_discovered_at_preserved(self, sample_project_record):
        sample_project_record["discovered_at"] = "2025-01-15T00:00:00+00:00"
        record = normalize_project_record(sample_project_record)
        assert record["discovered_at"] == "2025-01-15T00:00:00+00:00"

    def test_defaults_filled(self):
        record = normalize_project_record({"project_name": "X", "builder_name": "Y"})
        assert record["property_types"] == []
        assert record["configurations"] == []
        assert record["amenities"] == []
        assert record["project_highlights"] == []

    def test_null_fields_removed(self, sample_project_record):
        sample_project_record["district"] = None
        record = normalize_project_record(sample_project_record)
        assert "district" not in record

    def test_price_converted_to_int(self):
        record = normalize_project_record({
            "project_name": "Test",
            "builder_name": "Test Builder",
            "price_min": 7500000.0,
            "price_max": 12000000.0,
        })
        assert isinstance(record.get("price_min"), int)
        assert isinstance(record.get("price_max"), int)


class TestDeduplicateProjects:
    def test_same_project_merged(self, sample_project_record, sample_project_record_2):
        both = [normalize_project_record(sample_project_record), normalize_project_record(sample_project_record_2)]
        result = deduplicate_projects(both)
        assert len(result) == 1
        merged = result[0]
        assert merged["price_max"] == 15000000
        assert "magicbricks" in merged["source"]
        assert "housing" in merged["source"]
        assert len(merged.get("amenities", [])) >= 4

    def test_different_projects_kept(self, sample_project_record):
        other = dict(sample_project_record)
        other["project_name"] = "Different Project"
        other["builder_name"] = "Different Builder"
        p1 = normalize_project_record(sample_project_record)
        p2 = normalize_project_record(other)
        result = deduplicate_projects([p1, p2])
        assert len(result) == 2

    def test_empty_list(self):
        result = deduplicate_projects([])
        assert result == []

    def test_richest_source_wins(self):
        record1 = normalize_project_record({"project_name": "Test", "builder_name": "Builder", "price_min": 5000000, "source": "google"})
        record2 = normalize_project_record({"project_name": "Test", "builder_name": "Builder", "price_min": 5000000, "amenities": ["Pool", "Gym"], "source": "magicbricks"})
        result = deduplicate_projects([record1, record2])
        assert len(result) == 1
        assert len(result[0].get("amenities", [])) > 0


class TestBuilderTierClassification:
    def test_known_premium_builder(self):
        tier = ai_classify_builder("Prestige Group", {"price_min": 7500000})
        assert tier == "premium"

    def test_known_premium_sobha(self):
        tier = ai_classify_builder("Sobha Limited", {"price_min": 15000000})
        assert tier == "premium"

    def test_known_mid_segment(self):
        tier = ai_classify_builder("Artech", {"price_min": 5000000})
        assert tier == "mid-segment"

    def test_price_based_luxury_villa(self):
        tier = ai_classify_builder("Unknown Builder XYZ", {"price_min": 50000000, "property_types": ["villa"]})
        assert tier == "luxury"

    def test_price_based_affordable(self):
        tier = ai_classify_builder("Unknown Builder ABC", {"price_min": 2500000, "property_types": ["apartment"]})
        assert tier == "affordable"

    def test_price_based_premium_apartment(self):
        tier = ai_classify_builder("Unknown Builder PQR", {"price_min": 9000000, "property_types": ["apartment"]})
        assert tier == "premium"


class TestSchemaValidation:
    def test_standard_schema_fields_list(self):
        assert "project_name" in STANDARD_SCHEMA_FIELDS
        assert "builder_name" in STANDARD_SCHEMA_FIELDS
        assert "builder_tier" in STANDARD_SCHEMA_FIELDS
        assert "launch_status" in STANDARD_SCHEMA_FIELDS
        assert "rera_number" in STANDARD_SCHEMA_FIELDS
        assert "amenities" in STANDARD_SCHEMA_FIELDS

    def test_mandatory_fields_subset_of_standard(self):
        for field in MANDATORY_FIELDS:
            assert field in STANDARD_SCHEMA_FIELDS

    def test_normalized_record_has_mandatory_fields(self, sample_project_record):
        record = normalize_project_record(sample_project_record)
        for field in MANDATORY_FIELDS:
            assert field in record, f"Missing mandatory field: {field}"

    def test_normalized_record_null_handling(self):
        record = normalize_project_record({"project_name": "Minimal", "builder_name": "Builder"})
        for field in MANDATORY_FIELDS:
            assert record.get(field) is not None, f"Mandatory field {field} is None"


class TestHelperFunctions:
    def test_normalize_scalar_none(self):
        assert _normalize_scalar(None) is None

    def test_normalize_scalar_empty(self):
        assert _normalize_scalar("") is None

    def test_normalize_scalar_null_string(self):
        assert _normalize_scalar("NULL") is None

    def test_normalize_scalar_whitespace(self):
        assert _normalize_scalar("  hello  ") == "hello"

    def test_as_text_dict(self):
        assert _as_text({"value": "test"}) == "test"

    def test_as_text_list(self):
        assert _as_text(["first", "second"]) == "first"

    def test_maybe_int_commas(self):
        assert _maybe_int("1,250") == 1250

    def test_maybe_float_commas(self):
        assert _maybe_float("3.5") == 3.5

    def test_maybe_int_none(self):
        assert _maybe_int(None) is None

    def test_maybe_float_none(self):
        assert _maybe_float(None) is None
