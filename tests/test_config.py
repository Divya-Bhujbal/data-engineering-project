import copy
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakehouse.config import ConfigError, load_source, load_sources, order_by_dependency, validate_source  # noqa: E402

VALID = {
    "source": "orders",
    "landing_subdir": "orders",
    "primary_key": ["order_id"],
    "columns": [
        {"name": "order_id", "type": "string"},
        {"name": "amount", "type": "decimal(12,2)"},
        {"name": "order_date", "type": "date", "format": "yyyy-MM-dd"},
    ],
    "rules": [{"name": "amount_ok", "type": "range", "column": "amount", "min": 0}],
}


def test_shipped_configs_are_valid():
    cfgs = load_sources(ROOT / "configs" / "sources")
    assert [c["source"] for c in cfgs] == ["customers", "products", "orders"]  # dependencies first


def test_valid_config_has_no_errors():
    assert validate_source(VALID) == []


def test_missing_required_key():
    bad = copy.deepcopy(VALID)
    del bad["primary_key"]
    assert any("primary_key" in e for e in validate_source(bad))


def test_unknown_primary_key_column():
    bad = copy.deepcopy(VALID)
    bad["primary_key"] = ["nope"]
    assert any("nope" in e for e in validate_source(bad))


def test_unsupported_type():
    bad = copy.deepcopy(VALID)
    bad["columns"][1]["type"] = "float128"
    assert any("float128" in e for e in validate_source(bad))


def test_format_only_for_dates():
    bad = copy.deepcopy(VALID)
    bad["columns"][0]["format"] = "yyyy"
    assert any("format" in e for e in validate_source(bad))


def test_unknown_rule_type_and_column():
    bad = copy.deepcopy(VALID)
    bad["rules"] = [
        {"name": "r1", "type": "magic", "column": "amount"},
        {"name": "r2", "type": "range", "column": "missing_col", "min": 1},
    ]
    errors = validate_source(bad)
    assert any("unknown type 'magic'" in e for e in errors)
    assert any("missing_col" in e for e in errors)


def test_range_needs_bound_and_regex_needs_pattern():
    bad = copy.deepcopy(VALID)
    bad["rules"] = [
        {"name": "r1", "type": "range", "column": "amount"},
        {"name": "r2", "type": "regex", "column": "order_id"},
    ]
    errors = validate_source(bad)
    assert any("min" in e for e in errors)
    assert any("pattern" in e for e in errors)


def test_duplicate_rule_names_and_bad_severity():
    bad = copy.deepcopy(VALID)
    bad["rules"] = [
        {"name": "r1", "type": "not_null", "column": "amount"},
        {"name": "r1", "type": "not_null", "column": "amount", "severity": "fatal"},
    ]
    errors = validate_source(bad)
    assert any("duplicate rule" in e for e in errors)
    assert any("severity" in e for e in errors)


def test_rule_name_cannot_clash_with_column():
    bad = copy.deepcopy(VALID)
    bad["rules"] = [{"name": "amount", "type": "not_null", "column": "amount"}]
    assert any("clashes" in e for e in validate_source(bad))


def test_quality_gate_range():
    bad = copy.deepcopy(VALID)
    bad["quality_gate"] = {"max_error_rate": 1.5}
    assert any("max_error_rate" in e for e in validate_source(bad))


def test_dependency_order_and_unknown_reference():
    a = {"source": "a", "rules": [{"type": "referential", "ref_source": "b"}]}
    b = {"source": "b", "rules": []}
    assert [c["source"] for c in order_by_dependency({"a": a, "b": b})] == ["b", "a"]
    with pytest.raises(ConfigError):
        order_by_dependency({"a": a})


def test_circular_dependency_detected():
    a = {"source": "a", "rules": [{"type": "referential", "ref_source": "b"}]}
    b = {"source": "b", "rules": [{"type": "referential", "ref_source": "a"}]}
    with pytest.raises(ConfigError):
        order_by_dependency({"a": a, "b": b})


def test_load_source_raises_with_all_problems(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("source: Bad-Name\nlanding_subdir: x\nprimary_key: [id]\ncolumns: []\n")
    with pytest.raises(ConfigError) as e:
        load_source(p)
    assert "Bad-Name" in str(e.value)
