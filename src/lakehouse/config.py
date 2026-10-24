"""Load and validate source/project configs. Pure Python (no Spark) so it is unit-testable anywhere."""
import re
from pathlib import Path

import yaml

TYPE_PATTERN = re.compile(
    r"^(string|int|bigint|double|boolean|date|timestamp|decimal\(\s*\d+\s*,\s*\d+\s*\))$"
)
NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
RULE_TYPES = {"not_null", "unique", "regex", "range", "allowed_values", "referential", "expression"}
SEVERITIES = {"error", "warn"}


class ConfigError(ValueError):
    """Raised when a config file is invalid. The message lists every problem found."""


def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_source(cfg):
    """Return a list of human-readable problems (empty list = valid)."""
    errors = []
    if not isinstance(cfg, dict):
        return ["config must be a YAML mapping"]

    for key in ("source", "landing_subdir", "primary_key", "columns"):
        if key not in cfg:
            errors.append(f"missing required key '{key}'")
    if errors:
        return errors

    if not NAME_PATTERN.match(str(cfg["source"])):
        errors.append(f"source name '{cfg['source']}' must be lowercase letters, digits, underscores")

    # columns
    col_names = []
    for i, col in enumerate(cfg["columns"]):
        name = col.get("name")
        if not name or not NAME_PATTERN.match(str(name)):
            errors.append(f"columns[{i}]: invalid or missing name '{name}'")
            continue
        if name in col_names:
            errors.append(f"columns[{i}]: duplicate column name '{name}'")
        col_names.append(name)
        typ = str(col.get("type", "string"))
        if not TYPE_PATTERN.match(typ):
            errors.append(f"column '{name}': unsupported type '{typ}'")
        if col.get("format") and typ not in ("date", "timestamp"):
            errors.append(f"column '{name}': 'format' only applies to date/timestamp")

    for k in cfg["primary_key"]:
        if k not in col_names:
            errors.append(f"primary_key column '{k}' is not defined in columns")

    # rules
    seen = set()
    for i, rule in enumerate(cfg.get("rules", [])):
        label = f"rules[{i}]"
        name = rule.get("name")
        rtype = rule.get("type")
        if not name or not NAME_PATTERN.match(str(name)):
            errors.append(f"{label}: invalid or missing name '{name}'")
            continue
        label = f"rule '{name}'"
        if name in seen:
            errors.append(f"{label}: duplicate rule name")
        seen.add(name)
        if name in col_names:
            errors.append(f"{label}: rule name clashes with a column name")
        if rtype not in RULE_TYPES:
            errors.append(f"{label}: unknown type '{rtype}' (allowed: {sorted(RULE_TYPES)})")
            continue
        if rule.get("severity", "error") not in SEVERITIES:
            errors.append(f"{label}: severity must be one of {sorted(SEVERITIES)}")

        needs_column = rtype in {"not_null", "regex", "range", "allowed_values", "referential"}
        if needs_column and rule.get("column") not in col_names:
            errors.append(f"{label}: column '{rule.get('column')}' is not defined in columns")
        if rtype == "regex" and not rule.get("pattern"):
            errors.append(f"{label}: regex rule needs 'pattern'")
        if rtype == "range" and rule.get("min") is None and rule.get("max") is None:
            errors.append(f"{label}: range rule needs 'min' and/or 'max'")
        if rtype == "allowed_values" and not rule.get("values"):
            errors.append(f"{label}: allowed_values rule needs 'values'")
        if rtype == "referential":
            for k in ("ref_source", "ref_column"):
                if not rule.get(k):
                    errors.append(f"{label}: referential rule needs '{k}'")
        if rtype == "expression" and not rule.get("expr"):
            errors.append(f"{label}: expression rule needs 'expr'")
        if rtype == "unique":
            for c in rule.get("columns", []):
                if c not in col_names:
                    errors.append(f"{label}: column '{c}' is not defined in columns")

    gate = cfg.get("quality_gate")
    if gate is not None:
        rate = gate.get("max_error_rate")
        if not isinstance(rate, (int, float)) or not 0 <= rate <= 1:
            errors.append("quality_gate.max_error_rate must be a number between 0 and 1")
    return errors


def load_source(path):
    cfg = load_yaml(path)
    problems = validate_source(cfg)
    if problems:
        raise ConfigError(f"{Path(path).name}: " + "; ".join(problems))
    return cfg


def load_sources(directory):
    """Load every *.yaml in a directory, ordered so referenced sources load first."""
    cfgs = {}
    for p in sorted(Path(directory).glob("*.yaml")):
        cfg = load_source(p)
        cfgs[cfg["source"]] = cfg
    return order_by_dependency(cfgs)


def order_by_dependency(cfgs):
    """Topologically sort sources so referential rules always find their reference table."""
    deps = {
        name: {r["ref_source"] for r in cfg.get("rules", []) if r["type"] == "referential"} - {name}
        for name, cfg in cfgs.items()
    }
    for name, d in deps.items():
        missing = d - set(cfgs)
        if missing:
            raise ConfigError(f"source '{name}' references unknown source(s): {sorted(missing)}")
    ordered, done = [], set()
    while len(ordered) < len(cfgs):
        ready = [n for n in sorted(cfgs) if n not in done and deps[n] <= done]
        if not ready:
            raise ConfigError("circular referential dependency between sources")
        for n in ready:
            ordered.append(cfgs[n])
            done.add(n)
    return ordered


def load_project(path):
    cfg = load_yaml(path)
    for key in ("catalog", "schema", "landing_root"):
        if key not in cfg:
            raise ConfigError(f"project config missing '{key}'")
    cfg.setdefault("gold", [])
    return cfg
