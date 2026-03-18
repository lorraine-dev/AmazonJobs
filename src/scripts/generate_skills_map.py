"""
Generate a consolidated skills map (raw_variant -> canonical_label).

Sources:
- Canonical aliases from config/skill_aliases.yaml
- High-confidence fuzzy merges from data/processed/skills_fuzzy_candidates*.csv

Outputs:
- config/skills_map.json
- docs/skills_map.json (copied)

Usage:
  python -m src.scripts.generate_skills_map \
    --aliases config/skill_aliases.yaml \
    --fuzzy-candidates data/processed/skills_fuzzy_candidates.csv \
    --min-score 100 \
    --output-config-json config/skills_map.json \
    --output-docs-json docs/skills_map.json

Notes:
- We prioritize YAML aliases over fuzzy merges on conflict.
- We only accept fuzzy merges whose candidate canonical is a known canonical from YAML.
- Default is conservative (min-score=100) to avoid false merges.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Optional

from src.utils.skills_canonicalizer import load_skill_aliases


def load_yaml_alias_map(aliases_path: Optional[Path]) -> Dict[str, str]:
    cfg = load_skill_aliases(aliases_path) if aliases_path else load_skill_aliases()
    # canonical_map: Dict[canonical, List[aliases]]
    alias_map: Dict[str, str] = {}
    for canonical, alias_list in cfg.canonical_map.items():
        # Map each alias to canonical; also map canonical to itself for completeness
        alias_map.setdefault(canonical, canonical)
        for alias in alias_list:
            # Preserve alias surface form as-is
            alias_map.setdefault(alias, canonical)
    return alias_map


def merge_fuzzy_candidates(
    base_map: Dict[str, str],
    fuzzy_csv: Optional[Path],
    known_canonicals: set,
    min_score: int,
) -> Dict[str, str]:
    if not fuzzy_csv or not fuzzy_csv.exists():
        return base_map

    out = dict(base_map)
    with fuzzy_csv.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Expect columns: variant,candidate_canonical,score,...
        for row in reader:
            try:
                variant = (row.get("variant") or "").strip()
                canonical = (row.get("candidate_canonical") or "").strip()
                score_str = (row.get("score") or "0").strip()
                score = int(float(score_str)) if score_str else 0
            except Exception:
                continue
            if not variant or not canonical:
                continue
            if canonical not in known_canonicals:
                # Only accept merges to known canonicals (from YAML)
                continue
            if score < min_score:
                continue
            # Respect YAML-defined mapping on conflict
            if variant in out and out[variant] != canonical:
                continue
            out.setdefault(variant, canonical)
    return out


def write_json_map(mapping: Dict[str, str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2, sort_keys=True)


def copy_to_docs(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    data = src.read_text(encoding="utf-8")
    dst.write_text(data, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate skills_map.json from YAML aliases and fuzzy candidates")
    parser.add_argument("--aliases", type=str, default="", help="Path to config/skill_aliases.yaml (optional)")
    parser.add_argument(
        "--fuzzy-candidates",
        type=str,
        default="data/processed/skills_fuzzy_candidates.csv",
        help="Path to fuzzy candidates CSV (optional)",
    )
    parser.add_argument("--min-score", type=int, default=100, help="Minimum score to accept a fuzzy merge")
    parser.add_argument("--output-config-json", type=str, default="config/skills_map.json")
    parser.add_argument("--output-docs-json", type=str, default="docs/skills_map.json")
    args = parser.parse_args()

    aliases_path = Path(args.aliases) if args.aliases else None
    fuzzy_path = Path(args.fuzzy_candidates) if args.fuzzy_candidates else None

    # 1) Base from YAML aliases
    alias_map = load_yaml_alias_map(aliases_path)
    known_canonicals = {k for k, v in alias_map.items() if k == v}

    # 2) Merge in high-confidence fuzzy candidates
    merged = merge_fuzzy_candidates(alias_map, fuzzy_path, known_canonicals, args.min_score)

    # 3) Write outputs
    config_out = Path(args.output_config_json)
    docs_out = Path(args.output_docs_json)
    write_json_map(merged, config_out)
    copy_to_docs(config_out, docs_out)

    print(f"Wrote skills map with {len(merged)} entries to: {config_out}")
    print(f"Copied to: {docs_out}")


if __name__ == "__main__":
    main()
