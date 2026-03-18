#!/usr/bin/env python3
"""
Validate skills_map.json by rolling up raw skill frequencies and comparing with
canonical frequencies from extractor. Emits quick metrics and CSVs for review.
Now scans aliases within each raw line (word-boundary matching), allowing
multiple canonical hits per line to better mirror extractor behavior.

Inputs (defaults):
- data/processed/skills_raw_freq.csv (columns: skill,count)
- data/processed/skills_canonical_freq.csv (columns: canonical_skill,count)
- config/skills_map.json (or docs/skills_map.json as fallback)

Outputs (under data/processed/):
- skills_rollup_from_map.csv (canonical,count)
- skills_rollup_vs_extractor.csv (canonical,from_map,from_extractor,delta)
- skills_unmatched_top.csv (skill,count)  # raw skills not present in map
- skills_validation_report.json (summary metrics)

Usage:
  python -m src.scripts.validate_skills_map \
    --raw data/processed/skills_raw_freq.csv \
    --canonical data/processed/skills_canonical_freq.csv \
    --map config/skills_map.json \
    --out-dir data/processed

"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Tuple, List
import re


def load_csv_counts(path: Path, key_field: str, count_field: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row.get(key_field) or "").strip()
            if not key:
                continue
            try:
                cnt = int(str(row.get(count_field, "0")).strip() or 0)
            except ValueError:
                continue
            out[key] = out.get(key, 0) + cnt
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate skills_map.json roll-ups vs extractor")
    ap.add_argument("--raw", type=Path, default=Path("data/processed/skills_raw_freq.csv"))
    ap.add_argument("--canonical", type=Path, default=Path("data/processed/skills_canonical_freq.csv"))
    ap.add_argument("--map", dest="map_path", type=Path, default=Path("config/skills_map.json"))
    ap.add_argument("--out-dir", type=Path, default=Path("data/processed"))
    args = ap.parse_args()

    raw_counts = load_csv_counts(args.raw, key_field="skill", count_field="count")
    extractor_counts = load_csv_counts(args.canonical, key_field="canonical_skill", count_field="count")

    # Load skills map, fallback to docs if config missing
    map_path = args.map_path
    if not map_path.exists():
        alt = Path("docs/skills_map.json")
        if alt.exists():
            map_path = alt
    if not map_path.exists():
        raise FileNotFoundError(f"skills map not found at {args.map_path} or docs/skills_map.json")

    with map_path.open("r", encoding="utf-8") as f:
        skills_map = json.load(f)
    # Build alias patterns with word-boundary semantics, longest-first to catch longer phrases
    alias_patterns: List[Tuple[str, str, re.Pattern]] = []  # (alias, canonical, regex)
    short_alias_allow = {"r"}  # safe single-char allowlist
    for k, v in skills_map.items():
        if not isinstance(k, str) or not isinstance(v, str):
            continue
        alias = k.strip()
        if len(alias) < 2 and alias.lower() not in short_alias_allow:
            continue  # skip ultra-short unless allowlisted
        pattern = re.compile(rf"(?<!\w){re.escape(alias)}(?!\w)", re.IGNORECASE)
        alias_patterns.append((alias.lower(), v, pattern))
    # Sort by alias length desc so longer phrases are tried first (mostly useful if later we short-circuit)
    alias_patterns.sort(key=lambda t: -len(t[0]))

    # Roll-up RAW via alias-in-text scanning
    rollup: Dict[str, int] = {}
    unmatched: Dict[str, int] = {}
    lines_with_any_hit = 0
    for raw, cnt in raw_counts.items():
        text = str(raw)
        hits: set[str] = set()
        for _alias, canonical, pat in alias_patterns:
            if pat.search(text):
                hits.add(canonical)
        if not hits:
            unmatched[raw] = unmatched.get(raw, 0) + cnt
            continue
        lines_with_any_hit += cnt
        for c in hits:
            rollup[c] = rollup.get(c, 0) + cnt

    # Compare with extractor canonical counts
    all_canon = set(rollup) | set(extractor_counts)
    vs_rows: Dict[str, Tuple[int, int, int]] = {}
    for c in sorted(all_canon):
        m = rollup.get(c, 0)
        e = extractor_counts.get(c, 0)
        vs_rows[c] = (m, e, m - e)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    # Write roll-up from map
    with (args.out_dir / "skills_rollup_from_map.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["canonical", "count"])
        for c, cnt in sorted(rollup.items(), key=lambda x: (-x[1], x[0])):
            w.writerow([c, cnt])

    # Write comparison
    with (args.out_dir / "skills_rollup_vs_extractor.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["canonical", "from_map", "from_extractor", "delta"])
        for c, (m, e, d) in sorted(vs_rows.items(), key=lambda x: (-max(x[1][0], x[1][1]), x[0])):
            w.writerow([c, m, e, d])

    # Write unmatched
    with (args.out_dir / "skills_unmatched_top.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["skill", "count"])
        for raw, cnt in sorted(unmatched.items(), key=lambda x: (-x[1], x[0])):
            w.writerow([raw, cnt])

    # Metrics
    total_raw_lines = sum(raw_counts.values())
    hits_total = sum(rollup.values())  # counts across all canonical hits
    unmatched_lines = sum(unmatched.values())
    coverage_lines_any = (lines_with_any_hit / total_raw_lines) * 100.0 if total_raw_lines else 0.0
    coverage_hits = (hits_total / total_raw_lines) * 100.0 if total_raw_lines else 0.0

    report = {
        "inputs": {
            "raw_csv": str(args.raw),
            "canonical_csv": str(args.canonical),
            "skills_map": str(map_path),
        },
        "counts": {
            "unique_raw": len(raw_counts),
            "unique_canonical_from_map": len(rollup),
            "unique_canonical_extractor": len(extractor_counts),
        },
        "lines": {
            "total_raw_lines": total_raw_lines,
            "lines_with_any_hit": lines_with_any_hit,
            "unmatched_lines_via_map": unmatched_lines,
            "coverage_percent_lines_with_any_hit": round(coverage_lines_any, 2),
            "hits_total_across_canonicals": hits_total,
            "coverage_percent_hits": round(coverage_hits, 2),
        },
    }

    with (args.out_dir / "skills_validation_report.json").open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # Print concise summary
    print(json.dumps(report, indent=2))
    print(f"Wrote: {args.out_dir/'skills_rollup_from_map.csv'}")
    print(f"Wrote: {args.out_dir/'skills_rollup_vs_extractor.csv'}")
    print(f"Wrote: {args.out_dir/'skills_unmatched_top.csv'}")
    print(f"Wrote: {args.out_dir/'skills_validation_report.json'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
