"""
Extract unique raw skills and canonicalized counts from the combined jobs CSV.

Outputs (by default under data/processed/):
- skills_raw_freq.csv: columns [skill, count]
- skills_canonical_freq.csv: columns [canonical_skill, count]
- skills_pairs_sample.csv: sample of [job_id, section, raw_skill, canonical_skill]

Usage:
  python -m src.scripts.extract_skills --output-dir data/processed
  python src/scripts/extract_skills.py --output-dir data/processed

Notes:
- Uses config path via src/utils/paths.get_combined_file()
- Relies on columns: basic_qualifications, preferred_qualifications (HTML/text)
- Applies alias mapping from config/skill_aliases.yaml if present
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from src.utils.paths import get_combined_file
from src.utils.skills_canonicalizer import (
    SkillAliasConfig,
    build_alias_index,
    filter_stop_phrases,
    normalize_text,
    load_skill_aliases,
    split_html_bullets,
    prepare_alias_scanner,
    scan_line_for_skills,
)


def iter_skill_lines(df: pd.DataFrame) -> Iterable[Tuple[str, List[str]]]:
    """Yield pairs (section, lines) for each relevant column per row."""
    for _, row in df.iterrows():
        basic_html = row.get("basic_qualifications", None)
        pref_html = row.get("preferred_qualifications", None)
        # Fallback to legacy column names if new fields are absent/empty
        if (basic_html is None or (isinstance(basic_html, float) and pd.isna(basic_html))) and "basic_qual" in row.index:
            basic_html = row.get("basic_qual", None)
        if (pref_html is None or (isinstance(pref_html, float) and pd.isna(pref_html))) and "pref_qual" in row.index:
            pref_html = row.get("pref_qual", None)
        if isinstance(basic_html, float) and pd.isna(basic_html):
            basic_html = None
        if isinstance(pref_html, float) and pd.isna(pref_html):
            pref_html = None
        if basic_html:
            yield ("basic", split_html_bullets(basic_html))
        if pref_html:
            yield ("preferred", split_html_bullets(pref_html))


def compute_frequencies(df: pd.DataFrame, alias_cfg: SkillAliasConfig):
    alias_index = build_alias_index(alias_cfg.canonical_map)
    scanner = prepare_alias_scanner(alias_index)

    raw_counter: Counter[str] = Counter()
    canon_counter: Counter[str] = Counter()
    unmatched_counter: Counter[str] = Counter()
    pairs_sample: List[Tuple[str, str, str, str]] = []  # job_id, section, raw, canon

    # job_id may not always exist, so coerce to string if present
    id_col = "id" if "id" in df.columns else None

    for idx, row in df.iterrows():
        basic_html = row.get("basic_qualifications", None)
        pref_html = row.get("preferred_qualifications", None)
        # Fallback to legacy column names if new fields are absent/empty
        if (basic_html is None or (isinstance(basic_html, float) and pd.isna(basic_html))) and "basic_qual" in row.index:
            basic_html = row.get("basic_qual", None)
        if (pref_html is None or (isinstance(pref_html, float) and pd.isna(pref_html))) and "pref_qual" in row.index:
            pref_html = row.get("pref_qual", None)
        job_id = str(row.get(id_col)) if id_col else str(idx)

        sections = []
        if basic_html and not (isinstance(basic_html, float) and pd.isna(basic_html)):
            sections.append(("basic", split_html_bullets(basic_html)))
        if pref_html and not (isinstance(pref_html, float) and pd.isna(pref_html)):
            sections.append(("preferred", split_html_bullets(pref_html)))

        for section, lines in sections:
            # Apply stop phrase filtering before matching
            filtered_lines = filter_stop_phrases(lines, alias_cfg.stop_phrases)
            # raw frequency (post-filter)
            for s in filtered_lines:
                raw_counter[s] += 1
            # canonical via inline scanning; collect unmatched for audit
            for s in filtered_lines:
                matched_canons = scan_line_for_skills(s, scanner)
                if not matched_canons:
                    unmatched_counter[s] += 1
                    continue
                for canon in matched_canons:
                    pairs_sample.append((job_id, section, s, canon))
                    canon_counter[canon] += 1

    return raw_counter, canon_counter, pairs_sample, unmatched_counter


def _load_taxonomy_category_map() -> Dict[str, str]:
    """Load docs/skills_taxonomy.json and return mapping of canonical (lowercased) -> category.

    Falls back to empty mapping if file missing/invalid.
    """
    # Mirror DEFAULT_TAXONOMY_PATH logic from utils without importing internals
    tax_path = Path(__file__).resolve().parents[2] / "docs" / "skills_taxonomy.json"
    try:
        if not tax_path.exists():
            return {}
        with tax_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        mapping: Dict[str, str] = {}
        if isinstance(data, dict):
            for category, items in data.items():
                if not isinstance(items, list):
                    continue
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    name = str(it.get("name", "")).strip()
                    if not name:
                        continue
                    mapping[name.lower()] = str(category)
        return mapping
    except Exception:
        return {}


def _aggregate_by_category(canon_counter: Counter[str], canon_to_cat: Dict[str, str]) -> Counter[str]:
    by_cat: Counter[str] = Counter()
    for canon, c in canon_counter.items():
        cat = canon_to_cat.get(str(canon).lower(), "Uncategorized")
        by_cat[cat] += int(c)
    return by_cat


def _build_line_to_canons(pairs_sample: List[Tuple[str, str, str, str]]) -> Dict[str, set]:
    line_to_canons: Dict[str, set] = {}
    for _job_id, _section, raw_line, canon in pairs_sample:
        s = line_to_canons.setdefault(raw_line, set())
        s.add(canon)
    return line_to_canons


def _split_list_candidates(text: str) -> List[str]:
    """Extract likely list items from a line, prioritizing parenthetical content.

    Heuristics: pull items from (...) if present; otherwise split the whole line by
    comma/and/slash separators. Trim quotes and punctuation.
    """
    candidates: List[str] = []
    # Prefer content inside parentheses
    paren_matches = re.findall(r"\(([^)]{1,200})\)", text)
    segments: List[str] = paren_matches if paren_matches else [text]
    for seg in segments:
        # Remove leading e.g./i.e./such as
        seg = re.sub(r"\b(e\.g\.|i\.e\.|such as)\b[:\s]*", "", seg, flags=re.I)
        parts = re.split(r"\s*(?:,|\band\b|\bor\b|\/|\|)\s*", seg, flags=re.I)
        for p in parts:
            t = p.strip().strip('"\'').strip()
            if t:
                candidates.append(t)
    return candidates


def _is_toolish(token: str) -> bool:
    """Rough check to see if a token looks like a product/tool name.

    Accept if it contains uppercase letters, camelcase, or alnum with minimal symbols.
    """
    if len(token) < 3:
        return False
    if re.search(r"[0-9]{3,}", token):
        return False
    # Has uppercase or camel case or is all-caps short word
    if any(c.isupper() for c in token) or re.search(r"[A-Za-z]+[A-Z][a-z]+", token):
        return True
    # Allow simple alnum words (e.g., tableau) as potential tools
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9\+#-]{2,}", token))


_CANDIDATE_SKIP_PREFIXES = (
    "experience",
    "experience with",
    "experience in",
    "programming experience",
    "ability",
    "ability to",
    "strong",
    "proven",
    "have",
    "knowledge of",
    "advanced",
    "proficiency",
    "enrolled",
)


def _is_degree_token(token: str) -> bool:
    t = token.lower()
    return any(
        kw in t
        for kw in (
            "bachelor",  # bachelor's, bachelor degree
            "master",    # master's, masters
            "mba",
            "phd",
            "doctorate",
            "doctoral",
            "bs ",
            " bs",
            "ms ",
            " ms",
            "msc",
            "undergraduate",
            "graduate degree",
            "degree in",
        )
    )


def _should_ignore_candidate(token: str) -> bool:
    # Do not ignore degree-related tokens
    if _is_degree_token(token):
        return False
    # Skip generic leading phrases
    tl = token.strip().lower()
    if any(tl.startswith(pref) for pref in _CANDIDATE_SKIP_PREFIXES):
        return True
    # Skip e.g./i.e./etc remnants
    if re.search(r"\b(e\.g\.|i\.e\.|etc\.?|such as)\b", token, flags=re.I):
        return True
    # Skip tokens with apostrophes unless degree (handled above)
    if "'" in token:
        return True
    # Heuristic: drop overly long phrases (>5 words)
    if len(token.split()) > 5:
        return True
    # Drop obvious generic single/common words
    tl = token.strip().lower()
    _GENERIC_TOKENS = {
        "equivalent",
        "non-internship",
        "engineering",
        "operations",
        "testing",
        "implementation",
        "definition",
        "planning",
        "networking",
        "journals",
        "publications",
        "developers",
        "budget",
        "coaching",
        "c-level",
        "speak",
        "read",
        "written",
        "verbal",
        "team orientation",
        "analyzing",
        "reliability",
        "scaling",
        "optimization",
        "warehousing",
        "finance",
        "mathematics",
    }
    if tl in _GENERIC_TOKENS:
        return True
    # Drop generic container nouns like "space", "domain", "area"
    if re.search(r"\b(space|domain|area)\b", tl):
        return True
    return False


def _generate_suggestions(
    pairs_sample: List[Tuple[str, str, str, str]],
    raw_counter: Counter[str],
    unmatched_counter: Counter[str],
    alias_cfg: SkillAliasConfig,
    canon_to_cat: Dict[str, str],
):
    # Build known set from alias index (normalized)
    alias_index = build_alias_index(alias_cfg.canonical_map)
    scanner = prepare_alias_scanner(alias_index)
    known_norm = set(alias_index.keys()) | set(
        normalize_text(c) for c in alias_cfg.canonical_map.keys()
    )

    # Map raw line -> matched canon set
    line_to_canons = _build_line_to_canons(pairs_sample)

    # Aggregate suggestions
    candidate_counts: Counter[str] = Counter()
    candidate_reasons: Dict[str, set] = {}
    candidate_example: Dict[str, str] = {}
    candidate_cats: Dict[str, Counter[str]] = {}

    def add_candidate(name: str, base_line: str, reason: str, cats_from_line: List[str], weight: int):
        candidate_counts[name] += weight
        candidate_reasons.setdefault(name, set()).add(reason)
        candidate_example.setdefault(name, base_line)
        if cats_from_line:
            c = candidate_cats.setdefault(name, Counter())
            for cat in cats_from_line:
                c[cat] += weight

    # From peer-of-matched tokens on lines that had matches
    for line, canons in line_to_canons.items():
        items = _split_list_candidates(line)
        # Infer category from matched canons on this line
        cats = [canon_to_cat.get(c.lower(), "Uncategorized") for c in canons]
        for it in items:
            # Special-case: collapse 'B testing' tokenization artifact to 'A/B testing' if line indicates A/B testing
            if re.search(r"\b(a\s*/\s*b|a-\s*b|ab|a b)\s*testing\b", line, flags=re.I):
                if re.fullmatch(r"[ab]\s*testing", it, flags=re.I):
                    it = "A/B testing"
            if not _is_toolish(it):
                continue
            norm = normalize_text(it)
            if not norm or norm in known_norm:
                continue
            # If token already embeds a known alias (e.g., "Programming experience in Java"), skip it
            if not _is_degree_token(it):
                if scan_line_for_skills(it, scanner):
                    continue
            if _should_ignore_candidate(it):
                continue
            add_candidate(it, line, "peer-of-matched", cats, raw_counter.get(line, 1))

    # From unmatched lines: pick capitalized/toolish tokens
    for line, cnt in unmatched_counter.items():
        items = _split_list_candidates(line)
        # If no parentheses split yielded items, also try simple capitalized words
        if not items:
            items = re.findall(r"\b[A-Z][A-Za-z0-9\+#-]{2,}\b", line)
        for it in items:
            # Special-case: collapse 'B testing' tokenization artifact to 'A/B testing' if line indicates A/B testing
            if re.search(r"\b(a\s*/\s*b|a-\s*b|ab|a b)\s*testing\b", line, flags=re.I):
                if re.fullmatch(r"[ab]\s*testing", it, flags=re.I):
                    it = "A/B testing"
            if not _is_toolish(it):
                continue
            norm = normalize_text(it)
            if not norm or norm in known_norm:
                continue
            # If token already embeds a known alias (e.g., contains "SQL", "Tableau", etc.), skip unless degree
            if not _is_degree_token(it):
                if scan_line_for_skills(it, scanner):
                    continue
            if _should_ignore_candidate(it):
                continue
            add_candidate(it, line, "unmatched-highfreq", [], int(cnt))

    # Build rows
    rows = []
    for cand, freq in sorted(candidate_counts.items(), key=lambda kv: (-kv[1], kv[0].lower())):
        reasons = ",".join(sorted(candidate_reasons.get(cand, [])))
        cat = "Uncategorized"
        if cand in candidate_cats and candidate_cats[cand]:
            cat = max(candidate_cats[cand].items(), key=lambda x: x[1])[0]
        rows.append((cand, cat, int(freq), reasons, candidate_example.get(cand, "")))
    return rows


def write_outputs(
    output_dir: Path,
    raw_counter: Counter[str],
    canon_counter: Counter[str],
    pairs_sample: List[Tuple[str, str, str, str]],
    unmatched_counter: Counter[str],
    alias_cfg: SkillAliasConfig,
    top_pairs: int = 2000,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    # Raw frequency table
    raw_df = pd.DataFrame(
        sorted(raw_counter.items(), key=lambda kv: (-kv[1], kv[0])),
        columns=["skill", "count"],
    )
    raw_df.to_csv(output_dir / "skills_raw_freq.csv", index=False)

    # Canonical frequency table
    canon_df = pd.DataFrame(
        sorted(canon_counter.items(), key=lambda kv: (-kv[1], kv[0])),
        columns=["canonical_skill", "count"],
    )
    canon_df.to_csv(output_dir / "skills_canonical_freq.csv", index=False)

    # Sample pairs for audit
    if pairs_sample:
        pairs_df = pd.DataFrame(pairs_sample, columns=["job_id", "section", "raw_skill", "canonical_skill"])
        if top_pairs and top_pairs > 0:
            pairs_df = pairs_df.head(top_pairs)
        pairs_df.to_csv(output_dir / "skills_pairs_sample.csv", index=False)

    # Unmatched lines audit
    if unmatched_counter:
        unmatched_df = pd.DataFrame(
            sorted(unmatched_counter.items(), key=lambda kv: (-kv[1], kv[0])),
            columns=["line", "count"],
        )
        unmatched_df.to_csv(output_dir / "skills_unmatched_lines.csv", index=False)

    # Category aggregation (taxonomy-based)
    canon_to_cat = _load_taxonomy_category_map()
    if canon_to_cat:
        by_cat = _aggregate_by_category(canon_counter, canon_to_cat)
        if by_cat:
            cat_df = pd.DataFrame(
                sorted(by_cat.items(), key=lambda kv: (-kv[1], kv[0])),
                columns=["category", "count"],
            )
            cat_df.to_csv(output_dir / "skills_by_category.csv", index=False)

            # Helpful lookup of each canonical skill with its category and count
            map_rows = [
                (canon, canon_to_cat.get(canon.lower(), "Uncategorized"), count)
                for canon, count in sorted(canon_counter.items(), key=lambda kv: (-kv[1], kv[0]))
            ]
            map_df = pd.DataFrame(map_rows, columns=["canonical_skill", "category", "count"])
            map_df.to_csv(output_dir / "skills_with_category.csv", index=False)

    # Suggestions for taxonomy/alias gaps
    suggestions = _generate_suggestions(pairs_sample, raw_counter, unmatched_counter, alias_cfg, canon_to_cat)
    if suggestions:
        sugg_df = pd.DataFrame(
            suggestions,
            columns=["candidate", "suggested_category", "frequency", "reason", "example_line"],
        )
        sugg_df.to_csv(output_dir / "skills_suggestions.csv", index=False)



def main():
    parser = argparse.ArgumentParser(description="Extract unique skills and frequencies from combined jobs CSV")
    parser.add_argument("--output-dir", type=str, default="data/processed", help="Directory to write outputs")
    parser.add_argument("--aliases", type=str, default="", help="Path to config/skill_aliases.yaml (optional)")
    args = parser.parse_args()

    combined_path = get_combined_file()
    if not combined_path.exists():
        raise SystemExit(f"Combined CSV not found at {combined_path}. Run combine_jobs first.")

    print(f"Loading combined jobs from: {combined_path}")
    df = pd.read_csv(combined_path)

    cfg = load_skill_aliases(Path(args.aliases)) if args.aliases else load_skill_aliases()
    print(f"Loaded aliases: {len(cfg.canonical_map)} canonical labels; {len(cfg.stop_phrases)} stop phrases")

    raw_counter, canon_counter, pairs_sample, unmatched_counter = compute_frequencies(df, cfg)
    print(f"Unique raw skills: {len(raw_counter)} | Unique canonical: {len(canon_counter)}")
    print(f"Unmatched lines (post-filter): {sum(unmatched_counter.values())} unique={len(unmatched_counter)}")

    output_dir = Path(args.output_dir)
    write_outputs(output_dir, raw_counter, canon_counter, pairs_sample, unmatched_counter, cfg)
    print(f"Wrote outputs to: {output_dir}")


if __name__ == "__main__":
    main()
