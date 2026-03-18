from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List

import pandas as pd  # type: ignore

from src.utils.skills_canonicalizer import (
    SkillAliasConfig,
    load_skill_aliases,
    split_html_bullets,
    normalize_text,
    build_alias_index,
    prepare_alias_scanner,
    scan_line_for_skills,
)
from src.utils.skills_fuzzy_merge import compute_fuzzy_candidates, CandidateMerge


def _load_freq_from_raw_csv(path: Path) -> Dict[str, int]:
    df = pd.read_csv(path)
    # Expect columns: skill, count (per extract_skills.py outputs)
    if not {"skill", "count"}.issubset(df.columns):
        raise ValueError(f"Unexpected columns in {path}: {df.columns.tolist()}")
    out: Dict[str, int] = {}
    for _, row in df.iterrows():
        s = str(row["skill"]).strip()
        c = int(row["count"]) if pd.notna(row["count"]) else 0
        if s:
            out[s] = out.get(s, 0) + c
    return out


def _load_freq_from_combined(path: Path) -> Dict[str, int]:
    df = pd.read_csv(path)
    # Prefer parsed/normalized fields if present; otherwise use basic/pref raw HTML
    candidates_cols = [
        "basic_qualifications_parsed",
        "preferred_qualifications_parsed",
        "basic_qualifications",
        "preferred_qualifications",
    ]
    cols = [c for c in candidates_cols if c in df.columns]
    if not cols:
        raise ValueError("No qualification columns found in combined CSV")
    ctr: Counter[str] = Counter()
    for _, row in df.iterrows():
        lines: List[str] = []
        for c in cols:
            val = row.get(c)
            if pd.isna(val):
                continue
            if c.endswith("_parsed"):
                # Assume these are already semi-structured lists in string form; split by \n or ; as a heuristic
                text = str(val)
                parts = [p.strip() for p in text.replace(";", "\n").splitlines() if p and p.strip()]
                lines.extend(parts)
            else:
                # HTML-ish fields
                lines.extend(split_html_bullets(str(val)))
        for s in lines:
            ns = s.strip()
            if ns:
                ctr[ns] += 1
    return dict(ctr)


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate fuzzy merge candidates for skills")
    ap.add_argument("--input-raw-csv", type=str, default="data/processed/skills_raw_freq.csv",
                    help="Path to skills_raw_freq.csv produced by extract_skills.py")
    ap.add_argument("--combined", type=str, default="data/processed/combined_jobs.csv",
                    help="Fallback: unified jobs CSV to derive raw skill lines")
    ap.add_argument("--aliases", type=str, default="config/skill_aliases.yaml",
                    help="Path to skill_aliases.yaml")
    ap.add_argument("--min-count", type=int, default=2,
                    help="Minimum frequency of a raw skill to include before fuzzy matching (default: 2)")
    ap.add_argument("--threshold", type=int, default=90, help="RapidFuzz similarity threshold (0-100)")
    ap.add_argument("--method", type=str, default="token_set", choices=["token_set", "token_sort", "qratio"],
                    help="Similarity method")
    ap.add_argument("--block-mode", type=str, default="coarse", choices=["coarse", "first", "first_sig", "none"],
                    help="Blocking strategy: coarse (first token + length bucket), first (first token only), first_sig (first non-stopword token), none (no blocking)")
    ap.add_argument("--include-known", action="store_true",
                    help="Include pairs that already map to the same canonical label (for calibration)")
    ap.add_argument("--strict-skill-like", action="store_true",
                    help="Prefilter to keep only short, skill-like phrases (drops JD boilerplate)")
    ap.add_argument("--enforce-directional", action="store_true",
                    help="Keep merges where variant is rarer and canonical is not longer (rarer->common, shorter/equal canonical)")
    ap.add_argument("--topk-per-variant", type=int, default=1,
                    help="Keep top-K candidates per variant after scoring (default 1)")
    ap.add_argument("--prune-jd-stopwords", action="store_true",
                    help="Before scoring, drop common JD stopwords from phrases to reduce boilerplate similarity")
    ap.add_argument("--skip-both-long", action="store_true",
                    help="Skip comparing pairs where both sides have >4 tokens (reduces long-boilerplate matches)")
    ap.add_argument("--max-pairs-per-block", type=int, default=5000,
                    help="Limit comparisons per block for performance")
    ap.add_argument("--restrict-known-canonicals", action="store_true",
                    help="After scoring, keep only candidates whose canonical exists in aliases canonical_map")
    ap.add_argument("--anchor-to-alias", action="store_true",
                    help="If both phrases contain exactly one identical known alias, anchor canonical to that alias")
    ap.add_argument("--output", type=str, default="data/processed/skills_fuzzy_candidates.csv",
                    help="Output CSV path for candidate merges")
    args = ap.parse_args()

    raw_csv = Path(args.input_raw_csv)
    combined_csv = Path(args.combined)
    out_csv = Path(args.output)

    cfg: SkillAliasConfig = load_skill_aliases(Path(args.aliases))

    # Build frequency map
    if raw_csv.exists():
        freq = _load_freq_from_raw_csv(raw_csv)
        source = raw_csv
    else:
        freq = _load_freq_from_combined(combined_csv)
        source = combined_csv

    # Basic cleaning: drop rare skills below threshold to reduce noise
    cutoff = max(1, int(args.min_count))
    freq = {k: v for k, v in freq.items() if v >= cutoff}

    # Strict prefilter to keep only skill-like phrases (optional)
    if args.strict_skill_like:
        JD_STARTS = {"experience", "bachelor", "master", "degree", "knowledge", "mba", "publications"}
        # Keep if line embeds known skill aliases to avoid over-pruning
        alias_index = build_alias_index(cfg.canonical_map)
        scanner = prepare_alias_scanner(alias_index)
        def _is_skill_like(s: str) -> bool:
            n = normalize_text(s)
            toks = n.split()
            if not toks:
                return False
            # If contains a known skill alias, accept regardless of leading JD term
            if scan_line_for_skills(s, scanner):
                return True
            if toks[0] in JD_STARTS:
                return False
            # Allow short phrases; if long, allow only if tech-like chars/digits appear
            if len(toks) > 4:
                # contains +, #, or any digit token
                if not any(("+" in t) or ("#" in t) or any(ch.isdigit() for ch in t) for t in toks):
                    return False
            # require at least one token length >= 3
            if not any(len(t) >= 3 for t in toks):
                return False
            return True

        freq = {k: v for k, v in freq.items() if _is_skill_like(k)}

    # Select block function based on mode
    def _block_coarse(s: str) -> str:
        # reuse the default behavior by approximating here as needed
        n = normalize_text(s)
        toks = n.split()
        first = toks[0] if toks else ""
        bucket = min(len(n) // 5, 20)
        return f"{first[:12]}|{bucket}"

    def _block_first(s: str) -> str:
        n = normalize_text(s)
        toks = n.split()
        return toks[0] if toks else ""

    def _block_first_sig(s: str) -> str:
        n = normalize_text(s)
        toks = n.split()
        STOP = {"experience", "with", "in", "of", "and", "or", "the", "to", "for", "a", "an",
                "bachelor", "master", "degree", "knowledge", "mba", "publications", "using", "including", "s"}
        for t in toks:
            if t and t not in STOP:
                return t
        return toks[0] if toks else ""

    def _block_none(_s: str) -> str:
        return "all"

    if args.block_mode == "first":
        block_fn = _block_first
    elif args.block_mode == "first_sig":
        block_fn = _block_first_sig
    elif args.block_mode == "none":
        block_fn = _block_none
    else:
        block_fn = _block_coarse

    # JD stopwords used when pruning before scoring
    JD_STOPWORDS = {"experience", "with", "in", "of", "and", "or", "the", "to", "for", "a", "an",
                    "bachelor", "master", "degree", "knowledge", "mba", "publications", "using", "including", "s"}

    candidates: List[CandidateMerge] = compute_fuzzy_candidates(
        freq,
        cfg,
        block_fn=block_fn,
        threshold=args.threshold,
        method=args.method,
        max_pairs_per_block=args.max_pairs_per_block,
        skip_known=(not args.include_known),
        prune_stopwords=args.prune_jd_stopwords,
        jd_stopwords=JD_STOPWORDS,
        skip_both_long=args.skip_both_long,
        long_token_thresh=4,
        anchor_to_alias=args.anchor_to_alias,
    )

    # Prepare alias tools and canonical counts for post-filter remapping
    alias_index_all = build_alias_index(cfg.canonical_map)
    scanner_all = prepare_alias_scanner(alias_index_all)
    canon_counts_by_scan: Dict[str, int] = {}
    for s, cnt in freq.items():
        canons = scan_line_for_skills(s, scanner_all)
        for canon in canons:
            canon_counts_by_scan[canon] = canon_counts_by_scan.get(canon, 0) + cnt

    # Post-filters
    if args.enforce_directional:
        def _dir_ok(cm: CandidateMerge) -> bool:
            vn = normalize_text(cm.variant)
            cn = normalize_text(cm.candidate_canonical)
            return (cm.variant_count <= cm.canonical_count) and (len(cn) <= len(vn))
        candidates = [cm for cm in candidates if _dir_ok(cm)]

    if args.restrict_known_canonicals:
        known_canons = {normalize_text(k) for k in cfg.canonical_map.keys()}
        remapped: List[CandidateMerge] = []
        for cm in candidates:
            cand_norm = normalize_text(cm.candidate_canonical)
            if cand_norm in known_canons:
                remapped.append(cm)
                continue
            # Attempt to remap candidate canonical via alias scan of the phrase
            aliases = scan_line_for_skills(cm.candidate_canonical, scanner_all)
            uniq = list({*aliases})
            if len(uniq) == 1 and normalize_text(uniq[0]) in known_canons:
                new_canon = uniq[0]
                remapped.append(
                    CandidateMerge(
                        variant=cm.variant,
                        candidate_canonical=new_canon,
                        score=cm.score,
                        block=cm.block,
                        method=cm.method,
                        variant_count=cm.variant_count,
                        canonical_count=canon_counts_by_scan.get(new_canon, cm.canonical_count),
                        reason=f"{cm.reason}|alias_remap",
                    )
                )
        candidates = remapped

    # Keep top-K per variant
    K = max(1, int(args.topk_per_variant))
    if K > 0:
        by_variant: Dict[str, List[CandidateMerge]] = {}
        for cm in candidates:
            by_variant.setdefault(cm.variant, []).append(cm)
        pruned: List[CandidateMerge] = []
        for var, lst in by_variant.items():
            lst_sorted = sorted(lst, key=lambda x: (-x.score, -x.canonical_count, x.candidate_canonical))
            pruned.extend(lst_sorted[:K])
        candidates = pruned

    # Write output CSV
    import csv
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "variant",
            "candidate_canonical",
            "score",
            "block",
            "method",
            "variant_count",
            "canonical_count",
            "reason",
        ])
        for cm in candidates:
            w.writerow([
                cm.variant,
                cm.candidate_canonical,
                int(round(cm.score)),
                cm.block,
                cm.method,
                cm.variant_count,
                cm.canonical_count,
                cm.reason,
            ])

    print(
        f"Loaded {len(freq)} unique raw skills from {source}.\n"
        f"Emitted {len(candidates)} candidate merges to: {out_csv}"
    )


if __name__ == "__main__":
    main()
