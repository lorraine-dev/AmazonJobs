from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple, Callable, Optional, Set
from collections import defaultdict

from rapidfuzz import fuzz  # type: ignore

from .skills_canonicalizer import (
    normalize_text,
    build_alias_index,
    load_skill_aliases,
    SkillAliasConfig,
    prepare_alias_scanner,
    scan_line_for_skills,
)


@dataclass
class CandidateMerge:
    variant: str
    candidate_canonical: str
    score: float
    block: str
    method: str
    variant_count: int
    canonical_count: int
    reason: str


def default_block_key(s: str) -> str:
    """Heuristic block key to reduce pairwise comparisons.

    - normalize
    - take first token (up to 12 chars) and a coarse length bucket
    """
    n = normalize_text(s)
    if not n:
        return ""
    toks = n.split()
    first = toks[0] if toks else ""
    bucket = min(len(n) // 5, 20)
    return f"{first[:12]}|{bucket}"


def _choose_canonical(a: str, b: str, count_a: int, count_b: int, cfg: SkillAliasConfig) -> Tuple[str, str]:
    """Return (variant, canonical) given two similar strings and their counts.

    Preference order:
    - If one is an explicit canonical label in cfg.canonical_map keys, prefer it as canonical
    - Higher frequency becomes canonical
    - Shorter string as canonical
    - Lexicographic fallback
    """
    a_is_canon = normalize_text(a) in {normalize_text(k) for k in cfg.canonical_map.keys()}
    b_is_canon = normalize_text(b) in {normalize_text(k) for k in cfg.canonical_map.keys()}
    if a_is_canon and not b_is_canon:
        return b, a
    if b_is_canon and not a_is_canon:
        return a, b

    if count_a != count_b:
        # Higher count becomes canonical
        return (b, a) if count_a > count_b else (a, b)

    if len(a) != len(b):
        return (b, a) if len(a) < len(b) else (a, b)

    return (b, a) if a < b else (a, b)


def compute_fuzzy_candidates(
    freq_map: Dict[str, int],
    alias_cfg: SkillAliasConfig,
    *,
    block_fn: Callable[[str], str] = default_block_key,
    threshold: int = 90,
    method: str = "token_set",
    max_pairs_per_block: int = 5000,
    skip_known: bool = True,
    prune_stopwords: bool = False,
    jd_stopwords: Optional[Set[str]] = None,
    skip_both_long: bool = False,
    long_token_thresh: int = 4,
    anchor_to_alias: bool = False,
) -> List[CandidateMerge]:
    """Compute fuzzy merge candidates within blocks using RapidFuzz.

    - Only considers pairs with similarity >= threshold
    - Skips pairs already resolved by existing alias index (both map to same canonical)
    - Returns auditable CandidateMerge entries
    """
    items = [(s, c) for s, c in freq_map.items() if s and s.strip()]
    if not items:
        return []

    # Precompute alias mapping to skip pairs that already map to same canonical
    alias_index = build_alias_index(alias_cfg.canonical_map)

    def _maps_to_same_canon(x: str, y: str) -> bool:
        nx, ny = normalize_text(x), normalize_text(y)
        cx = alias_index.get(nx, nx)
        cy = alias_index.get(ny, ny)
        return cx == cy

    # Blocks
    blocks: Dict[str, List[Tuple[str, int]]] = defaultdict(list)
    for s, c in items:
        blocks[block_fn(s)].append((s, c))

    out: List[CandidateMerge] = []
    # Prepare alias scanner if anchoring is desired
    scanner = None
    canon_counts: Dict[str, int] = {}
    if anchor_to_alias:
        scanner = prepare_alias_scanner(alias_index)
        # Precompute canonical counts by scanning each phrase for aliases
        for s, c in items:
            canons = scan_line_for_skills(s, scanner)
            for canon in canons:
                canon_counts[canon] = canon_counts.get(canon, 0) + c

    def _prune(s: str) -> str:
        if not prune_stopwords:
            return s
        toks = normalize_text(s).split()
        sw = jd_stopwords if jd_stopwords is not None else set()
        pruned = [t for t in toks if t not in sw]
        return " ".join(pruned) if pruned else ""

    for block_key, pairs in blocks.items():
        # Small blocks: all-vs-all
        n = len(pairs)
        if n < 2:
            continue
        # Cap comparisons by early-exit if block is huge
        comparisons = 0
        for i in range(n):
            s1, c1 = pairs[i]
            for j in range(i + 1, n):
                if comparisons >= max_pairs_per_block:
                    break
                s2, c2 = pairs[j]
                comparisons += 1
                # Optionally skip pairs where both are long phrases
                if skip_both_long:
                    if len(normalize_text(s1).split()) > long_token_thresh and len(normalize_text(s2).split()) > long_token_thresh:
                        continue
                ps1, ps2 = _prune(s1), _prune(s2)
                # If pruning removed all content on either side, skip
                if not ps1 or not ps2:
                    continue
                if method == "token_set":
                    score = fuzz.token_set_ratio(ps1, ps2)
                elif method == "token_sort":
                    score = fuzz.token_sort_ratio(ps1, ps2)
                else:
                    score = fuzz.QRatio(ps1, ps2)
                if score < threshold:
                    continue
                # Optionally anchor canonical to a known alias detected within either phrase
                anchored_canon: Optional[str] = None
                if scanner is not None:
                    can1 = scan_line_for_skills(s1, scanner)
                    can2 = scan_line_for_skills(s2, scanner)
                    common = list(set(can1).intersection(can2))
                    if len(common) == 1:
                        anchored_canon = common[0]
                # If both map to the same canonical already and there is no anchoring signal,
                # skip this pair when skip_known is enabled. Allow anchored pairs through.
                if skip_known and (anchored_canon is None) and _maps_to_same_canon(s1, s2):
                    continue

                if anchored_canon:
                    # Choose variant as the rarer string
                    if c1 <= c2:
                        variant, canonical = s1, anchored_canon
                        vc, cc = c1, canon_counts.get(anchored_canon, 0)
                    else:
                        variant, canonical = s2, anchored_canon
                        vc, cc = c2, canon_counts.get(anchored_canon, 0)
                else:
                    variant, canonical = _choose_canonical(s1, s2, c1, c2, alias_cfg)
                    vc = freq_map.get(variant, 0)
                    cc = freq_map.get(canonical, 0)
                out.append(
                    CandidateMerge(
                        variant=variant,
                        candidate_canonical=canonical,
                        score=round(score, 0),
                        block=str(block_key),
                        method=method,
                        variant_count=vc,
                        canonical_count=cc,
                        reason=f"{method}:{int(round(score, 0))}>={threshold}",
                    )
                )
            if comparisons >= max_pairs_per_block:
                break

    # Deduplicate: keep best score per (variant, candidate)
    dedup: Dict[Tuple[str, str], CandidateMerge] = {}
    for cm in out:
        k = (cm.variant, cm.candidate_canonical)
        if k not in dedup or cm.score > dedup[k].score:
            dedup[k] = cm

    # Sort for auditability
    final = sorted(
        dedup.values(),
        key=lambda x: (-x.score, -x.canonical_count, x.candidate_canonical, x.variant),
    )
    return final


__all__ = [
    "CandidateMerge",
    "compute_fuzzy_candidates",
    "default_block_key",
]
