from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd  # type: ignore
import torch  # type: ignore
from sentence_transformers import SentenceTransformer, util  # type: ignore
import time

from src.utils.skills_canonicalizer import (
    SkillAliasConfig,
    load_skill_aliases,
    normalize_text,
    build_alias_index,
    prepare_alias_scanner,
    scan_line_for_skills,
)


@dataclass
class Suggestion:
    raw_skill: str
    raw_count: int
    suggested_canonical: str
    score: float
    model: str
    canonical_count_by_scan: int


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
    from collections import Counter

    ctr: Counter[str] = Counter()
    for _, row in df.iterrows():
        lines: List[str] = []
        for c in cols:
            val = row.get(c)
            if pd.isna(val):
                continue
            if c.endswith("_parsed"):
                text = str(val)
                parts = [p.strip() for p in text.replace(";", "\n").splitlines() if p and p.strip()]
                lines.extend(parts)
            else:
                from src.utils.skills_canonicalizer import split_html_bullets

                lines.extend(split_html_bullets(str(val)))
        for s in lines:
            ns = s.strip()
            if ns:
                ctr[ns] += 1
    return dict(ctr)


def _select_model(model_key: str) -> Tuple[str, str]:
    key = model_key.strip().lower()
    mapping = {
        "minilm": "sentence-transformers/all-MiniLM-L6-v2",
        "bge-small": "BAAI/bge-small-en-v1.5",
        "e5-small": "intfloat/e5-small-v2",
        "gte-small": "thenlper/gte-small",
        "bge-base": "BAAI/bge-base-en-v1.5",
        "minilm-l12": "sentence-transformers/all-MiniLM-L12-v2",
        "e5-base": "intfloat/e5-base-v2",
    }
    if key not in mapping:
        raise ValueError(f"Unknown model '{model_key}'. Choices: {sorted(mapping.keys())}")
    return key, mapping[key]


def _device() -> str:
    if torch.cuda.is_available():
        return "cuda"
    # macOS M1/M2 GPUs via Metal
    if torch.backends.mps.is_available():  # type: ignore[attr-defined]
        return "mps"
    return "cpu"


def _build_canonical_texts(cfg: SkillAliasConfig, use_alias_centroids: bool) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for canon, aliases in cfg.canonical_map.items():
        texts: List[str] = []
        # Always include the canonical label itself
        texts.append(canon)
        if use_alias_centroids:
            # Include aliases as additional signals
            for a in aliases or []:
                if a and a not in texts:
                    texts.append(a)
        out[canon] = texts
    return out


def _encode_texts(model: SentenceTransformer, texts: List[str], batch_size: int = 64) -> torch.Tensor:
    # We keep punctuation like +/# via normalize_text if needed, but embeddings often prefer original
    # Use original strings; fallback to normalized if empty
    to_encode = [t if t and t.strip() else "" for t in texts]
    emb = model.encode(to_encode, convert_to_tensor=True, batch_size=batch_size, show_progress_bar=True)
    # Explicit L2-normalization for compatibility across sentence-transformers versions
    emb = torch.nn.functional.normalize(emb, p=2, dim=1)
    return emb  # shape: [N, D], L2-normalized


def _mean_pooling(vectors: Sequence[torch.Tensor]) -> torch.Tensor:
    if not vectors:
        raise ValueError("No vectors to pool")
    stacked = torch.stack(list(vectors), dim=0)
    return torch.mean(stacked, dim=0)


def _compute_canonical_embeddings(model: SentenceTransformer, canon_texts: Mapping[str, List[str]], batch_size: int = 64) -> Tuple[List[str], torch.Tensor]:
    names: List[str] = []
    vecs: List[torch.Tensor] = []
    for canon, texts in canon_texts.items():
        names.append(canon)
        emb = _encode_texts(model, texts, batch_size=batch_size)
        # average across texts and renormalize to unit length
        pooled = _mean_pooling([emb[i] for i in range(emb.size(0))])
        pooled = torch.nn.functional.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
        vecs.append(pooled)
    mat = torch.stack(vecs, dim=0)  # [C, D]
    return names, mat


def _filter_skill_like(
    phrases: Iterable[Tuple[str, int]],
    cfg: SkillAliasConfig,
    strict: bool = False,
) -> Dict[str, int]:
    if not strict:
        return {k: v for k, v in phrases}
    JD_STARTS = {"experience", "bachelor", "master", "degree", "knowledge", "mba", "publications"}
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
            if not any(("+" in t) or ("#" in t) or any(ch.isdigit() for ch in t) for t in toks):
                return False
        if not any(len(t) >= 3 for t in toks):
            return False
        return True

    return {k: v for k, v in phrases if _is_skill_like(k)}


def _build_canonical_scan_counts(freq: Mapping[str, int], cfg: SkillAliasConfig) -> Dict[str, int]:
    alias_index_all = build_alias_index(cfg.canonical_map)
    scanner_all = prepare_alias_scanner(alias_index_all)
    canon_counts_by_scan: Dict[str, int] = {}
    for s, cnt in freq.items():
        canons = scan_line_for_skills(s, scanner_all)
        for canon in canons:
            canon_counts_by_scan[canon] = canon_counts_by_scan.get(canon, 0) + cnt
    return canon_counts_by_scan


def generate_suggestions(
    freq: Mapping[str, int],
    cfg: SkillAliasConfig,
    model: SentenceTransformer,
    canon_names: List[str],
    canon_emb: torch.Tensor,
    topk: int = 3,
    threshold: float = 0.6,
    exclude_known_alias_hits: bool = True,
    benchmark: Optional[Dict[str, float]] = None,
    batch_size: int = 64,
) -> List[Suggestion]:
    alias_index = build_alias_index(cfg.canonical_map)
    scanner = prepare_alias_scanner(alias_index)

    # Optionally exclude lines that already hit a known alias in-text
    items: List[Tuple[str, int]] = []
    for s, c in freq.items():
        if exclude_known_alias_hits and scan_line_for_skills(s, scanner):
            continue
        items.append((s, c))

    raw_phrases = [s for s, _ in items]
    raw_counts = [c for _, c in items]

    if not raw_phrases:
        return []

    _t0 = time.perf_counter()
    raw_emb = _encode_texts(model, raw_phrases, batch_size=batch_size)
    _t1 = time.perf_counter()
    if benchmark is not None:
        benchmark["encode_raw_sec"] = _t1 - _t0
        benchmark["raw_count"] = float(len(raw_phrases))
    # cosine similarity since both sides are normalized
    sims = util.cos_sim(raw_emb, canon_emb)  # [R, C]
    _t2 = time.perf_counter()
    if benchmark is not None:
        benchmark["similarity_sec"] = _t2 - _t1

    _t3a = time.perf_counter()
    canon_scan_counts = _build_canonical_scan_counts(freq, cfg)
    _t3b = time.perf_counter()
    if benchmark is not None:
        benchmark["scan_counts_sec"] = _t3b - _t2

    suggestions: List[Suggestion] = []
    for i in range(sims.size(0)):
        scores = sims[i]
        topk_val, topk_idx = torch.topk(scores, k=min(topk, scores.size(0)))
        for j in range(topk_val.size(0)):
            score = float(topk_val[j].item())
            if score < threshold:
                continue
            canon = canon_names[int(topk_idx[j].item())]
            suggestions.append(
                Suggestion(
                    raw_skill=raw_phrases[i],
                    raw_count=int(raw_counts[i]),
                    suggested_canonical=canon,
                    score=score,
                    model=str(model),
                    canonical_count_by_scan=int(canon_scan_counts.get(canon, 0)),
                )
            )
    # Sort: descending score, then descending canonical_count, then raw_count desc, then names
    suggestions.sort(key=lambda s: (-s.score, -s.canonical_count_by_scan, -s.raw_count, s.suggested_canonical, s.raw_skill))
    if benchmark is not None:
        benchmark["postproc_sec"] = time.perf_counter() - _t3b
    return suggestions


def _eval_against_known_alias(
    freq: Mapping[str, int],
    cfg: SkillAliasConfig,
    model: SentenceTransformer,
    canon_names: List[str],
    canon_emb: torch.Tensor,
    threshold: float,
    batch_size: int = 64,
) -> None:
    alias_index = build_alias_index(cfg.canonical_map)
    scanner = prepare_alias_scanner(alias_index)

    # Keep only lines with exactly one canonical alias hit
    eval_items: List[Tuple[str, str]] = []
    for s, _cnt in freq.items():
        canons = scan_line_for_skills(s, scanner)
        uniq = list({*canons})
        if len(uniq) == 1:
            eval_items.append((s, uniq[0]))

    if not eval_items:
        print("[eval] No lines with exactly one alias hit; skipping eval.")
        return

    raw_phrases = [s for s, _ in eval_items]
    gold = [c for _, c in eval_items]

    raw_emb = _encode_texts(model, raw_phrases)
    sims = util.cos_sim(raw_emb, canon_emb)  # [R, C]

    top1, top3 = 0, 0
    total = len(raw_phrases)
    for i in range(total):
        scores = sims[i]
        vals, idxs = torch.topk(scores, k=min(3, scores.size(0)))
        top1_hit = (vals[0].item() >= threshold) and (canon_names[int(idxs[0])] == gold[i])
        top3_hit = any((vals[j].item() >= threshold) and (canon_names[int(idxs[j])] == gold[i]) for j in range(len(vals)))
        top1 += 1 if top1_hit else 0
        top3 += 1 if top3_hit else 0

    print(f"[eval] Evaluated {total} lines with single known alias. Top-1@{threshold:.2f}: {top1/total:.3f}, Top-3@{threshold:.2f}: {top3/total:.3f}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Suggest semantic alias candidates using sentence embeddings (offline)")
    ap.add_argument("--input-raw-csv", type=str, default="data/processed/skills_raw_freq.csv",
                    help="Path to skills_raw_freq.csv produced by extract_skills.py")
    ap.add_argument("--combined", type=str, default="data/processed/combined_jobs.csv",
                    help="Fallback: unified jobs CSV to derive raw skill lines")
    ap.add_argument("--aliases", type=str, default="config/skill_aliases.yaml",
                    help="Path to skill_aliases.yaml")
    ap.add_argument("--model", type=str, default="minilm",
                    choices=[
                        "minilm",
                        "minilm-l12",
                        "bge-small",
                        "bge-base",
                        "e5-small",
                        "e5-base",
                        "gte-small",
                    ],
                    help="Embedding model to use")
    ap.add_argument("--topk", type=int, default=3, help="Top-K canonicals to emit per raw skill")
    ap.add_argument("--threshold", type=float, default=0.60, help="Cosine similarity threshold (0..1)")
    ap.add_argument("--min-count", type=int, default=2, help="Min frequency of a raw skill to include")
    ap.add_argument("--limit-raw", type=int, default=0, help="Limit number of raw phrases for a quick run (0 = no limit)")
    ap.add_argument("--strict-skill-like", action="store_true", help="Drop boilerplate JD lines before embedding")
    ap.add_argument("--use-alias-centroids", action="store_true", help="Average embeddings of canonical label + its aliases")
    ap.add_argument("--include-known-alias-hits", action="store_true", help="Include lines that already match a known alias")
    ap.add_argument("--eval-against-alias", action="store_true", help="Evaluate on lines with a single known alias hit (does not affect output)")
    ap.add_argument("--output", type=str, default="data/processed/semantic_alias_suggestions.csv",
                    help="Output CSV path for suggestions")
    ap.add_argument("--benchmark", action="store_true", help="Print per-stage timing and throughput summary")
    ap.add_argument("--batch-size", type=int, default=48, help="Batch size for model.encode; lower for larger models or limited VRAM/MPS")
    ap.add_argument("--cache-dir", type=str, default="", help="Local cache directory for HF models (defaults to global HF cache if empty)")
    args = ap.parse_args()

    total_t0 = time.perf_counter()
    raw_csv = Path(args.input_raw_csv)
    combined_csv = Path(args.combined)
    out_csv = Path(args.output)

    cfg: SkillAliasConfig = load_skill_aliases(Path(args.aliases))

    # Build frequency map
    load_t0 = time.perf_counter()
    if raw_csv.exists():
        freq = _load_freq_from_raw_csv(raw_csv)
        source = raw_csv
    else:
        freq = _load_freq_from_combined(combined_csv)
        source = combined_csv
    n_loaded = len(freq)

    # Basic cleaning: drop rare skills below threshold
    cutoff = max(1, int(args.min_count))
    freq = {k: v for k, v in freq.items() if v >= cutoff}
    n_after_min = len(freq)

    # Optional stricter prefilter for skill-like
    if args.strict_skill_like:
        freq = _filter_skill_like(freq.items(), cfg, strict=True)
    n_after_strict = len(freq)

    # Optional limit for quick experiments
    if args.limit_raw and args.limit_raw > 0:
        # Keep top by frequency
        freq = dict(sorted(freq.items(), key=lambda kv: -kv[1])[: int(args.limit_raw)])
    n_after_limit = len(freq)
    load_t1 = time.perf_counter()

    # Prepare model and canonical embeddings
    key, hf_name = _select_model(args.model)
    device = _device()
    cache_dir = args.cache_dir.strip()
    if cache_dir:
        cache_path = Path(cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        model = SentenceTransformer(hf_name, device=device, cache_folder=str(cache_path))
    else:
        model = SentenceTransformer(hf_name, device=device)
    canon_texts = _build_canonical_texts(cfg, use_alias_centroids=args.use_alias_centroids)
    total_canon_texts = sum(len(v) for v in canon_texts.values())
    canon_t0 = time.perf_counter()
    canon_names, canon_emb = _compute_canonical_embeddings(model, canon_texts, batch_size=args.batch_size)
    canon_t1 = time.perf_counter()

    # Generate suggestions
    bench: Optional[Dict[str, float]] = {} if args.benchmark else None
    suggestions = generate_suggestions(
        freq,
        cfg,
        model,
        canon_names,
        canon_emb,
        topk=max(1, int(args.topk)),
        threshold=float(args.threshold),
        exclude_known_alias_hits=(not args.include_known_alias_hits),
        benchmark=bench,
        batch_size=args.batch_size,
    )

    # Optional evaluation
    if args.eval_against_alias:
        _eval_against_known_alias(freq, cfg, model, canon_names, canon_emb, float(args.threshold), batch_size=args.batch_size)

    # Write output CSV
    import csv

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "raw_skill",
            "raw_count",
            "suggested_canonical",
            "score",
            "model",
            "canonical_count_by_scan",
        ])
        for s in suggestions:
            w.writerow([
                s.raw_skill,
                s.raw_count,
                s.suggested_canonical,
                f"{s.score:.4f}",
                key,
                s.canonical_count_by_scan,
            ])

    print(
        f"[semantic] Loaded {len(freq)} unique raw skills from {source}.\n"
        f"[semantic] Emitted {len(suggestions)} suggestions to: {out_csv}\n"
        f"[semantic] Model: {key} ({hf_name}) on device {device}."
    )

    if args.benchmark:
        # Compute device detail
        dev_detail = ""
        if device == "cuda" and torch.cuda.is_available():
            try:
                dev_detail = torch.cuda.get_device_name(0)
            except Exception:
                dev_detail = "CUDA GPU"
        elif device == "mps":
            dev_detail = "Apple Metal (MPS)"
        else:
            dev_detail = "CPU"

        total_sec = time.perf_counter() - total_t0
        load_sec = load_t1 - load_t0
        canon_sec = canon_t1 - canon_t0
        canon_rate = (float(total_canon_texts) / canon_sec) if canon_sec > 0 else 0.0
        raw_sec = float(bench.get("encode_raw_sec", 0.0)) if bench else 0.0
        raw_count = int(bench.get("raw_count", 0.0)) if bench else 0
        raw_rate = (raw_count / raw_sec) if raw_sec > 0 else 0.0
        sim_sec = float(bench.get("similarity_sec", 0.0)) if bench else 0.0

        # Optional: intermediate times
        scan_counts_sec = float(bench.get("scan_counts_sec", 0.0)) if bench else 0.0
        postproc_sec = float(bench.get("postproc_sec", 0.0)) if bench else 0.0

        print(
            f"[semantic] Device detail: {device} :: {dev_detail}\n"
            f"[semantic] Timings (s): load={load_sec:.3f}, canon_encode={canon_sec:.3f} (texts={total_canon_texts}, {canon_rate:.1f}/s), "
            f"raw_encode={raw_sec:.3f} (rows={raw_count}, {raw_rate:.1f}/s), sim={sim_sec:.3f}, scan_counts={scan_counts_sec:.3f}, postproc={postproc_sec:.3f}, total={total_sec:.3f}"
        )


if __name__ == "__main__":
    main()
