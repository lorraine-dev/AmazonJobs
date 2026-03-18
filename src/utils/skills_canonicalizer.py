"""
Utilities for normalizing and canonicalizing skill phrases.

Core pieces:
- load_skill_aliases(): load canonical_map and stop_phrases from YAML
- normalize_text()/normalize_token(): lowercasing, deaccenting, punctuation spacing, whitespace collapse
- build_alias_index(): map normalized alias -> canonical label
- canonicalize_name(): use index to map a raw/normalized skill to canonical
- split_html_bullets(): extract likely skill bullet items from HTML-ish text

These functions are reusable in offline pipelines (unique skill extraction, fuzzy grouping)
and can be used to generate a final raw_skill -> canonical_label mapping JSON.
"""
from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from typing import Pattern as RePattern

import yaml
from bs4 import BeautifulSoup  # type: ignore
from unidecode import unidecode  # type: ignore


DEFAULT_ALIAS_PATH = Path(__file__).resolve().parents[2] / "config" / "skill_aliases.yaml"
DEFAULT_TAXONOMY_PATH = Path(__file__).resolve().parents[2] / "docs" / "skills_taxonomy.json"


@dataclass
class SkillAliasConfig:
    canonical_map: Dict[str, List[str]]
    stop_phrases: List[str]


def load_skill_aliases(path: Optional[Path] = None) -> SkillAliasConfig:
    """Load YAML defining canonical skill labels and aliases.

    Schema:
        canonical_map:
          canonical_label:
            - alias1
            - alias2
        stop_phrases:
            - phrase to ignore
    """
    p = path or DEFAULT_ALIAS_PATH
    yaml_map: Dict[str, List[str]] = {}
    stop_phrases: List[str] = []
    if p.exists():
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        yaml_map = data.get("canonical_map", {}) or {}
        stop_phrases = data.get("stop_phrases", []) or []

    # Merge in taxonomy aliases from JSON if present. YAML takes precedence/overrides.
    tax_map: Dict[str, List[str]] = _load_taxonomy_aliases(DEFAULT_TAXONOMY_PATH)
    merged: Dict[str, List[str]] = dict(tax_map)
    for canon, aliases in (yaml_map or {}).items():
        # Overwrite taxonomy entries with YAML-provided ones
        merged[canon] = list(aliases or [])
    # Normalize keys to lowercase strings defensively
    canon_norm = {str(k).strip().lower(): [str(a).strip() for a in (v or [])] for k, v in merged.items()}
    stop_norm = [str(s).strip().lower() for s in stop_phrases]
    return SkillAliasConfig(canonical_map=canon_norm, stop_phrases=stop_norm)


def _load_taxonomy_aliases(path: Optional[Path]) -> Dict[str, List[str]]:
    """Load skills taxonomy JSON and convert to canonical_map-like structure.

    JSON schema:
        {
          "Category A": [ {"name": "Python", "aliases": ["Py"]}, ...],
          "Category B": [ ... ]
        }

    Returns a dict: { canonical_label: [aliases...] }
    If file is missing/invalid, returns empty dict.
    """
    if not path:
        return {}
    try:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        out: Dict[str, List[str]] = {}
        if isinstance(data, dict):
            for _, items in data.items():
                if not isinstance(items, list):
                    continue
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    name = str(it.get("name", "")).strip()
                    aliases = it.get("aliases", []) or []
                    if not name:
                        continue
                    out.setdefault(name, [])
                    for a in aliases:
                        s = str(a).strip()
                        if s and s not in out[name]:
                            out[name].append(s)
        return out
    except Exception:
        # Be permissive; fallback to empty when JSON is malformed
        return {}


# Characters we deliberately keep for languages like C++ and C#
_KEEP_CHARS = "+#"


def normalize_text(s: Optional[str]) -> str:
    """Robust normalization for skill-like phrases.

    - Convert to string, deaccent, HTML-decode, lowercase
    - Replace most punctuation with spaces but keep + and #
    - Collapse whitespace
    """
    if s is None:
        return ""
    t = str(s)
    t = html.unescape(t)
    t = unidecode(t)
    t = t.lower()
    # Normalize common separators to spaces
    t = re.sub(r"[\u00A0\s\t\r\n]+", " ", t)
    # Replace punctuation except keep-chars with spaces
    t = re.sub(fr"[^a-z0-9{re.escape(_KEEP_CHARS)}]+", " ", t)
    # Collapse multiple spaces
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t


def normalize_token(s: Optional[str]) -> str:
    """Single token normalization, additionally trim trailing punctuation."""
    return normalize_text(s)


def build_alias_index(canonical_map: Mapping[str, Sequence[str]]) -> Dict[str, str]:
    """Build alias -> canonical map using normalized keys.

    Includes identity mappings for canonical labels themselves.
    """
    index: Dict[str, str] = {}
    for canon, aliases in canonical_map.items():
        canon_norm = normalize_token(canon)
        if canon_norm:
            index[canon_norm] = canon  # map to canonical label as given in YAML
        for a in aliases or []:
            a_norm = normalize_token(a)
            if not a_norm:
                continue
            # Prefer first occurrence; later duplicates won't override
            index.setdefault(a_norm, canon)
    return index


def _build_alias_matchers(alias_index: Mapping[str, str]) -> List[Tuple[str, RePattern[str]]]:
    """Compile regex matchers for alias-in-text scanning.

    We operate on normalized text (see normalize_text), so aliases passed in
    are expected to be normalized too (alias_index keys are normalized).

    Boundary definition: characters [a-z0-9+#] are considered part of tokens.
    We match aliases when they are not immediately adjacent to those chars.
    """
    matchers: List[Tuple[str, RePattern[str]]] = []
    boundary = r"[a-z0-9+#]"
    for alias_norm, canon in alias_index.items():
        if not alias_norm:
            continue
        # Escape + and # etc. (even though alias_norm should already be plain)
        body = re.escape(alias_norm)
        pattern = rf"(?<!{boundary}){body}(?!{boundary})"
        matchers.append((canon, re.compile(pattern)))
    return matchers


def _scan_aliases_in_text(text: str, matchers: Sequence[Tuple[str, RePattern[str]]]) -> List[str]:
    """Return a list of canonical labels detected within the input text.

    - Normalizes the input similarly to alias normalization
    - Dedupe results per line
    """
    n = normalize_text(text)
    found: List[str] = []
    seen = set()
    for canon, pattern in matchers:
        if pattern.search(n):
            if canon not in seen:
                seen.add(canon)
                found.append(canon)
    return found


def prepare_alias_scanner(alias_index: Mapping[str, str]) -> List[Tuple[str, RePattern[str]]]:
    """Public helper to build alias matchers for repeated scanning."""
    return _build_alias_matchers(alias_index)


def scan_line_for_skills(text: str, scanner: Sequence[Tuple[str, RePattern[str]]]) -> List[str]:
    """Public helper to scan a single line for canonical skills using a prepared scanner."""
    return _scan_aliases_in_text(text, scanner)


def canonicalize_name(name: str, alias_index: Mapping[str, str]) -> str:
    """Map a raw/normalized skill name to its canonical label if known."""
    n = normalize_token(name)
    return alias_index.get(n, n)


def _strip_bullet_prefix(s: str) -> str:
    # Remove typical bullet markers and trailing punctuation
    s = re.sub(r"^[>\-–—•*·\s\u00A0]+", "", s)
    s = re.sub(r"[\s\.;:,]+$", "", s)
    return s.strip()


_SKIP_PATTERN = re.compile(
    r"(amazon\s+is\s+an\s+equal|privacy\s+notice|inclusive\s+culture|accommodations|recruiting\s+decisions|workforce|protecting\s+your\s+privacy)",
    re.I,
)


def split_html_bullets(html_text: Optional[str]) -> List[str]:
    """Split an HTML-ish blob into individual bullet-like lines similar to the JS logic.

    - HTML decode twice defensively
    - Replace <br>, </li>, </p> with newlines; strip other tags
    - Normalize whitespace; remove obvious disclaimers/URLs/too-long lines
    """
    if not html_text:
        return []
    text = str(html_text)
    # Decode entities twice defensively
    text = html.unescape(html.unescape(text))

    # Use BS4 to strip tags and preserve line breaks for certain tags
    soup = BeautifulSoup(text, "html.parser")
    # Replace <br> and end of <li>/<p> with newlines
    for br in soup.find_all(["br", "br/"]):
        br.replace_with("\n")
    for tag in soup.find_all(["li", "p"]):
        # Append newline after the tag content
        if tag.string is None:
            # Do nothing special; BS4 will handle
            pass
        # Ensure separation
        tag.append("\n")
    # Preserve separation between text nodes as newlines to keep bullets
    text = soup.get_text("\n")

    # Also convert literal encoded <br/> remnants to newline
    text = re.sub(r"\s*&lt;br\s*\/??&gt;\s*", "\n", text, flags=re.I)
    # Normalize line endings and collapse spaces but keep newlines
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]{2,}", " ", text)

    raw_lines = [s.strip() for s in re.split(r"\r?\n+", text) if s and s.strip()]
    out: List[str] = []
    seen = set()
    for line in raw_lines:
        s = _strip_bullet_prefix(line)
        looks_like_url = bool(re.search(r"https?://", s, flags=re.I))
        is_na = bool(re.match(r"^(?:n\s*\/??\s*a|n\.?a\.?|nan)$", s, flags=re.I))
        has_letters = bool(re.search(r"[a-z]", s, flags=re.I))
        if not s or looks_like_url or is_na or not has_letters or len(s) > 180:
            continue
        if _SKIP_PATTERN.search(s):
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def filter_stop_phrases(lines: Iterable[str], stop_phrases: Sequence[str]) -> List[str]:
    """Remove lines that contain any stop phrase (case-insensitive)."""
    stop = [normalize_text(p) for p in (stop_phrases or []) if p]
    if not stop:
        return list(lines)
    out: List[str] = []
    for s in lines:
        ns = normalize_text(s)
        if any(p and p in ns for p in stop):
            continue
        out.append(s)
    return out


def canonicalize_lines(lines: Iterable[str], alias_index: Mapping[str, str], stop_phrases: Sequence[str]) -> Tuple[List[Tuple[str, str]], Dict[str, int]]:
    """Canonicalize input lines and return:
    - pairs: list of (raw_line, canonical_label) for EACH matched alias in the line
    - counts: frequency of canonical labels across all lines

    Behavior:
    - Filters stop phrases first
    - Scans each line for aliases within text (not just exact matches)
    - Dedupe multiple occurrences of the same canonical label within a single line
    - If a line has no alias matches, it is ignored (no fallback to raw)
    """
    filtered = filter_stop_phrases(lines, stop_phrases)
    pairs: List[Tuple[str, str]] = []
    counts: Dict[str, int] = {}
    matchers = _build_alias_matchers(alias_index)
    for raw in filtered:
        matched_canons = _scan_aliases_in_text(raw, matchers)
        if not matched_canons:
            continue  # skip non-skill lines
        for canon in matched_canons:
            pairs.append((raw, canon))
            counts[canon] = counts.get(canon, 0) + 1
    return pairs, counts


def demo_canonicalize_from_html(basic_html: Optional[str], pref_html: Optional[str], cfg: Optional[SkillAliasConfig] = None) -> Dict[str, int]:
    """Convenience helper for quick testing: split both sections and canonicalize."""
    cfg = cfg or load_skill_aliases()
    alias_index = build_alias_index(cfg.canonical_map)
    lines = split_html_bullets(basic_html) + split_html_bullets(pref_html)
    _pairs, counts = canonicalize_lines(lines, alias_index, cfg.stop_phrases)
    return counts


if __name__ == "__main__":
    # Minimal CLI for quick manual checks
    import argparse

    ap = argparse.ArgumentParser(description="Canonicalize skills from text or HTML")
    ap.add_argument("--basic", type=str, default="", help="HTML/text for basic qualifications")
    ap.add_argument("--pref", type=str, default="", help="HTML/text for preferred qualifications")
    ap.add_argument("--aliases", type=str, default=str(DEFAULT_ALIAS_PATH), help="Path to skill_aliases.yaml")
    ap.add_argument("--dump", action="store_true", help="Print canonical counts as JSON")
    args = ap.parse_args()

    cfg = load_skill_aliases(Path(args.aliases))
    counts = demo_canonicalize_from_html(args.basic, args.pref, cfg)
    if args.dump:
        print(json.dumps(counts, indent=2, ensure_ascii=False))
    else:
        for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"{k}: {v}")
