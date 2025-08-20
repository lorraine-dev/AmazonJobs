from typing import List
import re

from langdetect import detect_langs, DetectorFactory

# Ensure deterministic predictions
DetectorFactory.seed = 0


def _clean_text(text: str) -> str:
    t = (text or "").strip()
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t)
    return t


def is_english_text(text: str, min_chars: int = 25, threshold: float = 0.90) -> bool:
    """Heuristically determine if text is English.

    - Uses langdetect's probability scores via detect_langs
    - Short texts (< min_chars) are judged with a lower threshold and default to True on error
    - Returns True on detection errors to avoid dropping valid rows due to parser issues
    """
    t = _clean_text(text)
    if not t:
        return True

    try:
        langs = detect_langs(t)
    except Exception:
        # For very short or noisy strings, be permissive
        return True

    # Pick dynamic threshold for short texts
    eff_threshold = 0.60 if len(t) < min_chars else threshold
    for lp in langs:
        if lp.lang == "en" and lp.prob >= eff_threshold:
            return True
    return False


essential_fields: List[str] = ["job_title", "title", "description"]


def job_is_english(job: dict) -> bool:
    """Check if a TheirStack job dict is English based on title + description."""
    title = job.get("job_title") or job.get("title") or ""
    desc = job.get("description") or ""
    combined = f"{title}. {desc}".strip()
    return is_english_text(combined)
