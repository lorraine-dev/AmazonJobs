import re
from typing import Dict, List, Tuple
from bs4 import BeautifulSoup

# Headings and synonyms (case-insensitive)
SECTION_ALIASES = {
    "about": [
        r"about( the (role|company|team))?",
        r"about us",
        r"who we are",
        r"our story",
        r"why join us",
        r"why should you join us",
        r"company overview",
        r"what we do",
        r"our values",
        r"working with us",
        r"engineering culture",
        r"our mission",
        r"summary",
        r"overview",
    ],
    "responsibilities": [
        r"(key )?responsibilit(?:y|ies)",
        r'what you(?:\'|")?ll do',
        r"what you will do",
        r"what you will be doing",
        r"what you do",
        r"duties",
        r"your impact",
        r"missions",
        r"day[- ]?to[- ]?day",
        r"role(?: and)? responsibilities",
        r"role overview",
        r"your responsibilities",
        r"tasks",
        r"accountabilit(?:y|ies)",
        r"your mission",
        r"scope of (?:work|role)",
        r"what you(?:'|’)ll work on",
        r"what you(?:'|’)ll own",
        r"main tasks",
        r"deliverables",
    ],
    "basic": [
        r"(required|basic|minimum) qualif(?:ication|ications)",
        r"requirements",
        r"must[- ]have",
        r"prereq(?:uisite)?s?",
        r"required skills",
        r"minimum requirements",
        r"you (?:have|bring)",
        r"you must have",
        r"what you bring to the table",
        r"who you are",
        r"about you",
        r"profile",
        r"skills and experience",
        r"what you will bring",
        r"what you bring",
        r"what we(?:'|’)re looking for",
        r"we look for",
        r"we are looking for",
        r"essential skills",
        r"mandatory skills",
        r"your background",
    ],
    "preferred": [
        r"preferred qualif(?:ication|ications)",
        r"additional desired qualifications",
        r"nice[- ]to[- ]have",
        r"nice[- ]to[- ]haves?",
        r"bonus",
        r"plus",
        r"good[- ]to[- ]have",
        r"would be a plus",
        r"is a plus",
        r"strongly preferred",
        r"preferred skills",
        r"ideally",
        r"preferred experience",
        r"bonus skills",
        r"desirable",
        r"pluses",
        r"it(?:'|’)s a plus if",
        r"additional qualifications",
        r"preferred but not required",
    ],
    "benefits": [
        r"benefits",
        r"perks",
        r"what we offer",
        r"what you can expect",
        r"compensation and benefits",
        r"we offer",
        r"our offer",
        r"why you(?:'|’)ll love",
        r"why you(?:'|’)ll love working here",
        r"why you will love",
        r"perks and benefits",
        r"compensation",
        r"compensation \& perks",
        r"what we provide",
        r"what (?:you|we) get",
        r"what you(?:'|’)ll get",
        r"what you will get",
        r"employee benefits",
        r"what(?:'|’)s in it for you",
        r"rewards",
    ],
}

HEADING_RE = re.compile(r"^\s{0,3}(?:#+\s*|<h[1-6][^>]*>)(.+?)\s*$", re.IGNORECASE)
BULLET_RE = re.compile(r"^\s*(?:[-*•]|\d+[.)])\s+(.+?)\s*$")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _strip_html_markdown(text: str) -> str:
    if not text:
        return ""
    # HTML -> text
    try:
        soup = BeautifulSoup(text, "html.parser")
        # Preserve line breaks for block elements
        for br in soup.find_all(
            ["br", "p", "li", "ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6"]
        ):
            br.insert_before("\n")
        text = soup.get_text()
    except Exception:
        pass
    # Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Remove code fences and URLs (reduce noise)
    text = re.sub(r"```[\s\S]*?```", " ", text)
    text = re.sub(r"`[^`]+`", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    # Collapse spaces/tabs but preserve newlines for heading/bullet parsing
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    # Reduce excessive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Keep common inline bullets also as newlines for robustness
    text = text.replace(" • ", "\n• ").replace(" - ", "\n- ")
    return text.strip()


def _is_heading_label(text: str) -> bool:
    t = text.strip().strip(":").lower()
    # Short labels are likely section headers
    if len(t.split()) <= 6:
        for name, rx in SECTION_MATCHERS.items():
            if re.fullmatch(rx, t):
                return True
        # Common canonical labels
        canon = {
            "about",
            "about the role",
            "about the company",
            "about us",
            "responsibilities",
            "key responsibilities",
            "required qualifications",
            "basic qualifications",
            "minimum qualifications",
            "preferred qualifications",
            "benefits",
            "perks",
        }
        if t in canon:
            return True
    return False


def _match_section(name: str) -> re.Pattern:
    aliases = SECTION_ALIASES[name]
    pat = r"|".join([f"(?:{a})" for a in aliases])
    return re.compile(pat, re.IGNORECASE)


SECTION_MATCHERS = {k: _match_section(k) for k in SECTION_ALIASES.keys()}


def _segment_by_headings(lines: List[str]) -> List[Tuple[str, List[str]]]:
    sections: List[Tuple[str, List[str]]] = []
    current_title = ""
    current_buf: List[str] = []

    def flush():
        nonlocal current_title, current_buf
        if current_title or current_buf:
            sections.append(
                (current_title.strip(), [line for line in current_buf if line.strip()])
            )
        current_title = ""
        current_buf = []

    allcaps_like = re.compile(r"^[A-Z0-9][A-Z0-9 &/,-]{2,80}:?$")
    mixedcase_colon = re.compile(r"^[A-Za-z0-9][^\n]{1,80}:$")

    for raw in lines:
        line = raw.strip()
        if not line:
            if current_buf and current_buf[-1] != "":
                current_buf.append("")
            continue
        # Markdown (#), ALL-CAPS colon, or MixedCase ending with ':'
        if (
            line.startswith("#")
            or allcaps_like.match(line)
            or mixedcase_colon.match(line)
        ):
            m = HEADING_RE.match(line)
            if m:
                flush()
                current_title = m.group(1)
                continue
            # If colon style matched, strip trailing ':' as title
            if mixedcase_colon.match(line):
                flush()
                current_title = line[:-1]
                continue
        current_buf.append(raw)

    flush()
    return sections


def _classify_title(title: str) -> str:
    t = title.lower().strip()
    for name, rx in SECTION_MATCHERS.items():
        if rx.search(t):
            return name
    return ""


def _extract_bullets(block_lines: List[str]) -> List[str]:
    items: List[str] = []
    for ln in block_lines:
        m = BULLET_RE.match(ln)
        if m:
            cand = m.group(1).strip()
            if not _is_heading_label(cand):
                items.append(cand)
    # Fallback: split sentences if no bullets found
    if not items:
        blob = " ".join(block_lines).strip()
        # Light sentence split
        parts = re.split(r"(?<=[.!?])\s+", blob)
        items = [p.strip() for p in parts if len(p.strip()) > 0]
    # Deduplicate while preserving order
    seen = set()
    out: List[str] = []
    for it in items:
        k = it.strip()
        if k and k not in seen and not _is_heading_label(k):
            seen.add(k)
            out.append(k)
    return out


def _fallback_classify(lines: List[str]) -> Dict[str, List[str]]:
    responsibilities: List[str] = []
    basic: List[str] = []
    preferred: List[str] = []
    benefits: List[str] = []

    verb_terms = [
        r"design",
        r"architect",
        r"build",
        r"develop",
        r"implement",
        r"integrate",
        r"deliver",
        r"execute",
        r"coordinate",
        r"support",
        r"communicat(e|e with)",
        r"research",
        r"own",
        r"lead",
        r"drive",
        r"collaborate",
        r"partner",
        r"work",
        r"plan",
        r"analyz(e|e)",
        r"optimi[sz]e",
        r"maintain",
        r"ensure",
        r"mentor",
        r"manage",
        r"monitor",
        r"test",
        r"troubleshoot",
        r"document",
        r"review",
    ]
    verb_leads = re.compile(r"^(" + "|".join(verb_terms) + r")\b", re.IGNORECASE)

    basic_terms = [
        r"required",
        r"must",
        r"minimum",
        r"bachelor",
        r"degree",
        r"years of experience",
        r"authorization",
        r"proficient",
        r"expertise",
        r"strong",
        r"essential",
        r"mandatory",
        r"background",
        r"you must have",
        r"what we(?:'|’)re looking for",
    ]
    basic_kw = re.compile(r"\b(" + "|".join(basic_terms) + r")\b", re.IGNORECASE)

    pref_terms = [
        r"preferred",
        r"nice to have",
        r"good to have",
        r"plus",
        r"pluses",
        r"desirable",
        r"it(?:'|’)s a plus if",
        r"preferred but not required",
        r"familiarity",
        r"experience with",
        r"bonus skills?",
    ]
    pref_kw = re.compile(r"\b(" + "|".join(pref_terms) + r")\b", re.IGNORECASE)
    ben_kw = re.compile(
        r"\b(benefits|perks|insurance|vacation|holiday|bonus|equity|stock|401k|pension|remote|flexible)\b",
        re.IGNORECASE,
    )

    for ln in lines:
        m = BULLET_RE.match(ln)
        cand = m.group(1).strip() if m else ln.strip()
        if not cand:
            continue
        if ben_kw.search(cand):
            benefits.append(cand)
        elif pref_kw.search(cand):
            preferred.append(cand)
        elif basic_kw.search(cand):
            basic.append(cand)
        elif verb_leads.search(cand):
            responsibilities.append(cand)

    return {
        "responsibilities": responsibilities,
        "basic": basic,
        "preferred": preferred,
        "benefits": benefits,
    }


def parse_job_description(text: str) -> Dict[str, object]:
    """Parse a free-form job description into structured sections.

    Returns keys: about (str), responsibilities (list[str]),
    basic_qualifications (list[str]), preferred_qualifications (list[str]), benefits (list[str]).
    """
    cleaned = _strip_html_markdown(text or "")
    if not cleaned:
        return {
            "about": "",
            "responsibilities": [],
            "basic_qualifications": [],
            "preferred_qualifications": [],
            "benefits": [],
        }

    # Re-split into lines to preserve potential bullets we reintroduced
    lines = [line for line in cleaned.split("\n") if line is not None]
    sections = _segment_by_headings(lines)

    out_about: List[str] = []
    out_responsibilities: List[str] = []
    out_basic: List[str] = []
    out_preferred: List[str] = []
    out_benefits: List[str] = []

    found_any = False
    for title, block in sections:
        label = _classify_title(title) if title else ""
        if not label:
            continue
        found_any = True
        items = _extract_bullets(block)
        if label == "about":
            # Use full paragraph for about
            out_about.append(" ".join(block).strip())
        elif label == "responsibilities":
            out_responsibilities.extend(items)
        elif label == "basic":
            out_basic.extend(items)
        elif label == "preferred":
            out_preferred.extend(items)
        elif label == "benefits":
            out_benefits.extend(items)

    if not found_any:
        # Fallback: classify bullets/sentences without headings
        fb = _fallback_classify(lines)
        out_responsibilities = fb["responsibilities"]
        out_basic = fb["basic"]
        out_preferred = fb["preferred"]
        out_benefits = fb["benefits"]
        # About = everything not classified
        classified = set(
            out_responsibilities + out_basic + out_preferred + out_benefits
        )
        remaining = []
        for ln in lines:
            m = BULLET_RE.match(ln)
            cand = m.group(1).strip() if m else ln.strip()
            if cand and cand not in classified:
                remaining.append(cand)
        if remaining:
            out_about.append(" ".join(remaining[:3]))

    # Deduplicate and trim
    def uniq(xs: List[str]) -> List[str]:
        seen = set()
        res = []
        for x in [x.strip() for x in xs if x and x.strip()]:
            if x not in seen:
                seen.add(x)
                res.append(x)
        return res

    return {
        "about": " ".join([s for s in out_about if s]).strip(),
        "responsibilities": uniq(out_responsibilities),
        "basic_qualifications": uniq(out_basic),
        "preferred_qualifications": uniq(out_preferred),
        "benefits": uniq(out_benefits),
    }
