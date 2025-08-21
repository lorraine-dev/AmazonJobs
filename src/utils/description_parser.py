import re
from typing import Dict, List, Tuple
from bs4 import BeautifulSoup

# Headings and synonyms (case-insensitive)
SECTION_ALIASES = {
    "about": [
        r"about( the (role|company|team))?",
        r"about us",
        r"who we are",
        r"who we are and what we do",
        r"who you(?:'|’)ll work with",
        r"who you will work with",
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
        r"the role",
        r"context of the position",
        r"^role$",
        r"how to apply",
        r"our long[- ]term vision",
    ],
    "responsibilities": [
        r"(key )?responsibilit(?:y|ies)",
        r"what you(?:'|’)?ll do",
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
        r"we are counting on you to",
    ],
    "basic": [
        r"(required|basic|minimum) qualif(?:ication|ications)",
        r"qualifications",
        r"requirements",
        r"must[- ]have",
        r"prereq(?:uisite)?s?",
        r"required skills",
        r"minimum requirements",
        r"key knowledge, skills(?: ?(?:&|and) ?)?experience",
        r"knowledge, skills(?: ?(?:&|and) ?)?experience",
        r"personal attributes",
        r"who [^:]{1,50} is for",
        r"who [^:]{1,50} is not for",
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
        r"moreover",
    ],
    "preferred": [
        r"preferred qualif(?:ication|ications)",
        r"bonus",
        r"good to have",
        r"preferences",
        r"ideally",
        r"bonus points",
        r"pluses",
        r"it(?:'|’)s a plus if",
        r"additional qualifications",
        r"preferred but not required",
        r"desired skills and competency areas",
    ],
    "benefits": [
        r"benefits",
        r"perks",
        r"what we offer",
        r"what you can expect",
        r"you can expect",
        r"compensation and benefits",
        r"we offer",
        r"we offer you",
        r"in addition,? we offer",
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
# Support common bullet markers including hyphen, asterisk, bullet, middle dot, en/em dashes, and arrows
# Also handle double-markers like '· - item' by allowing an optional second marker before content
BULLET_RE = re.compile(r"^\s*(?:[-*•·▪►–—]|\d+[.)])(?:\s+[-*•·▪►–—])?\s+(.+?)\s*$")
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
    # Treat standalone bold markdown lines as headings (e.g., **Responsibilities**)
    bold_md = re.compile(r"^\s*\*\*([^*][^*]{0,100}?)\*\*\s*$")

    # Iterate with indices so we can look ahead for bullet-starting blocks
    for i, raw in enumerate(lines):
        line = raw.strip()
        if not line:
            if current_buf and current_buf[-1] != "":
                current_buf.append("")
            continue
        # Bold-only markdown heading
        m_bold = bold_md.match(line)
        if m_bold:
            flush()
            current_title = m_bold.group(1)
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
        # Heuristic: standalone non-bullet line treated as heading if next non-empty line is a bullet
        # This catches patterns like:
        #   We are counting on you to
        #   - Do X
        if line and len(line) <= 120 and not BULLET_RE.match(line):
            j = i + 1
            while j < len(lines) and not (lines[j] or "").strip():
                j += 1
            if j < len(lines):
                nxt = (lines[j] or "").strip()
                if BULLET_RE.match(nxt):
                    flush()
                    current_title = line
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
    # Build bullets, allowing non-bullet continuation lines to extend the last bullet
    for ln in block_lines:
        m = BULLET_RE.match(ln)
        if m:
            cand = m.group(1).strip()
            if not _is_heading_label(cand):
                items.append(cand)
        else:
            tail = (ln or "").strip()
            if tail and items and not _is_heading_label(tail):
                # Likely a wrapped continuation line (e.g., word split across newline)
                # Attach to the previous bullet, merging split words when appropriate.
                prev = items[-1]
                m_prev = re.search(r"([A-Za-z]{3,})$", prev)
                m_tail = re.match(r"^([a-z]{2,12})(\b.*)$", tail)
                if m_prev and m_tail:
                    # Merge without space for seamless word join
                    merged_word = m_prev.group(1) + m_tail.group(1)
                    items[-1] = prev[: m_prev.start(1)] + merged_word + m_tail.group(2)
                else:
                    items[-1] = f"{prev} {tail}".strip()
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
        # Expanded verbs frequently seen in bullet lists
        r"contribute",
        r"participate",
        r"create",
        r"help",
        r"verify",
        r"improve",
        r"deploy",
        r"write",
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

    # Quick benefits keyword matcher for merging heuristics
    ben_kw_quick = re.compile(
        r"\b(benefits|perks|equity|stock|compensation|remote|flexible|hybrid|work[- ]from[- ]anywhere|pto|vacation|holiday)\b",
        re.IGNORECASE,
    )

    # Merge unlabeled sections that are actually bullet lines promoted to titles
    # into the previous labeled section; additionally, if an unlabeled section
    # follows a Benefits section and looks like benefits (bullets/keywords),
    # merge it into Benefits to avoid fragmentation.
    merged: List[Tuple[str, List[str]]] = []
    last_labeled_idx = -1
    last_labeled_label = ""
    for title, block in sections:
        label = _classify_title(title) if title else ""
        if label:
            merged.append((title, block))
            last_labeled_idx = len(merged) - 1
            last_labeled_label = label
        else:
            t = (title or "").strip()
            if t and BULLET_RE.match(t) and last_labeled_idx >= 0:
                prev_title, prev_block = merged[last_labeled_idx]
                merged[last_labeled_idx] = (prev_title, prev_block + [title] + block)
            elif last_labeled_idx >= 0 and last_labeled_label == "benefits":
                has_bullets = any(BULLET_RE.match(ln or "") for ln in block)
                block_text = " ".join(block)
                if has_bullets or ben_kw_quick.search(block_text):
                    prev_title, prev_block = merged[last_labeled_idx]
                    # If the unlabeled title looks like a lowercase continuation (e.g., 'ment with ...'),
                    # include it as a non-bullet continuation line to allow stitch-back in extraction.
                    include_title = bool(
                        (title or "").strip()
                        and re.match(r"^[a-z]", (title or "").strip())
                    )
                    addition = ([title] if include_title else []) + block
                    merged[last_labeled_idx] = (prev_title, prev_block + addition)
                else:
                    merged.append((title, block))
            elif last_labeled_idx >= 0 and (t in {":", ";", "-", "–", "—"}):
                # Workday often splits 'Qualifications:' across lines as 'Qualifications' + ':'
                # Merge such colon-only titled blocks into the previous labeled section
                prev_title, prev_block = merged[last_labeled_idx]
                merged[last_labeled_idx] = (prev_title, prev_block + block)
            else:
                merged.append((title, block))
    sections = merged

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
            # Use full paragraph for about, but if the block is a short connector
            # (e.g., 'as', 'reporting to the', 'in Luxembourg.'), stitch it with the title
            blk = " ".join(block).strip()
            if (
                title
                and blk
                and len(blk) <= 60
                and re.match(
                    r"^(as|in|at|for|with|reporting to|based in|located in)\b",
                    blk,
                    re.IGNORECASE,
                )
            ):
                out_about.append(f"{title.strip()} {blk}".strip())
            elif title and not blk:
                out_about.append(title.strip())
            else:
                out_about.append(blk)
        elif label == "responsibilities":
            out_responsibilities.extend(items)
        elif label == "basic":
            out_basic.extend(items)
        elif label == "preferred":
            out_preferred.extend(items)
        elif label == "benefits":
            out_benefits.extend(items)

    # Always incorporate preface unlabeled sections before the first labeled section into About.
    # This handles Workday pages where intro paragraphs are split by decorative lines.
    if found_any:
        first_label_idx = None
        for idx, (t, _) in enumerate(sections):
            if _classify_title(t):
                first_label_idx = idx
                break
        if first_label_idx is not None and first_label_idx > 0:
            preface_chunks: List[str] = []
            for t, block in sections[:first_label_idx]:
                if block and any(not BULLET_RE.match(ln or "") for ln in block):
                    prefix = (t.strip() + " ") if t else ""
                    preface_chunks.append((prefix + " ".join(block)).strip())
            if preface_chunks:
                preface = " ".join(preface_chunks).strip()
                if out_about:
                    # Prepend to existing About
                    out_about[0] = f"{preface} {out_about[0]}".strip()
                else:
                    out_about.append(preface)

    # Redirect qualification-like bullets that leaked into responsibilities
    if out_responsibilities:
        qual_like = re.compile(
            r"^(?:we look for|we are looking for|in other words|you (?:are|have|love|excel|thrive|enjoy|possess)|you are a\b)",
            re.IGNORECASE,
        )
        moved = [x for x in out_responsibilities if qual_like.search(x)]
        if moved:
            out_responsibilities = [x for x in out_responsibilities if x not in moved]
            out_basic.extend(moved)

    # Split common run-on patterns inside responsibility bullets, conservatively
    if out_responsibilities:
        split_resps: List[str] = []
        for x in out_responsibilities:
            m = re.search(r"\bResponding to\b", x)
            if m:
                left = x[: m.start()].rstrip(" ,;:")
                right = x[m.start() :].strip()
                if left and right:
                    split_resps.extend([left, right])
                    continue
            split_resps.append(x)
        out_responsibilities = split_resps

    # Move benefit-like bullets that leaked into basic into benefits
    if out_basic:
        ben_line = re.compile(
            (
                r"^(?:we (?:offer|provide)|in addition,? we offer|what we offer|our offer|you will (?:get|enjoy)|"
                r"perks?\b|benefits? (?:include|we offer))"
            ),
            re.IGNORECASE,
        )
        moved_ben = [x for x in out_basic if ben_line.search(x)]
        if moved_ben:
            out_basic = [x for x in out_basic if x not in moved_ben]
            out_benefits.extend(moved_ben)

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
