import logging
import re
from typing import Dict, List, Any, Tuple

import yaml


LOGGER = logging.getLogger(__name__)

# Canonical categories mirroring Amazon taxonomy used in the dashboard
CANONICAL_CATEGORIES: List[str] = [
    "Software Development",
    "Data Science",
    "Machine Learning Science",
    "Business Intelligence",
    "Project/Program/Product Management--Technical",
    "Operations, IT, & Support Engineering",
    "Research Science",
    "Solutions Architect",
    "Security",
    "Other",
]


def _load_yaml_mapping() -> Dict[str, Any]:
    """Load keyword/slug mapping from YAML. Falls back to defaults if missing."""
    path = "config/category_mapping.yaml"
    try:
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                raise ValueError(
                    "category_mapping.yaml must define a mapping dictionary"
                )
            return data
    except Exception as e:  # pragma: no cover - runtime fallback
        LOGGER.debug(f"Category mapping YAML not found or invalid, using defaults: {e}")
        return _default_mapping()


def _default_mapping() -> Dict[str, Any]:
    """Reasonable defaults if YAML is absent."""
    return {
        "categories": CANONICAL_CATEGORIES,
        "rules": {
            "Machine Learning Science": {
                "title": [
                    "machine learning",
                    "ml",
                    "deep learning",
                    "nlp",
                    "natural language",
                    "computer vision",
                    "cv",
                    "reinforcement learning",
                    "ai ",  # trailing space to avoid generic 'aid' etc.
                    " generative ai",
                    "genai",
                    " llm",
                ],
                "tech_slugs": [
                    "tensorflow",
                    "pytorch",
                    "scikit-learn",
                    "keras",
                    "huggingface",
                    "xgboost",
                    "lightgbm",
                    "transformers",
                ],
            },
            "Data Science": {
                "title": [
                    "data scientist",
                    "quant ",
                    "quantitative",
                    "experimentation",
                    "causal",
                    "econometric",
                    "statistician",
                    "applied scientist",  # DS if not strongly ML
                ],
                "tech_slugs": [
                    "statsmodels",
                    "prophet",
                ],
            },
            "Business Intelligence": {
                "title": [
                    "business intelligence",
                    "bi ",
                    "analytics engineer",
                    "data analyst",
                    "business analyst",
                    "analytics ",
                ],
                "tech_slugs": [
                    "tableau",
                    "powerbi",
                    "looker",
                    "qlik",
                    "superset",
                    "mode",
                    "metabase",
                ],
            },
            "Software Development": {
                "title": [
                    "software engineer",
                    "developer",
                    "sde",
                    "backend",
                    "front-end",
                    "frontend",
                    "full stack",
                    "data engineer",
                    "etl",
                    "pipeline",
                    "warehouse",
                    "platform engineer",
                ],
                "tech_slugs": [
                    "spark",
                    "kafka",
                    "hadoop",
                    "dbt",
                    "airflow",
                    "snowflake",
                    "bigquery",
                    "redshift",
                    "databricks",
                ],
            },
            "Operations, IT, & Support Engineering": {
                "title": [
                    "devops",
                    "sre",
                    "site reliability",
                    "systems engineer",
                    "infrastructure",
                    "platform",
                    "sysadmin",
                ],
                "tech_slugs": [
                    "kubernetes",
                    "docker",
                    "terraform",
                    "ansible",
                    "grafana",
                    "prometheus",
                    "helm",
                    "eks",
                    "ecs",
                ],
            },
            "Solutions Architect": {
                "title": [
                    "solutions architect",
                    "solution architect",
                    "cloud architect",
                ],
                "tech_slugs": [],
            },
            "Security": {
                "title": [
                    "security engineer",
                    "application security",
                    "appsec",
                    "cloud security",
                    "infosec",
                    "security analyst",
                ],
                "tech_slugs": [
                    "vault",
                    "snyk",
                    "burp",
                    "wireshark",
                    "osquery",
                ],
            },
            "Research Science": {
                "title": [
                    "research scientist",
                    "researcher",
                ],
                "tech_slugs": [],
            },
            "Project/Program/Product Management--Technical": {
                "title": [
                    "product manager",
                    "technical product manager",
                    "program manager",
                    "project manager",
                    "tpm",
                ],
                "tech_slugs": [],
            },
        },
        # Category precedence tweaks for ties
        "tie_breakers": {
            "ml_over_data_science": True,
            "bi_over_data_science": True,
            "security_override": True,
            "solutions_architect_override": True,
        },
        # Weights used for scoring
        "weights": {
            "title": 1.0,
            "normalized_title": 1.0,
            "tech_slugs": 0.8,
            "description": 0.4,
        },
        # Minimum score to avoid 'Other'
        "min_score": 1.0,
    }


_MAPPING: Dict[str, Any] = _load_yaml_mapping()
_RULES: Dict[str, Any] = _MAPPING.get("rules", {})
_WEIGHTS: Dict[str, float] = _MAPPING.get("weights", {})
_TIE: Dict[str, bool] = _MAPPING.get("tie_breakers", {})
_MIN_SCORE: float = float(_MAPPING.get("min_score", 1.0))


def _tokenize(text: str) -> str:
    return (text or "").lower()


def _count_keyword_hits(text: str, keywords: List[str]) -> int:
    if not text or not keywords:
        return 0
    t = text
    hits = 0
    for kw in keywords:
        if not kw:
            continue
        if re.search(r"\b" + re.escape(kw.strip()) + r"\b", t):
            hits += 1
        elif kw in t:
            # fallback substring
            hits += 1
    return hits


def _score_category(cat: str, job: Dict[str, Any]) -> float:
    rule = _RULES.get(cat, {}) or {}
    score = 0.0

    title = _tokenize(job.get("job_title", ""))
    ntitle = _tokenize(job.get("normalized_title", job.get("job_title", "")))
    desc = _tokenize(job.get("description", ""))
    slugs: List[str] = [s.lower() for s in job.get("technology_slugs", []) or []]

    score += _WEIGHTS.get("title", 1.0) * _count_keyword_hits(
        title, rule.get("title", [])
    )
    score += _WEIGHTS.get("normalized_title", 1.0) * _count_keyword_hits(
        ntitle, rule.get("title", [])
    )

    if slugs and rule.get("tech_slugs"):
        slug_hits = len(
            set(slugs) & set([s.lower() for s in rule.get("tech_slugs", [])])
        )
        score += _WEIGHTS.get("tech_slugs", 0.8) * slug_hits

    score += _WEIGHTS.get("description", 0.4) * _count_keyword_hits(
        desc, rule.get("title", [])
    )

    return score


def infer_job_category(job: Dict[str, Any]) -> str:
    """Infer the canonical category using multiple signals with tie-breaking.

    Args:
        job: TheirStack job dictionary
    Returns:
        Category string from CANONICAL_CATEGORIES
    """
    # First, early overrides for very specific roles
    title_all = " ".join(
        [
            _tokenize(job.get("job_title", "")),
            _tokenize(job.get("normalized_title", "")),
        ]
    )

    if _TIE.get("solutions_architect_override") and re.search(
        r"\bsolutions? architect\b", title_all
    ):
        return "Solutions Architect"
    if _TIE.get("security_override") and re.search(r"\bsecurity\b", title_all):
        return "Security"

    # Score all categories except Other
    scores: List[Tuple[str, float]] = []
    for cat in CANONICAL_CATEGORIES:
        if cat == "Other":
            continue
        scores.append((cat, _score_category(cat, job)))

    # Tie-break adjustments
    score_dict: Dict[str, float] = dict(scores)
    if (
        _TIE.get("ml_over_data_science")
        and score_dict.get("Machine Learning Science", 0) > 0
        and score_dict.get("Data Science", 0) > 0
    ):
        score_dict["Machine Learning Science"] += 0.5
    if (
        _TIE.get("bi_over_data_science")
        and score_dict.get("Business Intelligence", 0) > 0
        and score_dict.get("Data Science", 0) > 0
    ):
        score_dict["Business Intelligence"] += 0.5

    # If role clearly PM/TPM, keep it there (do not promote/demote by seniority)
    pm_hits = _count_keyword_hits(
        title_all,
        _RULES.get("Project/Program/Product Management--Technical", {}).get(
            "title", []
        ),
    )
    if pm_hits > 0:
        score_dict["Project/Program/Product Management--Technical"] += 0.5

    # Pick best
    best_cat, best_score = (
        max(score_dict.items(), key=lambda x: x[1]) if score_dict else ("Other", 0.0)
    )
    if best_score < _MIN_SCORE:
        return "Other"
    return best_cat
