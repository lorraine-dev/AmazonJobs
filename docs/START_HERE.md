# Start Here

This document is the operational map for this repository.

## What This Project Does

- Scrapes Amazon Jobs plus TheirStack.
- Merges raw outputs into a combined dataset.
- Generates and deploys a GitHub Pages dashboard.
- Builds a skills canonicalization map used by dashboard analytics.

## Quick Navigation

- Setup and full usage: [README.md](../README.md)
- CI workflow and schedule: [scraper workflow](../.github/workflows/scraper.yml)
- Pipeline entrypoint: [run_scraper.py](../src/scripts/run_scraper.py)
- Dashboard build: [data_processor.py](../src/utils/data_processor.py)
- Frontend interactions: [dashboard_interactions.js](dashboard_interactions.js)

## Skills Pipeline Map

- Extract skills: [extract_skills.py](../src/scripts/extract_skills.py)
- Generate fuzzy merge candidates: [generate_fuzzy_merges.py](../src/scripts/generate_fuzzy_merges.py)
- Generate final map: [generate_skills_map.py](../src/scripts/generate_skills_map.py)
- Canonicalization helpers: [skills_canonicalizer.py](../src/utils/skills_canonicalizer.py)
- Fuzzy merge helpers: [skills_fuzzy_merge.py](../src/utils/skills_fuzzy_merge.py)
- Taxonomy and aliases:
  - [skills_taxonomy.json](skills_taxonomy.json)
  - [skill_aliases.yaml](../config/skill_aliases.yaml)
  - [skills_map.json](skills_map.json)

## State and Runtime Data

These paths are generated at runtime and should remain untracked:

- `data/`
- `logs/`
- `.hf_cache/`
- `semantic_alias_runs_log.md`

## Recommended Commit Grouping

1. Workflow automation changes.
2. Skills extraction/canonicalization source + config changes.
3. Dashboard template/rendering changes.
4. Documentation updates.

## Recovery Checklist

If context is lost, do this in order:

1. Check current branch and pending edits: `git status --short`.
2. Confirm workflow settings and cadence in [scraper workflow](../.github/workflows/scraper.yml).
3. Run local pipeline entrypoint: `python src/scripts/run_scraper.py`.
4. Regenerate dashboard: `python -m src.utils.data_processor`.
5. Verify docs and mapping files are aligned:
   - [README.md](../README.md)
   - [skills_taxonomy.json](skills_taxonomy.json)
   - [skills_map.json](skills_map.json)
