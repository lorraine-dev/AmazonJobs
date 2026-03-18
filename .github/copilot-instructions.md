# Copilot Workspace Instructions

This file is the start point for repository context and operational state.

## Where To Start

1. Read [README.md](../README.md) for architecture and local/CI usage.
2. Read [docs/START_HERE.md](../docs/START_HERE.md) for current operational state, commit boundaries, and runbook.
3. Read [.github/workflows/scraper.yml](workflows/scraper.yml) for schedule and automation behavior.

## Current Workflow Expectations

- Scheduled run cadence: every 5 days at 08:00 UTC.
- Manual runs: use workflow_dispatch and set amazon_engine only when needed.
- Scraper artifacts can be absent/expired on old runs; the workflow tolerates this.

## Source Of Truth Files

- Pipeline entrypoint: [src/scripts/run_scraper.py](../src/scripts/run_scraper.py)
- Data merge: [src/utils/combine_jobs.py](../src/utils/combine_jobs.py)
- Dashboard generation: [src/utils/data_processor.py](../src/utils/data_processor.py)
- Dashboard interactions: [docs/dashboard_interactions.js](../docs/dashboard_interactions.js)
- Skills taxonomy and mapping:
  - [docs/skills_taxonomy.json](../docs/skills_taxonomy.json)
  - [config/skill_aliases.yaml](../config/skill_aliases.yaml)
  - [config/skills_map.json](../config/skills_map.json)

## Commit Hygiene

Before commit, run `git status --short` and separate changes into logical commits:

- Workflow and automation updates.
- Skills extraction and canonicalization code/config.
- Dashboard rendering/JS updates.

Do not commit local caches or generated runtime data:

- `.hf_cache/`
- `data/`
- `logs/`
- `semantic_alias_runs_log.md`

## Internal Notes

Internal planning docs are intentionally local-only and ignored:

- [INTERNAL_NOTES.md](../INTERNAL_NOTES.md)
- [PROJECT_OPTIMIZATION.md](../PROJECT_OPTIMIZATION.md)
- [ProjectSummary.md](../ProjectSummary.md)
