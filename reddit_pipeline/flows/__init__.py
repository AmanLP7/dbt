"""Prefect orchestration for the pipeline.

Two flows, split along the ingestion/transformation seam:

- `reddit_extract_flow` calls the Reddit API and loads the raw Delta tables,
  then triggers `dbt-build` once the loads have succeeded.
- `dbt_build_flow` owns everything dbt and runs standalone when only the models
  need rebuilding.

They live outside `ingestion/` because the extract flow spans more than
ingestion: it hands off to the dbt layer. Run either from the repo root
(`python -m flows.reddit_extract_flow`, `python -m flows.dbt_build_flow`) so the
package imports resolve.

`prefect.yaml` deploys `reddit-extract` to the local `reddit-pool` work pool;
`dbt-build` runs only as the subflow it triggers, so it has no deployment.
"""
