{{
  config(
    materialized = 'view',
    bind=False
  )
}}

{% set artifact_models = [
  "dbt_models",
  "dbt_tests",
  "dbt_sources",
  "dbt_snapshots",
  "dbt_metrics",
  "dbt_exposures",
  "dbt_seeds",
  "dbt_columns",
] %}

SELECT TOP 100 PERCENT *
FROM (
{% for artifact_model in artifact_models %}
    SELECT
      '{{ artifact_model }}' AS artifacts_model,
       metadata_hash
    FROM {{ ref(artifact_model) }}
    {% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
) AS combined_data
ORDER BY metadata_hash