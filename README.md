# NBA DBT Project

## Usage

`make shell`

```bash
dbt seed --project-dir /usr/app/dbt/nba_analytics

dbt run --select tag:staging --full-refresh --project-dir /usr/app/dbt/nba_analytics

dbt run --select intermediate --full-refresh --project-dir /usr/app/dbt/nba_analytics

dbt run --select tag:dimension --full-refresh --project-dir /usr/app/dbt/nba_analytics

dbt run --select tag:facts --full-refresh --project-dir /usr/app/dbt/nba_analytics
```
