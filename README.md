# dbt_nba

## Launch Commands

```bash
docker compose down

docker stop $(docker ps -q --filter network=dbt_nba_nba_network)

docker stop $(docker ps -q --filter network=dbt_nba_nba_network) && docker compose down;

docker compose down

docker compose --profile dbt up --build -d
```

## DBT Commands once Docker Container Launched

```bash
docker compose exec dbt_runner bash

dbt run --select tag:staging --project-dir /usr/app/dbt/nba_analytics

dbt run --select intermediate --project-dir /usr/app/dbt/nba_analytics

exit
```

## Commands General

```bash
# You should still be inside the container. If not, run:
docker compose exec dbt_runner bash

# Run the command again
dbt run --select tag:staging --project-dir /usr/app/dbt/nba_analytics
```

```bash
# 1. Get a shell in your running container
docker compose exec dbt_runner bash

# 2. Run dbt debug, telling it exactly where the project is
dbt debug --project-dir /usr/app/dbt/nba_analytics

# --- Expected Successful Output ---
# 05:30:00  Running with dbt=1.10.13
# ...
# 05:30:01  Using profiles.yml file at /usr/app/dbt/profiles.yml
# 05:30:01  Using dbt_project.yml file at /usr/app/dbt/nba_analytics/dbt_project.yml
# ...
# 05:30:01  Connection:
# 05:30:01    host: postgres
# 05:30:01    port: 5432
# ...
# 05:30:01    search_path: analytics_dev, public
# ...
# 05:30:01  All checks passed!

# 3. Now run your models the same way
dbt run --project-dir /usr/app/dbt/nba_analytics

# 4. And your tests
dbt test --project-dir /usr/app/dbt/nba_analytics

# 5. Exit when done
exit
```

---

```bash
# First, start the dbt_runner container.
# The `--profile dbt` flag is needed because you defined it in the compose file.
docker compose --profile dbt up -d

 docker compose --profile dbt up --build -d

# 1. Open a shell inside the running container
docker compose exec dbt_runner bash

# --- You are now inside the container's shell ---

# 2. Navigate to your dbt project directory
# (Note: Your volume maps the whole ./dbt directory, so nba_analytics is inside)
cd nba_analytics

# 3. Verify your database connection
dbt debug

# 4. Install any dbt packages (if you had any)
dbt deps

# 5. Run your models
dbt run

# 6. Run your tests
dbt test

# 7. Exit the container shell
exit
```

`docker stop $(docker ps -q --filter network=dbt_nba_nba_network)`
