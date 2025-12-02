# Postgres Backup and Restore

## Clear Schema

### Option 1: Drop the entire public schema and recreate it

```bash
docker exec nba_postgres psql -U nba_admin -d nba_analytics -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
```

### Option 2: Restore without the -c flag (since we already dropped everything)

```bash
docker exec nba_postgres pg_restore -U nba_admin -d nba_analytics --no-owner --no-acl 2025.11.18_nba_backup.dump
```

## pg_dump

```bash
docker exec nba_go-postgres-1 pg_dump -U nbago -Fc nba_db > ./nba_backup.dump
```

```bash
docker exec nba_go-postgres-1 pg_dump -U nbago -Fc nba_db > ./2025.12_nba_backup.dump
```

## pg_restore

Copy .dump file into container

```bash
pg_restore -U nba_admin -d nba_analytics --no-owner --no-acl
```

```bash
docker exec nba_postgres pg_restore -U nba_admin -d nba_analytics --no-owner --no-acl ./nba_backup.dump
```

```bash
pg_restore -U nba_admin -d nba_analytics --no-owner --no-acl ./2025.11.27_nba_backup.dump
```

```bash
docker exec nba_postgres pg_restore -U nba_admin -d nba_analytics --no-owner --no-acl /Volumes/ROG_BLACK/code/projects/dbt_nba/data/pg_dumps/2025.11.25_nba_backup.dump
```

The `--no-owner` and `--no-acl flags` will prevent the "role 'nbago' does not exist" errors.
