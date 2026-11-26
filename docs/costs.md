# Costs and teardown

The cloud version is designed to cost about **$5 a month**, with a hard cap of 4 Snowflake credits (about $8).

## Where the money goes

| Service | What's billed | Expected per month |
|---------|---------------|--------------------|
| Snowflake compute | The X-Small warehouse: 1 credit an hour while running, billed per second with a 60-second minimum each time it starts. About $2 a credit on Standard edition | **$4–6**: about 4 minutes a day for the load and dbt, plus about a minute when the dashboard first queries new data |
| Snowflake storage | Compressed data, about $23 per TB a month | Under $0.05 (tens of MB) |
| Snowflake cloud services | Metadata, compiling queries, `COPY` file tracking | Free while it stays under 10% of the day's compute, which it does here |
| S3 | Storage ($0.023 per GB), uploads ($0.005 per 1,000) | Under $0.10 (about 150 MB and 750 uploads a month) |
| Data transfer S3 → Snowflake | Both are in AWS `us-east-2` | Free |
| Streamlit Community Cloud | Hosting the dashboard | Free |
| GitHub Actions | CI | Free for public repos (2,000 minutes a month on private ones) |
| Airflow | Runs in Docker on a laptop | Free |

What keeps it cheap:

- **Hourly extract, daily load.** Downloading to S3 doesn't involve Snowflake. The warehouse starts once a day.
- **Auto-suspend after 60 seconds** (`01_account.sql`), so the warehouse never sits idle for long.
- **Fixed dashboard queries.** Snowflake answers a repeated query from its result cache, without the warehouse, until the underlying tables change. The dashboard always runs the same two queries and filters in pandas, so visitors between daily loads cost nothing.
- **Resource monitor `QUAKE_MONTHLY`.** It notifies at 75% of 4 credits and suspends the warehouse at 100%, so a bug can't run up a large bill. If it triggers, the pipeline and dashboard stop until the next month or until the quota is raised.

## Checking what it actually costs

### Snowflake

In a Snowsight worksheet (usage views lag by up to 3 hours):

```sql
use role accountadmin;

-- Credits per day for the last 30 days.
select to_date(start_time) as day, round(sum(credits_used), 3) as credits
from snowflake.account_usage.warehouse_metering_history
where warehouse_name = 'QUAKE_WH' and start_time > dateadd(day, -30, current_timestamp())
group by day
order by day;

-- Credits used this month against the 4-credit quota.
show resource monitors like 'QUAKE_MONTHLY';

-- Which part of the pipeline runs queries: load, dbt or dashboard.
select query_tag, count(*) as queries, round(sum(total_elapsed_time) / 1000) as seconds
from snowflake.account_usage.query_history
where query_tag like 'quake-pipeline:%' and start_time > dateadd(day, -7, current_timestamp())
group by query_tag
order by seconds desc;
```

Snowsight also shows spend under **Admin → Cost management**.

### AWS

In the console, open **Billing and Cost Management**: **Bills** shows this month's charges by service. A budget under **Budgets** (for example $2 a month, with an email alert) warns you before a surprise.

## Pausing

To stop all Snowflake compute without deleting anything, pause `quake_transform` in Airflow. Downloads to S3 can keep running for free. Once the result cache expires (24 hours), the first dashboard visit after that runs the warehouse for about a minute.

## Teardown

Removes everything this project created. Steps 3 and 4 can't be undone.

**1. Dashboard.** On [share.streamlit.io](https://share.streamlit.io), open the app's **⋮** menu and choose **Delete**.

**2. Airflow.** Stops the containers and deletes the local Postgres volume (Airflow's run history):

```sh
docker compose down -v
```

**3. Snowflake.** In a Snowsight worksheet (select all, then run):

```sql
use role accountadmin;
drop database if exists quake;          -- all tables, the stage and its stored S3 keys
drop warehouse if exists quake_wh;
drop resource monitor if exists quake_monthly;
drop user if exists pipeline_user;
drop user if exists dashboard_user;
drop role if exists pipeline_role;
drop role if exists reporter_role;
```

**4. AWS.** From the repo root, after `aws login`:

```sh
aws s3 rm s3://quake-pipeline-raw-phs67z --recursive
aws s3api delete-bucket --bucket quake-pipeline-raw-phs67z

# Each IAM user: delete its access keys, its inline policy, then the user.
aws iam list-access-keys --user-name quake-pipeline        # note each AccessKeyId
aws iam delete-access-key --user-name quake-pipeline --access-key-id <AccessKeyId>
aws iam delete-user-policy --user-name quake-pipeline --policy-name write-raw
aws iam delete-user --user-name quake-pipeline

aws iam list-access-keys --user-name quake-snowflake-reader
aws iam delete-access-key --user-name quake-snowflake-reader --access-key-id <AccessKeyId>
aws iam delete-user-policy --user-name quake-snowflake-reader --policy-name read-raw
aws iam delete-user --user-name quake-snowflake-reader
```

**5. Local secrets.** Delete `secrets/`, `dashboard/.streamlit/secrets.toml`, and the AWS and Snowflake lines in `.env`.
