# AWS setup

One-time setup for the cloud version, run from the repo root with the AWS CLI
(`aws login` first). Region: `us-east-2`.

| Resource | Name |
|----------|------|
| S3 bucket | `quake-pipeline-raw-phs67z` (private; raw files under `raw/`) |
| IAM user for Snowflake | `quake-snowflake-reader` (read-only on `raw/`, keys stored in the Snowflake stage) |
| IAM user for Airflow | `quake-pipeline` (read/write on `raw/`, programmatic access only) |

## 1. Bucket

```sh
aws s3api create-bucket --bucket quake-pipeline-raw-phs67z --region us-east-2 \
    --create-bucket-configuration LocationConstraint=us-east-2
aws s3api get-public-access-block --bucket quake-pipeline-raw-phs67z   # all four should be true
```

## 2. User Snowflake reads the bucket with

Snowflake would normally assume an IAM role (a storage integration). This account was created
with AWS's new "project" sign-up, whose AWS-managed resource control policy
`DenyAnyoneOutsideMyOrgAndAWS` denies `sts:*` and `s3:*` to principals outside the AWS
organization, so Snowflake's IAM user cannot assume the role. Changing that policy requires
activating advanced features, which is irreversible. Instead, Snowflake uses the keys of a
read-only user inside this account:

```sh
aws iam create-user --user-name quake-snowflake-reader
aws iam put-user-policy --user-name quake-snowflake-reader --policy-name read-raw \
    --policy-document file://infra/aws/snowflake-reader-policy.json
aws iam create-access-key --user-name quake-snowflake-reader   # paste into the stage only
```

The keys go straight into the Snowflake stage (`infra/snowflake/03_s3_stage.sql`), not into
`.env` or git.

## 3. User Airflow uploads with

```sh
aws iam create-user --user-name quake-pipeline
aws iam put-user-policy --user-name quake-pipeline --policy-name write-raw \
    --policy-document file://infra/aws/pipeline-user-policy.json
aws iam create-access-key --user-name quake-pipeline   # secret is shown once
```

The keys go in `.env` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), never in git. To rotate:
create a second key, update `.env`, then `aws iam delete-access-key` the old one.
