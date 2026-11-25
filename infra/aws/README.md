# AWS setup

One-time setup for the cloud version, run from the repo root with the AWS CLI
(`aws login` first). Region: `us-east-2`.

| Resource | Name |
|----------|------|
| S3 bucket | `quake-pipeline-raw-phs67z` (private; raw files under `raw/`) |
| IAM role for Snowflake | `quake-snowflake-s3-read` (read-only on `raw/`) |
| IAM user for Airflow | `quake-pipeline` (read/write on `raw/`, programmatic access only) |

## 1. Bucket

```sh
aws s3api create-bucket --bucket quake-pipeline-raw-phs67z --region us-east-2 \
    --create-bucket-configuration LocationConstraint=us-east-2
aws s3api get-public-access-block --bucket quake-pipeline-raw-phs67z   # all four should be true
```

## 2. Role Snowflake will assume to read the bucket

```sh
aws iam create-role --role-name quake-snowflake-s3-read \
    --assume-role-policy-document file://infra/aws/snowflake-role-trust.json
aws iam put-role-policy --role-name quake-snowflake-s3-read --policy-name read-raw \
    --policy-document file://infra/aws/snowflake-role-policy.json
```

`snowflake-role-trust.json` is a placeholder that trusts only this account. After the
Snowflake storage integration is created (Step 3), it is replaced with Snowflake's IAM
user ARN and external ID from `DESC INTEGRATION`, then applied with
`aws iam update-assume-role-policy`.

## 3. User Airflow uploads with

```sh
aws iam create-user --user-name quake-pipeline
aws iam put-user-policy --user-name quake-pipeline --policy-name write-raw \
    --policy-document file://infra/aws/pipeline-user-policy.json
aws iam create-access-key --user-name quake-pipeline   # secret is shown once
```

The keys go in `.env` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), never in git. To rotate:
create a second key, update `.env`, then `aws iam delete-access-key` the old one.
