-- Part 3: let Snowflake read the S3 bucket.
--
-- The usual approach is a storage integration: Snowflake assumes an IAM role in our AWS
-- account, and no AWS keys are stored in Snowflake. That doesn't work here. This AWS
-- account was created with the new "project" sign-up, whose AWS-managed resource control
-- policy (DenyAnyoneOutsideMyOrgAndAWS) denies sts:* and s3:* to principals outside the
-- AWS organization, including Snowflake's IAM user. It can only be changed by activating
-- advanced features (irreversible).
--
-- Instead the stage uses access keys of a read-only IAM user inside our account,
-- quake-snowflake-reader (infra/aws/README.md). Snowflake stores them encrypted.
-- Trade-off: long-lived keys to rotate by hand, instead of keyless role access.
--
-- Run as pipeline_role. Paste the keys into the worksheet only, never into this file.

use role pipeline_role;
use warehouse quake_wh;

create or replace stage quake.raw.s3_raw
    url = 's3://quake-pipeline-raw-phs67z/raw/'
    credentials = (aws_key_id = '<ACCESS KEY ID>' aws_secret_key = '<SECRET ACCESS KEY>')
    file_format = (type = json);

-- The test: should list the hourly files Airflow uploaded.
list @quake.raw.s3_raw;

-- To rotate the keys: create a new key for quake-snowflake-reader, then
--   alter stage quake.raw.s3_raw set credentials = (aws_key_id = '...' aws_secret_key = '...');
-- check `list` works, and delete the old key in AWS.
