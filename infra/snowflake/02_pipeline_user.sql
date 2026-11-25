-- Part 2: the user Airflow and dbt sign in as.
-- TYPE = SERVICE: a program, not a person. It has no password and signs in with a key pair.
-- Paste the public key (the lines between BEGIN and END in secrets/snowflake/pipeline_key.pub,
-- joined without line breaks) in place of the placeholder below.

use role accountadmin;

create user if not exists pipeline_user
    type = service
    default_role = pipeline_role
    default_warehouse = quake_wh
    default_namespace = quake.raw
    rsa_public_key = '<PASTE PUBLIC KEY HERE>'
    comment = 'Airflow and dbt (quake-pipeline)';

grant role pipeline_role to user pipeline_user;

-- Check: RSA_PUBLIC_KEY_FP should show a SHA256 fingerprint.
desc user pipeline_user;
