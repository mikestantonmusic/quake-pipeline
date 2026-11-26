-- Part 4: the user the Streamlit dashboard signs in as.
-- reporter_role (created in 01_account.sql) can only read the marts schema and use the
-- warehouse, so a leaked dashboard key can't change anything or see the raw data.
-- Paste the public key (the lines between BEGIN and END in
-- secrets/snowflake/dashboard_key.pub, joined without line breaks) below.

use role accountadmin;

create user if not exists dashboard_user
    type = service
    default_role = reporter_role
    default_warehouse = quake_wh
    default_namespace = quake.marts
    rsa_public_key = '<PASTE PUBLIC KEY HERE>'
    comment = 'Streamlit dashboard (quake-pipeline), read-only';

grant role reporter_role to user dashboard_user;

-- Check: RSA_PUBLIC_KEY_FP should show a SHA256 fingerprint.
desc user dashboard_user;
