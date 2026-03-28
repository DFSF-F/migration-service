truncate table raw.access_system_accounts_raw;
truncate table raw.access_role_assignments_raw;
truncate table raw.access_privileged_access_raw;
truncate table raw.access_login_events_raw;
truncate table raw.access_file_operations_raw;
truncate table raw.access_network_activity_raw;

\copy raw.access_system_accounts_raw from '/data/input/access_system_accounts_raw.csv' delimiter ',' csv header;
\copy raw.access_role_assignments_raw from '/data/input/access_role_assignments_raw.csv' delimiter ',' csv header;
\copy raw.access_privileged_access_raw from '/data/input/access_privileged_access_raw.csv' delimiter ',' csv header;
\copy raw.access_login_events_raw from '/data/input/access_login_events_raw.csv' delimiter ',' csv header;
\copy raw.access_file_operations_raw from '/data/input/access_file_operations_raw.csv' delimiter ',' csv header;
\copy raw.access_network_activity_raw from '/data/input/access_network_activity_raw.csv' delimiter ',' csv header;