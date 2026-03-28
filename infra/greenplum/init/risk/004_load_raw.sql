truncate table raw.risk_ib_incidents_raw;
truncate table raw.risk_security_incidents_raw;
truncate table raw.risk_compliance_incidents_raw;
truncate table raw.risk_nonwork_activity_raw;
truncate table raw.risk_employee_registry_raw;
truncate table raw.risk_org_structure_raw;

\copy raw.risk_org_structure_raw from '/data/input/risk_org_structure_raw.csv' delimiter ',' csv header;
\copy raw.risk_employee_registry_raw from '/data/input/risk_employee_registry_raw.csv' delimiter ',' csv header;
\copy raw.risk_ib_incidents_raw from '/data/input/risk_ib_incidents_raw.csv' delimiter ',' csv header;
\copy raw.risk_security_incidents_raw from '/data/input/risk_security_incidents_raw.csv' delimiter ',' csv header;
\copy raw.risk_compliance_incidents_raw from '/data/input/risk_compliance_incidents_raw.csv' delimiter ',' csv header;
\copy raw.risk_nonwork_activity_raw from '/data/input/risk_nonwork_activity_raw.csv' delimiter ',' csv header;