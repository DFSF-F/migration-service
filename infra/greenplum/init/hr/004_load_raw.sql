truncate table raw.hr_employee_master_raw;
truncate table raw.hr_position_history_raw;
truncate table raw.hr_department_history_raw;
truncate table raw.hr_absence_events_raw;
truncate table raw.hr_overtime_events_raw;
truncate table raw.hr_dismissal_signals_raw;

\copy raw.hr_employee_master_raw from '/data/input/hr_employee_master_raw.csv' delimiter ',' csv header;
\copy raw.hr_position_history_raw from '/data/input/hr_position_history_raw.csv' delimiter ',' csv header;
\copy raw.hr_department_history_raw from '/data/input/hr_department_history_raw.csv' delimiter ',' csv header;
\copy raw.hr_absence_events_raw from '/data/input/hr_absence_events_raw.csv' delimiter ',' csv header;
\copy raw.hr_overtime_events_raw from '/data/input/hr_overtime_events_raw.csv' delimiter ',' csv header;
\copy raw.hr_dismissal_signals_raw from '/data/input/hr_dismissal_signals_raw.csv' delimiter ',' csv header;