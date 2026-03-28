truncate table raw.finance_employee_expense_raw;
truncate table raw.finance_corporate_card_txn_raw;
truncate table raw.finance_advance_report_raw;
truncate table raw.finance_payroll_adjustment_raw;
truncate table raw.finance_vendor_payment_raw;
truncate table raw.finance_budget_limit_raw;

\copy raw.finance_employee_expense_raw from '/data/input/finance_employee_expense_raw.csv' delimiter ',' csv header;
\copy raw.finance_corporate_card_txn_raw from '/data/input/finance_corporate_card_txn_raw.csv' delimiter ',' csv header;
\copy raw.finance_advance_report_raw from '/data/input/finance_advance_report_raw.csv' delimiter ',' csv header;
\copy raw.finance_payroll_adjustment_raw from '/data/input/finance_payroll_adjustment_raw.csv' delimiter ',' csv header;
\copy raw.finance_vendor_payment_raw from '/data/input/finance_vendor_payment_raw.csv' delimiter ',' csv header;
\copy raw.finance_budget_limit_raw from '/data/input/finance_budget_limit_raw.csv' delimiter ',' csv header;