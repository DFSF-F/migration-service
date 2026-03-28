from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path


@dataclass
class Employee:
    employee_src_id: str
    employee_number: str
    full_name: str
    tab_num: str
    employment_status: str
    hire_date: date
    department_src_id: str
    department_name: str
    manager_src_id: str | None


EXPENSE_TYPES = [
    ("TRAVEL", "Командировочные расходы", "TRAVEL"),
    ("MEAL", "Представительские расходы", "HOSPITALITY"),
    ("TRAINING", "Обучение", "DEVELOPMENT"),
    ("OFFICE", "Офисные расходы", "OFFICE"),
    ("SOFTWARE", "Покупка ПО", "IT"),
    ("MOBILE", "Мобильная связь", "TELECOM"),
]

CARD_CATEGORIES = [
    ("TRAVEL", "4511"),
    ("HOTEL", "7011"),
    ("RESTAURANT", "5812"),
    ("SOFTWARE", "5734"),
    ("ELECTRONICS", "5732"),
    ("RETAIL", "5311"),
]

PAYROLL_ADJUSTMENTS = [
    ("BONUS_CORR", "Корректировка премии", "BONUS"),
    ("MANUAL_DEDUCT", "Ручное удержание", "DEDUCTION"),
    ("COMP_CORR", "Корректировка компенсации", "COMPENSATION"),
    ("REIMBURSEMENT_CORR", "Корректировка возмещения", "REIMBURSEMENT"),
]

PAYMENT_TYPES = [
    ("SERVICE", "Оплата услуг"),
    ("GOODS", "Оплата товаров"),
    ("URGENT", "Срочная оплата"),
]

VENDORS = [
    ("V0001", "ООО Тревел Сервис"),
    ("V0002", "АО Город Отель"),
    ("V0003", "ООО Софт Системс"),
    ("V0004", "ООО Офис Маркет"),
    ("V0005", "ООО Бизнес Такси"),
    ("V0006", "ООО Телеком Плюс"),
    ("V0007", "ООО Аналитика и Данные"),
    ("V0008", "ООО Корпоративное питание"),
    ("V0009", "АО Вендор Групп"),
    ("V0010", "ООО Экспресс Поставки"),
]

COUNTRIES = [
    ("Russia", "Moscow"),
    ("Russia", "Saint Petersburg"),
    ("Russia", "Kazan"),
    ("Kazakhstan", "Almaty"),
    ("Turkey", "Istanbul"),
    ("UAE", "Dubai"),
]

MERCHANTS = [
    "AeroTickets",
    "Hotel Plaza",
    "OfficeMarket",
    "SoftCloud",
    "TaxiGo",
    "Market24",
    "TechStore",
    "Restaurant Hall",
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def d_to_str(value: date | None) -> str:
    return value.strftime("%Y-%m-%d") if value else ""


def dt_to_str(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else ""


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def random_datetime(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = int(delta.total_seconds())
    return start + timedelta(seconds=random.randint(0, seconds))


def load_employees(output_dir: Path) -> list[Employee]:
    path = output_dir / "hr_employee_master_raw.csv"
    if not path.exists():
        raise FileNotFoundError("Generate HR raw data first: hr_employee_master_raw.csv not found.")

    employees: list[Employee] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            employees.append(
                Employee(
                    employee_src_id=row["employee_src_id"],
                    employee_number=row["employee_number"],
                    full_name=row["full_name"],
                    tab_num=row["tab_num"],
                    employment_status=row["employment_status"],
                    hire_date=datetime.strptime(row["hire_date"], "%Y-%m-%d").date(),
                    department_src_id=row["department_src_id"],
                    department_name=row["department_name"],
                    manager_src_id=row["manager_src_id"] or None,
                )
            )
    return employees


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate raw data for finance product.")
    parser.add_argument("--output-dir", default="scripts/data_gen/output")
    parser.add_argument("--seed", type=int, default=45)
    parser.add_argument("--batch-id", default="finance_batch_001")
    args = parser.parse_args()

    random.seed(args.seed)

    output_dir = Path(args.output_dir)
    ensure_dir(output_dir)

    employees = load_employees(output_dir)
    load_dttm = datetime(2024, 12, 31, 12, 0, 0)
    year_start = datetime(2024, 1, 1, 0, 0, 0)
    year_end = datetime(2024, 12, 31, 23, 59, 59)

    expense_rows: list[dict] = []
    card_rows: list[dict] = []
    advance_rows: list[dict] = []
    payroll_rows: list[dict] = []
    vendor_payment_rows: list[dict] = []
    budget_rows: list[dict] = []

    expense_id = 1
    card_id = 1
    advance_id = 1
    payroll_id = 1
    vendor_payment_id = 1
    budget_id = 1

    cost_centers: dict[str, str] = {}
    for e in employees:
        cost_centers[f"CC_{e.department_src_id}"] = f"ЦФО {e.department_name}"

    for cost_center_code, cost_center_name in cost_centers.items():
        for month in (date(2024, 3, 1), date(2024, 6, 1), date(2024, 9, 1), date(2024, 12, 1)):
            budget_amount = round(random.uniform(800_000, 5_000_000), 2)
            consumed_amount = round(budget_amount * random.uniform(0.55, 1.25), 2)
            budget_rows.append(
                {
                    "src_budget_limit_id": f"BL-{budget_id:06d}",
                    "cost_center_code": cost_center_code,
                    "cost_center_name": cost_center_name,
                    "budget_period": d_to_str(month),
                    "budget_amount_rub": budget_amount,
                    "consumed_amount_rub": consumed_amount,
                    "exceeded_flag": "Y" if consumed_amount > budget_amount else "N",
                    "source_system": "finance_budget",
                    "load_dttm": dt_to_str(load_dttm),
                    "batch_id": args.batch_id,
                }
            )
            budget_id += 1

    for e in employees:
        cost_center_code = f"CC_{e.department_src_id}"

        expense_cnt = random.choices([0, 1, 2, 3, 4], weights=[18, 26, 28, 18, 10], k=1)[0]
        for _ in range(expense_cnt):
            exp_code, exp_name, exp_category = random.choice(EXPENSE_TYPES)
            vendor_src_id, _vendor_name = random.choice(VENDORS)
            expense_rows.append(
                {
                    "src_expense_id": f"EXP-{expense_id:07d}",
                    "employee_src_id": e.employee_src_id,
                    "expense_date": d_to_str(date(2024, 1, 1) + timedelta(days=random.randint(0, 364))),
                    "expense_type_code": exp_code,
                    "expense_type_name": exp_name,
                    "expense_category": exp_category,
                    "amount_rub": round(random.uniform(1_500, 85_000), 2),
                    "currency_code": "RUB",
                    "project_code": f"PRJ-{random.randint(1, 120):04d}",
                    "cost_center_code": cost_center_code,
                    "vendor_src_id": vendor_src_id,
                    "expense_status": random.choice(["APPROVED", "APPROVED", "PAID", "PENDING"]),
                    "reimbursable_flag": "Y" if random.random() < 0.82 else "N",
                    "source_system": "finance_expense",
                    "load_dttm": dt_to_str(load_dttm),
                    "batch_id": args.batch_id,
                }
            )
            expense_id += 1

        card_cnt = random.choices([0, 1, 2, 3, 5, 8], weights=[8, 16, 22, 24, 20, 10], k=1)[0]
        for _ in range(card_cnt):
            category, mcc = random.choice(CARD_CATEGORIES)
            country, city = random.choice(COUNTRIES)
            suspicious = "Y" if (country != "Russia" and random.random() < 0.35) or random.random() < 0.05 else "N"

            card_rows.append(
                {
                    "src_card_txn_id": f"CARD-{card_id:07d}",
                    "employee_src_id": e.employee_src_id,
                    "transaction_dttm": dt_to_str(random_datetime(year_start, year_end)),
                    "merchant_name": random.choice(MERCHANTS),
                    "mcc_code": mcc,
                    "transaction_category": category,
                    "amount_rub": round(random.uniform(500, 120_000), 2),
                    "currency_code": "RUB",
                    "country_name": country,
                    "city_name": city,
                    "card_present_flag": "Y" if random.random() < 0.45 else "N",
                    "reversal_flag": "Y" if random.random() < 0.03 else "N",
                    "suspicious_flag": suspicious,
                    "source_system": "finance_card",
                    "load_dttm": dt_to_str(load_dttm),
                    "batch_id": args.batch_id,
                }
            )
            card_id += 1

        advance_cnt = random.choices([0, 1, 2], weights=[35, 45, 20], k=1)[0]
        for _ in range(advance_cnt):
            total_amount = round(random.uniform(5_000, 180_000), 2)
            rejected_amount = round(total_amount * random.uniform(0.0, 0.35), 2) if random.random() < 0.20 else 0.0
            approved_amount = round(total_amount - rejected_amount, 2)
            overdue_days = random.randint(1, 45) if random.random() < 0.18 else 0
            status = "OVERDUE" if overdue_days > 0 else random.choice(["APPROVED", "CLOSED", "IN_REVIEW"])

            advance_rows.append(
                {
                    "src_advance_report_id": f"ADV-{advance_id:07d}",
                    "employee_src_id": e.employee_src_id,
                    "report_period": d_to_str(date(2024, random.randint(1, 12), 1)),
                    "total_amount_rub": total_amount,
                    "approved_amount_rub": approved_amount,
                    "rejected_amount_rub": rejected_amount,
                    "overdue_days": overdue_days,
                    "report_status": status,
                    "approver_employee_src_id": e.manager_src_id or "",
                    "source_system": "finance_advance",
                    "load_dttm": dt_to_str(load_dttm),
                    "batch_id": args.batch_id,
                }
            )
            advance_id += 1

        payroll_cnt = random.choices([0, 1, 2], weights=[62, 28, 10], k=1)[0]
        for _ in range(payroll_cnt):
            adj_code, adj_name, adj_group = random.choice(PAYROLL_ADJUSTMENTS)
            payroll_rows.append(
                {
                    "src_payroll_adj_id": f"PAY-{payroll_id:07d}",
                    "employee_src_id": e.employee_src_id,
                    "payroll_month": d_to_str(date(2024, random.randint(1, 12), 1)),
                    "adjustment_type_code": adj_code,
                    "adjustment_type_name": adj_name,
                    "adjustment_reason_group": adj_group,
                    "amount_rub": round(random.uniform(-45_000, 65_000), 2),
                    "manual_flag": "Y" if random.random() < 0.55 else "N",
                    "approved_flag": "Y" if random.random() < 0.88 else "N",
                    "source_system": "finance_payroll",
                    "load_dttm": dt_to_str(load_dttm),
                    "batch_id": args.batch_id,
                }
            )
            payroll_id += 1

    for _ in range(max(2500, len(employees))):
        e = random.choice(employees)
        vendor_src_id, vendor_name = random.choice(VENDORS)
        payment_type_code, payment_type_name = random.choice(PAYMENT_TYPES)
        urgent = "Y" if payment_type_code == "URGENT" or random.random() < 0.08 else "N"

        vendor_payment_rows.append(
            {
                "src_vendor_payment_id": f"VP-{vendor_payment_id:07d}",
                "employee_src_id": e.employee_src_id if random.random() < 0.82 else "",
                "vendor_src_id": vendor_src_id,
                "vendor_name": vendor_name,
                "payment_date": d_to_str(date(2024, 1, 1) + timedelta(days=random.randint(0, 364))),
                "payment_amount_rub": round(random.uniform(3_000, 350_000), 2),
                "payment_type_code": payment_type_code,
                "payment_type_name": payment_type_name,
                "contract_code": f"CTR-{random.randint(1, 800):05d}",
                "urgent_flag": urgent,
                "source_system": "finance_ap",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": args.batch_id,
            }
        )
        vendor_payment_id += 1

    write_csv(
        output_dir / "finance_employee_expense_raw.csv",
        [
            "src_expense_id",
            "employee_src_id",
            "expense_date",
            "expense_type_code",
            "expense_type_name",
            "expense_category",
            "amount_rub",
            "currency_code",
            "project_code",
            "cost_center_code",
            "vendor_src_id",
            "expense_status",
            "reimbursable_flag",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        expense_rows,
    )

    write_csv(
        output_dir / "finance_corporate_card_txn_raw.csv",
        [
            "src_card_txn_id",
            "employee_src_id",
            "transaction_dttm",
            "merchant_name",
            "mcc_code",
            "transaction_category",
            "amount_rub",
            "currency_code",
            "country_name",
            "city_name",
            "card_present_flag",
            "reversal_flag",
            "suspicious_flag",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        card_rows,
    )

    write_csv(
        output_dir / "finance_advance_report_raw.csv",
        [
            "src_advance_report_id",
            "employee_src_id",
            "report_period",
            "total_amount_rub",
            "approved_amount_rub",
            "rejected_amount_rub",
            "overdue_days",
            "report_status",
            "approver_employee_src_id",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        advance_rows,
    )

    write_csv(
        output_dir / "finance_payroll_adjustment_raw.csv",
        [
            "src_payroll_adj_id",
            "employee_src_id",
            "payroll_month",
            "adjustment_type_code",
            "adjustment_type_name",
            "adjustment_reason_group",
            "amount_rub",
            "manual_flag",
            "approved_flag",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        payroll_rows,
    )

    write_csv(
        output_dir / "finance_vendor_payment_raw.csv",
        [
            "src_vendor_payment_id",
            "employee_src_id",
            "vendor_src_id",
            "vendor_name",
            "payment_date",
            "payment_amount_rub",
            "payment_type_code",
            "payment_type_name",
            "contract_code",
            "urgent_flag",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        vendor_payment_rows,
    )

    write_csv(
        output_dir / "finance_budget_limit_raw.csv",
        [
            "src_budget_limit_id",
            "cost_center_code",
            "cost_center_name",
            "budget_period",
            "budget_amount_rub",
            "consumed_amount_rub",
            "exceeded_flag",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        budget_rows,
    )

    print("Finance raw data generated successfully.")
    print(f"Output directory: {output_dir.resolve()}")
    print(f"Employees used: {len(employees)}")
    print(f"Expenses generated: {len(expense_rows)}")
    print(f"Card transactions generated: {len(card_rows)}")
    print(f"Advance reports generated: {len(advance_rows)}")
    print(f"Payroll adjustments generated: {len(payroll_rows)}")
    print(f"Vendor payments generated: {len(vendor_payment_rows)}")
    print(f"Budget limits generated: {len(budget_rows)}")


if __name__ == "__main__":
    main()