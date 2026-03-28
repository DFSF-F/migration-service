from __future__ import annotations

import argparse
import csv
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path


@dataclass
class Department:
    department_src_id: str
    department_name: str
    parent_department_src_id: str | None
    block_name: str
    function_name: str
    region_name: str
    org_level: int


@dataclass
class Employee:
    employee_src_id: str
    employee_number: str
    full_name: str
    tab_num: str
    department_src_id: str
    department_name: str
    manager_src_id: str | None
    employment_status: str
    hire_date: date
    dismissal_date: date | None


GENDERS = ["M", "F"]
WORK_FORMATS = ["OFFICE", "HYBRID", "REMOTE"]
GRADES = ["G1", "G2", "G3", "G4", "G5", "M1", "M2"]
ABSENCE_TYPES = [
    ("VACATION", "Ежегодный отпуск", "PLANNED"),
    ("SICK_LEAVE", "Больничный", "HEALTH"),
    ("UNPAID_LEAVE", "Отпуск без содержания", "PERSONAL"),
    ("DAY_OFF", "Отгул", "WORKLOAD"),
    ("TRAINING_LEAVE", "Учебный отпуск", "DEVELOPMENT"),
]
OVERTIME_REASONS = [
    ("MONTH_END", "Закрытие периода"),
    ("PROJECT_LOAD", "Проектная нагрузка"),
    ("STAFF_SHORTAGE", "Недостаток персонала"),
    ("PEAK_LOAD", "Пиковая нагрузка"),
]
DISMISSAL_SIGNALS = [
    ("LOW_ENGAGEMENT", "Снижение вовлечённости", "ENGAGEMENT"),
    ("MARKET_ACTIVITY", "Активность на рынке труда", "ATTRITION"),
    ("MANAGER_CONFLICT", "Конфликт с руководителем", "RELATIONS"),
    ("COMPENSATION_GAP", "Риск из-за уровня компенсации", "COMPENSATION"),
    ("ROLE_STAGNATION", "Карьерная стагнация", "CAREER"),
]

POSITION_PREFIXES = {
    "Безопасность": ["Специалист", "Ведущий специалист", "Старший аналитик", "Главный эксперт"],
    "Комплаенс": ["Специалист", "Эксперт", "Старший эксперт", "Менеджер"],
    "Продажи": ["Менеджер", "Старший менеджер", "Главный менеджер", "Руководитель группы"],
    "Операции": ["Специалист", "Старший специалист", "Операционный менеджер", "Руководитель смены"],
    "ИТ": ["Инженер", "Старший инженер", "Системный аналитик", "Технический лидер"],
}

FIRST_NAMES_M = ["Иван", "Максим", "Даниил", "Алексей", "Павел", "Сергей", "Андрей", "Михаил"]
FIRST_NAMES_F = ["Анна", "Ольга", "Мария", "Елена", "Наталья", "Татьяна", "Виктория", "Юлия"]
LAST_NAMES = ["Иванов", "Петров", "Сидоров", "Кузнецов", "Смирнов", "Фролов", "Попов", "Волков", "Лебедев"]
MIDDLE_NAMES_M = ["Иванович", "Сергеевич", "Андреевич", "Павлович", "Олегович", "Игоревич"]
MIDDLE_NAMES_F = ["Ивановна", "Сергеевна", "Андреевна", "Павловна", "Олеговна", "Игоревна"]


def dt_to_str(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else ""


def d_to_str(value: date | None) -> str:
    return value.strftime("%Y-%m-%d") if value else ""


def random_datetime(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = int(delta.total_seconds())
    return start + timedelta(seconds=random.randint(0, seconds))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_full_name(gender: str) -> str:
    last_name = random.choice(LAST_NAMES)
    if gender == "M":
        return f"{last_name} {random.choice(FIRST_NAMES_M)} {random.choice(MIDDLE_NAMES_M)}"
    return f"{last_name}а {random.choice(FIRST_NAMES_F)} {random.choice(MIDDLE_NAMES_F)}"


def infer_position(function_name: str) -> tuple[str, str]:
    prefixes = POSITION_PREFIXES.get(function_name, ["Специалист", "Старший специалист"])
    prefix = random.choice(prefixes)
    suffix = {
        "Безопасность": "по безопасности",
        "Комплаенс": "по комплаенсу",
        "Продажи": "по продажам",
        "Операции": "операций",
        "ИТ": "ИТ",
    }.get(function_name, "подразделения")
    position_name = f"{prefix} {suffix}"
    position_code = (
        prefix.upper().replace(" ", "_")
        + "_"
        + function_name.upper().replace(" ", "_")
    )
    return position_code[:64], position_name[:255]


def load_risk_departments(output_dir: Path) -> list[Department]:
    path = output_dir / "risk_org_structure_raw.csv"
    if not path.exists():
        return []

    departments: list[Department] = []
    seen: set[str] = set()

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dept_id = row["department_src_id"]
            if dept_id in seen:
                continue
            seen.add(dept_id)
            departments.append(
                Department(
                    department_src_id=dept_id,
                    department_name=row["department_name"],
                    parent_department_src_id=row["parent_department_src_id"] or None,
                    block_name=row["block_name"],
                    function_name=row["function_name"],
                    region_name=row["region_name"],
                    org_level=int(row["org_level"]),
                )
            )

    return departments


def load_risk_employees(output_dir: Path) -> list[Employee]:
    path = output_dir / "risk_employee_registry_raw.csv"
    if not path.exists():
        return []

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
                    department_src_id=row["department_src_id"],
                    department_name=row["department_name"],
                    manager_src_id=row["manager_src_id"] or None,
                    employment_status=row["employment_status"],
                    hire_date=datetime.strptime(row["hire_date"], "%Y-%m-%d").date(),
                    dismissal_date=datetime.strptime(row["dismissal_date"], "%Y-%m-%d").date()
                    if row["dismissal_date"]
                    else None,
                )
            )

    return employees


def fallback_departments(count: int) -> list[Department]:
    blocks = [
        ("Корпоративный блок", "Безопасность"),
        ("Корпоративный блок", "Комплаенс"),
        ("Бизнес-блок", "Продажи"),
        ("Операционный блок", "Операции"),
        ("Технологический блок", "ИТ"),
    ]
    regions = ["Москва", "Санкт-Петербург", "Екатеринбург", "Казань", "Новосибирск"]

    result = []
    for i in range(1, count + 1):
        block_name, function_name = random.choice(blocks)
        result.append(
            Department(
                department_src_id=f"D{i:04d}",
                department_name=f"{function_name} {i}",
                parent_department_src_id=None if i <= 5 else f"D{random.randint(1, max(1, i // 3)):04d}",
                block_name=block_name,
                function_name=function_name,
                region_name=random.choice(regions),
                org_level=1 if i <= 5 else random.choice([2, 3]),
            )
        )
    return result


def fallback_employees(count: int, departments: list[Department]) -> list[Employee]:
    result = []
    for i in range(1, count + 1):
        dept = random.choice(departments)
        gender = random.choice(GENDERS)
        result.append(
            Employee(
                employee_src_id=f"E{i:05d}",
                employee_number=f"EMP-{i:05d}",
                full_name=build_full_name(gender),
                tab_num=f"T{i:05d}",
                department_src_id=dept.department_src_id,
                department_name=dept.department_name,
                manager_src_id=None if i <= 20 else f"E{random.randint(1, i - 1):05d}",
                employment_status="ACTIVE",
                hire_date=date(2019, 1, 1) + timedelta(days=random.randint(0, 2200)),
                dismissal_date=None,
            )
        )
    return result


def generate_department_history(
    output_dir: Path,
    departments: list[Department],
    batch_id: str,
    load_dttm: datetime,
) -> None:
    rows: list[dict] = []

    for idx, d in enumerate(departments, start=1):
        rows.append(
            {
                "src_department_event_id": f"DEPT-HIST-{idx:06d}-1",
                "department_src_id": d.department_src_id,
                "department_name": d.department_name,
                "parent_department_src_id": d.parent_department_src_id or "",
                "block_name": d.block_name,
                "function_name": d.function_name,
                "region_name": d.region_name,
                "org_level": d.org_level,
                "manager_src_id": "",
                "effective_from": "2024-01-01",
                "effective_to": "",
                "source_system": "hr_department_history",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": batch_id,
            }
        )

        if random.random() < 0.25:
            rows.append(
                {
                    "src_department_event_id": f"DEPT-HIST-{idx:06d}-0",
                    "department_src_id": d.department_src_id,
                    "department_name": f"{d.department_name} (архив)",
                    "parent_department_src_id": d.parent_department_src_id or "",
                    "block_name": d.block_name,
                    "function_name": d.function_name,
                    "region_name": d.region_name,
                    "org_level": d.org_level,
                    "manager_src_id": "",
                    "effective_from": "2023-01-01",
                    "effective_to": "2023-12-31",
                    "source_system": "hr_department_history",
                    "load_dttm": dt_to_str(load_dttm),
                    "batch_id": batch_id,
                }
            )

    write_csv(
        output_dir / "hr_department_history_raw.csv",
        [
            "src_department_event_id",
            "department_src_id",
            "department_name",
            "parent_department_src_id",
            "block_name",
            "function_name",
            "region_name",
            "org_level",
            "manager_src_id",
            "effective_from",
            "effective_to",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        rows,
    )


def generate_employee_master_and_position_history(
    output_dir: Path,
    employees: list[Employee],
    departments: list[Department],
    batch_id: str,
    load_dttm: datetime,
) -> None:
    dept_map = {d.department_src_id: d for d in departments}
    employee_master_rows: list[dict] = []
    position_history_rows: list[dict] = []

    for idx, e in enumerate(employees, start=1):
        dept = dept_map.get(e.department_src_id, random.choice(departments))
        gender = random.choice(GENDERS)
        birth_date = date(1975, 1, 1) + timedelta(days=random.randint(0, 12000))
        work_format = random.choice(WORK_FORMATS)
        grade_code = random.choice(GRADES)

        history_cnt = random.randint(1, 4)
        effective_date = max(e.hire_date, date(2020, 1, 1))
        last_position_code = ""
        last_position_name = ""

        for h in range(1, history_cnt + 1):
            position_code, position_name = infer_position(dept.function_name)
            event_type_code = random.choice(["HIRE", "MOVE", "PROMOTION", "TRANSFER"])
            event_type_name = {
                "HIRE": "Приём",
                "MOVE": "Перемещение",
                "PROMOTION": "Повышение",
                "TRANSFER": "Перевод",
            }[event_type_code]

            end_date = None
            if h < history_cnt:
                end_date = effective_date + timedelta(days=random.randint(120, 420))

            position_history_rows.append(
                {
                    "src_position_event_id": f"POS-{idx:05d}-{h:02d}",
                    "employee_src_id": e.employee_src_id,
                    "position_code": position_code,
                    "position_name": position_name,
                    "department_src_id": dept.department_src_id,
                    "department_name": dept.department_name,
                    "event_type_code": event_type_code,
                    "event_type_name": event_type_name,
                    "grade_code": grade_code,
                    "salary_change_flag": "Y" if event_type_code in ("PROMOTION", "TRANSFER") else "N",
                    "effective_from": d_to_str(effective_date),
                    "effective_to": d_to_str(end_date),
                    "source_system": "hr_position_history",
                    "load_dttm": dt_to_str(load_dttm),
                    "batch_id": batch_id,
                }
            )

            last_position_code = position_code
            last_position_name = position_name

            if end_date:
                effective_date = end_date + timedelta(days=1)

        employee_master_rows.append(
            {
                "employee_src_id": e.employee_src_id,
                "employee_number": e.employee_number,
                "full_name": e.full_name,
                "tab_num": e.tab_num,
                "birth_date": d_to_str(birth_date),
                "gender_code": gender,
                "employment_status": e.employment_status,
                "hire_date": d_to_str(e.hire_date),
                "dismissal_date": d_to_str(e.dismissal_date),
                "current_position_code": last_position_code,
                "current_position_name": last_position_name,
                "department_src_id": dept.department_src_id,
                "department_name": dept.department_name,
                "manager_src_id": e.manager_src_id or "",
                "grade_code": grade_code,
                "work_format": work_format,
                "location_name": dept.region_name,
                "snapshot_date": "2024-12-31",
                "source_system": "hr_master",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": batch_id,
            }
        )

    write_csv(
        output_dir / "hr_employee_master_raw.csv",
        [
            "employee_src_id",
            "employee_number",
            "full_name",
            "tab_num",
            "birth_date",
            "gender_code",
            "employment_status",
            "hire_date",
            "dismissal_date",
            "current_position_code",
            "current_position_name",
            "department_src_id",
            "department_name",
            "manager_src_id",
            "grade_code",
            "work_format",
            "location_name",
            "snapshot_date",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        employee_master_rows,
    )

    write_csv(
        output_dir / "hr_position_history_raw.csv",
        [
            "src_position_event_id",
            "employee_src_id",
            "position_code",
            "position_name",
            "department_src_id",
            "department_name",
            "event_type_code",
            "event_type_name",
            "grade_code",
            "salary_change_flag",
            "effective_from",
            "effective_to",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        position_history_rows,
    )


def generate_absence_events(
    output_dir: Path,
    employees: list[Employee],
    batch_id: str,
    load_dttm: datetime,
) -> None:
    rows: list[dict] = []
    counter = 1

    for e in employees:
        event_cnt = random.choices([0, 1, 2, 3, 4], weights=[30, 28, 22, 14, 6], k=1)[0]
        for _ in range(event_cnt):
            code, name, group_name = random.choice(ABSENCE_TYPES)
            start = date(2024, 1, 1) + timedelta(days=random.randint(0, 340))
            duration = random.randint(1, 14) if code != "VACATION" else random.randint(7, 21)
            end = start + timedelta(days=duration - 1)

            rows.append(
                {
                    "src_absence_event_id": f"ABS-{counter:07d}",
                    "employee_src_id": e.employee_src_id,
                    "absence_type_code": code,
                    "absence_type_name": name,
                    "absence_reason_group": group_name,
                    "start_date": d_to_str(start),
                    "end_date": d_to_str(end),
                    "duration_days": duration,
                    "approved_flag": "Y" if random.random() < 0.9 else "N",
                    "source_system": "hr_absence",
                    "load_dttm": dt_to_str(load_dttm),
                    "batch_id": batch_id,
                }
            )
            counter += 1

    write_csv(
        output_dir / "hr_absence_events_raw.csv",
        [
            "src_absence_event_id",
            "employee_src_id",
            "absence_type_code",
            "absence_type_name",
            "absence_reason_group",
            "start_date",
            "end_date",
            "duration_days",
            "approved_flag",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        rows,
    )


def generate_overtime_events(
    output_dir: Path,
    employees: list[Employee],
    batch_id: str,
    load_dttm: datetime,
) -> None:
    rows: list[dict] = []
    counter = 1

    for e in employees:
        event_cnt = random.choices(
            [0, 1, 2, 3, 5, 8, 12],
            weights=[15, 18, 20, 18, 14, 10, 5],
            k=1,
        )[0]

        for _ in range(event_cnt):
            reason_code, reason_name = random.choice(OVERTIME_REASONS)
            overtime_date = date(2024, 1, 1) + timedelta(days=random.randint(0, 364))
            overtime_hours = round(random.uniform(1.0, 6.0), 2)

            rows.append(
                {
                    "src_overtime_event_id": f"OT-{counter:07d}",
                    "employee_src_id": e.employee_src_id,
                    "overtime_date": d_to_str(overtime_date),
                    "overtime_hours": overtime_hours,
                    "overtime_reason_code": reason_code,
                    "overtime_reason_name": reason_name,
                    "approved_flag": "Y" if random.random() < 0.85 else "N",
                    "source_system": "hr_overtime",
                    "load_dttm": dt_to_str(load_dttm),
                    "batch_id": batch_id,
                }
            )
            counter += 1

    write_csv(
        output_dir / "hr_overtime_events_raw.csv",
        [
            "src_overtime_event_id",
            "employee_src_id",
            "overtime_date",
            "overtime_hours",
            "overtime_reason_code",
            "overtime_reason_name",
            "approved_flag",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        rows,
    )


def generate_dismissal_signals(
    output_dir: Path,
    employees: list[Employee],
    batch_id: str,
    load_dttm: datetime,
) -> None:
    rows: list[dict] = []
    counter = 1
    start_dt = datetime(2024, 1, 1, 0, 0, 0)
    end_dt = datetime(2024, 12, 31, 23, 59, 59)

    riskier_pool = employees[: max(30, len(employees) // 8)]

    for _ in range(max(500, len(employees) // 2)):
        e = random.choice(riskier_pool if random.random() < 0.55 else employees)
        code, name, group_name = random.choice(DISMISSAL_SIGNALS)

        rows.append(
            {
                "src_signal_id": f"DS-{counter:07d}",
                "employee_src_id": e.employee_src_id,
                "signal_code": code,
                "signal_name": name,
                "signal_group": group_name,
                "signal_value_num": round(random.uniform(0.5, 5.0), 4),
                "signal_value_text": random.choice(
                    [
                        "Сигнал выявлен автоматически",
                        "Требует внимания HRBP",
                        "Подтверждён руководителем",
                        "Нужна дополнительная проверка",
                    ]
                ),
                "detected_at": dt_to_str(random_datetime(start_dt, end_dt)),
                "signal_status": random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "CLOSED", "PENDING"]),
                "source_system": "hr_attrition_monitor",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": batch_id,
            }
        )
        counter += 1

    write_csv(
        output_dir / "hr_dismissal_signals_raw.csv",
        [
            "src_signal_id",
            "employee_src_id",
            "signal_code",
            "signal_name",
            "signal_group",
            "signal_value_num",
            "signal_value_text",
            "detected_at",
            "signal_status",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        rows,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate raw data for HR product.")
    parser.add_argument("--output-dir", default="scripts/data_gen/output", help="Output directory for CSV files.")
    parser.add_argument("--seed", type=int, default=43, help="Random seed.")
    parser.add_argument("--fallback-departments", type=int, default=40, help="Departments if risk files are absent.")
    parser.add_argument("--fallback-employees", type=int, default=3000, help="Employees if risk files are absent.")
    parser.add_argument("--batch-id", default="hr_batch_001", help="Batch identifier.")
    args = parser.parse_args()

    random.seed(args.seed)

    output_dir = Path(args.output_dir)
    ensure_dir(output_dir)

    load_dttm = datetime(2024, 12, 31, 10, 0, 0)

    departments = load_risk_departments(output_dir)
    if not departments:
        departments = fallback_departments(args.fallback_departments)

    employees = load_risk_employees(output_dir)
    if not employees:
        employees = fallback_employees(args.fallback_employees, departments)

    generate_department_history(output_dir, departments, args.batch_id, load_dttm)
    generate_employee_master_and_position_history(output_dir, employees, departments, args.batch_id, load_dttm)
    generate_absence_events(output_dir, employees, args.batch_id, load_dttm)
    generate_overtime_events(output_dir, employees, args.batch_id, load_dttm)
    generate_dismissal_signals(output_dir, employees, args.batch_id, load_dttm)

    print("HR raw data generated successfully.")
    print(f"Output directory: {output_dir.resolve()}")
    print(f"Departments used: {len(departments)}")
    print(f"Employees used: {len(employees)}")


if __name__ == "__main__":
    main()