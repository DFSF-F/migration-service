from __future__ import annotations

import argparse
import csv
import random
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
    position_name: str
    department_src_id: str
    department_name: str
    manager_src_id: str | None
    employment_status: str
    hire_date: date
    dismissal_date: date | None


FIRST_NAMES = [
    "Иван", "Анна", "Максим", "Ольга", "Даниил", "Мария", "Алексей", "Елена",
    "Павел", "Наталья", "Сергей", "Виктория", "Андрей", "Татьяна", "Михаил",
]
LAST_NAMES = [
    "Иванов", "Петров", "Сидоров", "Кузнецов", "Смирнов", "Фролов", "Попов",
    "Васильев", "Соколов", "Морозов", "Волков", "Лебедев", "Козлов",
]
MIDDLE_NAMES = [
    "Иванович", "Сергеевич", "Андреевич", "Павлович", "Олегович", "Игоревич",
    "Викторовна", "Павловна", "Сергеевна", "Игоревна", "Андреевна",
]

POSITIONS = [
    "Ведущий специалист", "Старший аналитик", "Специалист", "Менеджер",
    "Главный эксперт", "Операционный менеджер", "Специалист по контролю",
]

REGIONS = ["Москва", "Санкт-Петербург", "Екатеринбург", "Новосибирск", "Казань"]

BLOCKS = [
    ("Корпоративный блок", "Безопасность"),
    ("Корпоративный блок", "Комплаенс"),
    ("Бизнес-блок", "Продажи"),
    ("Операционный блок", "Операции"),
    ("Технологический блок", "ИТ"),
]

IB_INCIDENT_TYPES = [
    ("USB_COPY", "Копирование данных на внешний носитель", "HIGH"),
    ("PHISH_CLICK", "Переход по фишинговой ссылке", "MEDIUM"),
    ("UNAUTH_UPLOAD", "Попытка несанкционированной выгрузки", "CRITICAL"),
    ("PASSWORD_SHARE", "Передача пароля третьему лицу", "HIGH"),
    ("POLICY_MAIL", "Отправка служебных данных на внешнюю почту", "HIGH"),
]

SECURITY_INCIDENT_TYPES = [
    ("PHYS_ACCESS", "Нарушение физического доступа"),
    ("DOC_LEAK", "Подозрение на утечку документов"),
    ("BADGE_MISUSE", "Некорректное использование пропуска"),
    ("THIRD_PARTY", "Подозрительное взаимодействие с внешним лицом"),
]

COMPLIANCE_INCIDENT_TYPES = [
    ("COI", "Конфликт интересов", "ETHICS", "HIGH"),
    ("POLICY_BREAK", "Нарушение внутренней политики", "POLICY", "MEDIUM"),
    ("GIFT_POLICY", "Нарушение политики подарков", "ANTI_FRAUD", "LOW"),
    ("DECLARATION_GAP", "Неактуальные декларационные сведения", "DISCLOSURE", "MEDIUM"),
]

NONWORK_ACTIVITY_TYPES = [
    ("EXT_JOB", "Внешняя занятость", "EMPLOYMENT"),
    ("GAMBLING", "Азартные игры", "BEHAVIOR"),
    ("DEBT_LOAD", "Повышенная долговая нагрузка", "FINANCIAL"),
    ("EXT_AFFILIATION", "Внешняя аффилированность", "AFFILIATION"),
]


def dt_to_str(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else ""


def d_to_str(value: date | None) -> str:
    return value.strftime("%Y-%m-%d") if value else ""


def random_datetime(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = int(delta.total_seconds())
    return start + timedelta(seconds=random.randint(0, seconds))


def build_departments(count: int) -> list[Department]:
    departments: list[Department] = []
    for i in range(1, count + 1):
        block_name, function_name = random.choice(BLOCKS)
        region = random.choice(REGIONS)
        dept_id = f"D{i:04d}"
        parent_id = None if i <= 5 else f"D{random.randint(1, max(1, i // 3)):04d}"
        org_level = 1 if parent_id is None else random.choice([2, 3])
        departments.append(
            Department(
                department_src_id=dept_id,
                department_name=f"{function_name} {i}",
                parent_department_src_id=parent_id,
                block_name=block_name,
                function_name=function_name,
                region_name=region,
                org_level=org_level,
            )
        )
    return departments


def build_full_name() -> str:
    return f"{random.choice(LAST_NAMES)} {random.choice(FIRST_NAMES)} {random.choice(MIDDLE_NAMES)}"


def build_employees(count: int, departments: list[Department]) -> list[Employee]:
    employees: list[Employee] = []
    for i in range(1, count + 1):
        dept = random.choice(departments)
        employee_id = f"E{i:05d}"
        hire_date = date(2019, 1, 1) + timedelta(days=random.randint(0, 2200))
        dismissal_date = None
        status = "ACTIVE"
        if random.random() < 0.05:
            status = "DISMISSED"
            dismissal_date = hire_date + timedelta(days=random.randint(100, 1000))
        manager_src_id = None if i <= 20 else f"E{random.randint(1, i - 1):05d}"
        employees.append(
            Employee(
                employee_src_id=employee_id,
                employee_number=f"EMP-{i:05d}",
                full_name=build_full_name(),
                tab_num=f"T{i:05d}",
                position_name=f"{random.choice(POSITIONS)} {dept.function_name}",
                department_src_id=dept.department_src_id,
                department_name=dept.department_name,
                manager_src_id=manager_src_id,
                employment_status=status,
                hire_date=hire_date,
                dismissal_date=dismissal_date,
            )
        )
    return employees


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=",")
        writer.writeheader()
        writer.writerows(rows)


def generate_org_structure(output_dir: Path, departments: list[Department], batch_id: str, load_dttm: datetime) -> None:
    rows = []
    snapshot_date = date(2024, 12, 31)
    for d in departments:
        rows.append(
            {
                "department_src_id": d.department_src_id,
                "department_name": d.department_name,
                "parent_department_src_id": d.parent_department_src_id or "",
                "block_name": d.block_name,
                "function_name": d.function_name,
                "region_name": d.region_name,
                "org_level": d.org_level,
                "valid_from": "2024-01-01",
                "valid_to": "",
                "snapshot_date": d_to_str(snapshot_date),
                "source_system": "hr_org",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": batch_id,
            }
        )

    write_csv(
        output_dir / "risk_org_structure_raw.csv",
        [
            "department_src_id", "department_name", "parent_department_src_id",
            "block_name", "function_name", "region_name", "org_level",
            "valid_from", "valid_to", "snapshot_date",
            "source_system", "load_dttm", "batch_id",
        ],
        rows,
    )


def generate_employee_registry(output_dir: Path, employees: list[Employee], batch_id: str, load_dttm: datetime) -> None:
    rows = []
    snapshot_date = date(2024, 12, 31)
    for e in employees:
        rows.append(
            {
                "employee_src_id": e.employee_src_id,
                "employee_number": e.employee_number,
                "full_name": e.full_name,
                "tab_num": e.tab_num,
                "position_name": e.position_name,
                "department_src_id": e.department_src_id,
                "department_name": e.department_name,
                "manager_src_id": e.manager_src_id or "",
                "employment_status": e.employment_status,
                "hire_date": d_to_str(e.hire_date),
                "dismissal_date": d_to_str(e.dismissal_date),
                "snapshot_date": d_to_str(snapshot_date),
                "source_system": "hr_core",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": batch_id,
            }
        )

    write_csv(
        output_dir / "risk_employee_registry_raw.csv",
        [
            "employee_src_id", "employee_number", "full_name", "tab_num",
            "position_name", "department_src_id", "department_name",
            "manager_src_id", "employment_status", "hire_date",
            "dismissal_date", "snapshot_date", "source_system",
            "load_dttm", "batch_id",
        ],
        rows,
    )


def weighted_employee_choice(employees: list[Employee]) -> Employee:
    # Небольшой перекос: часть сотрудников получает больше риск-событий
    riskier_pool = employees[: max(20, len(employees) // 10)]
    return random.choice(riskier_pool if random.random() < 0.35 else employees)


def generate_ib_incidents(output_dir: Path, employees: list[Employee], count: int, batch_id: str, load_dttm: datetime) -> None:
    rows = []
    start_dt = datetime(2024, 1, 1, 0, 0, 0)
    end_dt = datetime(2024, 12, 31, 23, 59, 59)
    for i in range(1, count + 1):
        e = weighted_employee_choice(employees)
        code, name, severity = random.choice(IB_INCIDENT_TYPES)
        detected = random_datetime(start_dt, end_dt)
        is_open = random.random() < 0.2
        closed = None if is_open else detected + timedelta(days=random.randint(0, 10), hours=random.randint(1, 12))
        rows.append(
            {
                "src_incident_id": f"IB-{i:07d}",
                "employee_src_id": e.employee_src_id,
                "incident_code": code,
                "incident_name": name,
                "severity_level": severity,
                "incident_status": "OPEN" if is_open else "CLOSED",
                "detected_at": dt_to_str(detected),
                "closed_at": dt_to_str(closed),
                "channel_name": random.choice(["DLP", "SOC", "MAIL_GW", "EDR"]),
                "source_system": "ib_incidents",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": batch_id,
            }
        )

    write_csv(
        output_dir / "risk_ib_incidents_raw.csv",
        [
            "src_incident_id", "employee_src_id", "incident_code", "incident_name",
            "severity_level", "incident_status", "detected_at", "closed_at",
            "channel_name", "source_system", "load_dttm", "batch_id",
        ],
        rows,
    )


def generate_security_incidents(output_dir: Path, employees: list[Employee], count: int, batch_id: str, load_dttm: datetime) -> None:
    rows = []
    start_dt = datetime(2024, 1, 1, 0, 0, 0)
    end_dt = datetime(2024, 12, 31, 23, 59, 59)
    for i in range(1, count + 1):
        e = weighted_employee_choice(employees)
        case_code, case_name = random.choice(SECURITY_INCIDENT_TYPES)
        registered = random_datetime(start_dt, end_dt)
        is_open = random.random() < 0.25
        resolved = None if is_open else registered + timedelta(days=random.randint(1, 12))
        confirmed = random.random() < 0.65
        rows.append(
            {
                "src_case_id": f"SEC-{i:07d}",
                "employee_src_id": e.employee_src_id,
                "case_type_code": case_code,
                "case_type_name": case_name,
                "risk_flag": "Y" if confirmed else "N",
                "case_status": "OPEN" if is_open else "CLOSED",
                "registered_at": dt_to_str(registered),
                "resolved_at": dt_to_str(resolved),
                "resolution_code": "" if is_open else ("CONFIRMED" if confirmed else "REJECTED"),
                "resolution_name": "" if is_open else ("Нарушение подтверждено" if confirmed else "Риск не подтвержден"),
                "source_system": "security_cases",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": batch_id,
            }
        )

    write_csv(
        output_dir / "risk_security_incidents_raw.csv",
        [
            "src_case_id", "employee_src_id", "case_type_code", "case_type_name",
            "risk_flag", "case_status", "registered_at", "resolved_at",
            "resolution_code", "resolution_name", "source_system", "load_dttm", "batch_id",
        ],
        rows,
    )


def generate_compliance_incidents(output_dir: Path, employees: list[Employee], count: int, batch_id: str, load_dttm: datetime) -> None:
    rows = []
    start_dt = datetime(2024, 1, 1, 0, 0, 0)
    end_dt = datetime(2024, 12, 31, 23, 59, 59)
    for i in range(1, count + 1):
        e = weighted_employee_choice(employees)
        code, name, control_area, materiality = random.choice(COMPLIANCE_INCIDENT_TYPES)
        detected = random_datetime(start_dt, end_dt)
        is_open = random.random() < 0.3
        decision_at = None if is_open else detected + timedelta(days=random.randint(2, 15))
        decision_text = "" if is_open else random.choice([
            "Требуется дополнительный контроль",
            "Назначено служебное замечание",
            "Нарушение урегулировано",
            "Требуется повторная проверка",
        ])
        rows.append(
            {
                "src_violation_id": f"CMP-{i:07d}",
                "employee_src_id": e.employee_src_id,
                "violation_code": code,
                "violation_name": name,
                "control_area": control_area,
                "materiality_level": materiality,
                "violation_status": "OPEN" if is_open else "CLOSED",
                "detected_at": dt_to_str(detected),
                "decision_at": dt_to_str(decision_at),
                "decision_text": decision_text,
                "source_system": "compliance_cases",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": batch_id,
            }
        )

    write_csv(
        output_dir / "risk_compliance_incidents_raw.csv",
        [
            "src_violation_id", "employee_src_id", "violation_code", "violation_name",
            "control_area", "materiality_level", "violation_status", "detected_at",
            "decision_at", "decision_text", "source_system", "load_dttm", "batch_id",
        ],
        rows,
    )


def generate_nonwork_activity(output_dir: Path, employees: list[Employee], count: int, batch_id: str, load_dttm: datetime) -> None:
    rows = []
    start_dt = datetime(2024, 1, 1, 0, 0, 0)
    end_dt = datetime(2024, 12, 31, 23, 59, 59)
    for i in range(1, count + 1):
        e = weighted_employee_choice(employees)
        code, name, group_name = random.choice(NONWORK_ACTIVITY_TYPES)
        detected = random_datetime(start_dt, end_dt)
        valid_from = detected.date()
        is_closed = random.random() < 0.25
        valid_to = valid_from + timedelta(days=random.randint(10, 80)) if is_closed else None
        rows.append(
            {
                "src_activity_id": f"NWA-{i:07d}",
                "employee_src_id": e.employee_src_id,
                "activity_type_code": code,
                "activity_type_name": name,
                "activity_group": group_name,
                "activity_status": "CLOSED" if is_closed else "ACTIVE",
                "detected_at": dt_to_str(detected),
                "valid_from": d_to_str(valid_from),
                "valid_to": d_to_str(valid_to),
                "comment_text": random.choice([
                    "Сигнал выявлен автоматическим мониторингом",
                    "Требуется дополнительная проверка",
                    "Признак учтён в риск-профиле сотрудника",
                    "Сигнал подтверждён службой контроля",
                ]),
                "source_system": "hr_risk_monitor",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": batch_id,
            }
        )

    write_csv(
        output_dir / "risk_nonwork_activity_raw.csv",
        [
            "src_activity_id", "employee_src_id", "activity_type_code", "activity_type_name",
            "activity_group", "activity_status", "detected_at", "valid_from", "valid_to",
            "comment_text", "source_system", "load_dttm", "batch_id",
        ],
        rows,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate raw data for risk product.")
    parser.add_argument("--output-dir", default="scripts/data_gen/output", help="Output directory for CSV files.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument("--departments", type=int, default=40, help="Number of departments.")
    parser.add_argument("--employees", type=int, default=3000, help="Number of employees.")
    parser.add_argument("--ib-incidents", type=int, default=20000, help="Number of IB incidents.")
    parser.add_argument("--security-incidents", type=int, default=12000, help="Number of security incidents.")
    parser.add_argument("--compliance-incidents", type=int, default=8000, help="Number of compliance incidents.")
    parser.add_argument("--nonwork-activities", type=int, default=6000, help="Number of nonwork activity records.")
    parser.add_argument("--batch-id", default="risk_batch_001", help="Batch identifier.")
    args = parser.parse_args()

    random.seed(args.seed)
    output_dir = Path(args.output_dir)
    ensure_dir(output_dir)

    load_dttm = datetime(2024, 12, 31, 9, 0, 0)

    departments = build_departments(args.departments)
    employees = build_employees(args.employees, departments)

    generate_org_structure(output_dir, departments, args.batch_id, load_dttm)
    generate_employee_registry(output_dir, employees, args.batch_id, load_dttm)
    generate_ib_incidents(output_dir, employees, args.ib_incidents, args.batch_id, load_dttm)
    generate_security_incidents(output_dir, employees, args.security_incidents, args.batch_id, load_dttm)
    generate_compliance_incidents(output_dir, employees, args.compliance_incidents, args.batch_id, load_dttm)
    generate_nonwork_activity(output_dir, employees, args.nonwork_activities, args.batch_id, load_dttm)

    print("Risk raw data generated successfully.")
    print(f"Output directory: {output_dir.resolve()}")


if __name__ == "__main__":
    main()