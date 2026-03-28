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
    hire_date: date
    employment_status: str
    position_name: str
    department_src_id: str
    department_name: str


SYSTEMS = [
    ("AD", "Active Directory"),
    ("MAIL", "Corporate Mail"),
    ("VPN", "VPN Gateway"),
    ("ERP", "ERP Platform"),
    ("CRM", "CRM Platform"),
    ("DWH", "Data Warehouse"),
    ("FILE", "File Storage"),
]

SYSTEM_ROLE_MAP = {
    "AD": [
        ("AD_USER", "Пользователь домена", "BASE"),
        ("AD_ADMIN", "Администратор домена", "PRIVILEGED"),
        ("AD_SUPPORT", "Поддержка домена", "SUPPORT"),
    ],
    "MAIL": [
        ("MAIL_USER", "Почтовый пользователь", "BASE"),
        ("MAIL_POWER", "Расширенный почтовый доступ", "POWER"),
        ("MAIL_ADMIN", "Администратор почты", "PRIVILEGED"),
    ],
    "VPN": [
        ("VPN_USER", "VPN пользователь", "BASE"),
        ("VPN_EXT", "Расширенный VPN доступ", "POWER"),
        ("VPN_ADMIN", "Администратор VPN", "PRIVILEGED"),
    ],
    "ERP": [
        ("ERP_USER", "Пользователь ERP", "BASE"),
        ("ERP_APPROVER", "Согласующий ERP", "POWER"),
        ("ERP_ADMIN", "Администратор ERP", "PRIVILEGED"),
    ],
    "CRM": [
        ("CRM_USER", "Пользователь CRM", "BASE"),
        ("CRM_MANAGER", "Менеджер CRM", "POWER"),
        ("CRM_ADMIN", "Администратор CRM", "PRIVILEGED"),
    ],
    "DWH": [
        ("DWH_READER", "Чтение DWH", "BASE"),
        ("DWH_ANALYST", "Аналитик DWH", "POWER"),
        ("DWH_ADMIN", "Администратор DWH", "PRIVILEGED"),
    ],
    "FILE": [
        ("FILE_USER", "Пользователь файлового хранилища", "BASE"),
        ("FILE_EXT", "Расширенный файловый доступ", "POWER"),
        ("FILE_ADMIN", "Администратор файлового хранилища", "PRIVILEGED"),
    ],
}

ACCOUNT_TYPE_MAP = {
    "AD": ("DOMAIN", "Доменная учётная запись"),
    "MAIL": ("MAILBOX", "Почтовая учётная запись"),
    "VPN": ("REMOTE", "Удалённый доступ"),
    "ERP": ("BUSINESS", "Бизнес-учётная запись"),
    "CRM": ("BUSINESS", "Бизнес-учётная запись"),
    "DWH": ("ANALYTICS", "Аналитическая учётная запись"),
    "FILE": ("STORAGE", "Файловая учётная запись"),
}

PRIVILEGED_ACCESS_TYPES = [
    ("PIM_SESSION", "Сеанс привилегированного доступа"),
    ("BREAK_GLASS", "Аварийный привилегированный доступ"),
    ("DB_ADMIN", "Администрирование БД"),
]

FILE_OPERATION_TYPES = [
    ("READ", "Чтение файла"),
    ("DOWNLOAD", "Скачивание файла"),
    ("UPLOAD", "Загрузка файла"),
    ("DELETE", "Удаление файла"),
    ("SHARE", "Предоставление доступа"),
]

COUNTRIES = [
    ("Russia", "Moscow"),
    ("Russia", "Saint Petersburg"),
    ("Russia", "Yekaterinburg"),
    ("Kazakhstan", "Almaty"),
    ("Germany", "Berlin"),
    ("Turkey", "Istanbul"),
]

DESTINATIONS = [
    ("INTERNAL", "corp.local"),
    ("INTERNAL", "intranet.local"),
    ("EXTERNAL", "dropbox.com"),
    ("EXTERNAL", "drive.google.com"),
    ("EXTERNAL", "github.com"),
    ("EXTERNAL", "telegram.org"),
]


def dt_to_str(value: datetime | None) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else ""


def d_to_str(value: date | None) -> str:
    return value.strftime("%Y-%m-%d") if value else ""


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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
    hr_path = output_dir / "hr_employee_master_raw.csv"
    risk_path = output_dir / "risk_employee_registry_raw.csv"

    employees: list[Employee] = []

    if hr_path.exists():
        with hr_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                employees.append(
                    Employee(
                        employee_src_id=row["employee_src_id"],
                        employee_number=row["employee_number"],
                        full_name=row["full_name"],
                        tab_num=row["tab_num"],
                        hire_date=datetime.strptime(row["hire_date"], "%Y-%m-%d").date(),
                        employment_status=row["employment_status"],
                        position_name=row["current_position_name"],
                        department_src_id=row["department_src_id"],
                        department_name=row["department_name"],
                    )
                )
        return employees

    if risk_path.exists():
        with risk_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                employees.append(
                    Employee(
                        employee_src_id=row["employee_src_id"],
                        employee_number=row["employee_number"],
                        full_name=row["full_name"],
                        tab_num=row["tab_num"],
                        hire_date=datetime.strptime(row["hire_date"], "%Y-%m-%d").date(),
                        employment_status=row["employment_status"],
                        position_name=row["position_name"],
                        department_src_id=row["department_src_id"],
                        department_name=row["department_name"],
                    )
                )
        return employees

    raise FileNotFoundError(
        "Employees not found. Generate HR or risk raw data first."
    )


def choose_systems_for_employee() -> list[tuple[str, str]]:
    result = [("AD", "Active Directory")]

    if random.random() < 0.85:
        result.append(("MAIL", "Corporate Mail"))
    if random.random() < 0.55:
        result.append(("VPN", "VPN Gateway"))

    business_candidates = [("ERP", "ERP Platform"), ("CRM", "CRM Platform"), ("DWH", "Data Warehouse"), ("FILE", "File Storage")]
    random.shuffle(business_candidates)

    if random.random() < 0.60:
        result.append(business_candidates[0])
    if random.random() < 0.25:
        result.append(business_candidates[1])

    # убираем дубликаты
    uniq = []
    seen = set()
    for item in result:
        if item[0] not in seen:
            uniq.append(item)
            seen.add(item[0])

    return uniq


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate raw data for access product.")
    parser.add_argument("--output-dir", default="scripts/data_gen/output", help="Output directory for CSV files.")
    parser.add_argument("--seed", type=int, default=44, help="Random seed.")
    parser.add_argument("--batch-id", default="access_batch_001", help="Batch identifier.")
    args = parser.parse_args()

    random.seed(args.seed)

    output_dir = Path(args.output_dir)
    ensure_dir(output_dir)

    employees = load_employees(output_dir)
    load_dttm = datetime(2024, 12, 31, 11, 0, 0)
    year_start = datetime(2024, 1, 1, 0, 0, 0)
    year_end = datetime(2024, 12, 31, 23, 59, 59)

    accounts_rows: list[dict] = []
    role_rows: list[dict] = []
    priv_rows: list[dict] = []
    login_rows: list[dict] = []
    file_rows: list[dict] = []
    network_rows: list[dict] = []

    accounts: list[dict] = []

    account_counter = 1
    role_counter = 1
    priv_counter = 1
    login_counter = 1
    file_counter = 1
    network_counter = 1

    for e in employees:
        employee_systems = choose_systems_for_employee()

        for system_code, system_name in employee_systems:
            account_type_code, account_type_name = ACCOUNT_TYPE_MAP[system_code]
            created_at = datetime.combine(e.hire_date, datetime.min.time()) + timedelta(days=random.randint(0, 120))
            privileged_flag = "Y" if system_code in ("DWH", "ERP", "AD") and random.random() < 0.10 else "N"
            admin_flag = "Y" if privileged_flag == "Y" and random.random() < 0.35 else "N"

            account = {
                "src_account_id": f"ACC-{account_counter:07d}",
                "employee_src_id": e.employee_src_id,
                "system_code": system_code,
                "system_name": system_name,
                "account_login": f"{e.employee_src_id.lower()}_{system_code.lower()}",
                "account_type_code": account_type_code,
                "account_type_name": account_type_name,
                "privileged_flag": privileged_flag,
                "admin_flag": admin_flag,
                "account_status": "ACTIVE" if random.random() < 0.96 else "DISABLED",
                "created_at": dt_to_str(created_at),
                "disabled_at": "",
                "source_system": "iam_core",
                "load_dttm": dt_to_str(load_dttm),
                "batch_id": args.batch_id,
            }

            if account["account_status"] == "DISABLED":
                account["disabled_at"] = dt_to_str(created_at + timedelta(days=random.randint(60, 300)))

            accounts_rows.append(account)
            accounts.append(account)
            account_counter += 1

            possible_roles = SYSTEM_ROLE_MAP[system_code]
            role_cnt = random.randint(1, 2)
            if privileged_flag == "Y":
                role_cnt = random.randint(2, 3)

            selected_roles = possible_roles[:]
            random.shuffle(selected_roles)
            selected_roles = selected_roles[:role_cnt]

            if admin_flag == "Y":
                admin_roles = [r for r in possible_roles if r[2] == "PRIVILEGED"]
                if admin_roles and admin_roles[0] not in selected_roles:
                    selected_roles.append(admin_roles[0])

            seen_roles = set()
            for role_code, role_name, role_group in selected_roles:
                if role_code in seen_roles:
                    continue
                seen_roles.add(role_code)

                assigned_at = random_datetime(year_start, year_end - timedelta(days=30))
                revoked_at = ""
                status = "ACTIVE"
                if random.random() < 0.08:
                    revoked = assigned_at + timedelta(days=random.randint(10, 120))
                    revoked_at = dt_to_str(revoked)
                    status = "REVOKED"

                role_rows.append(
                    {
                        "src_role_assignment_id": f"ROLE-{role_counter:07d}",
                        "employee_src_id": e.employee_src_id,
                        "src_account_id": account["src_account_id"],
                        "system_code": system_code,
                        "role_code": role_code,
                        "role_name": role_name,
                        "role_group": role_group,
                        "assigned_at": dt_to_str(assigned_at),
                        "revoked_at": revoked_at,
                        "assignment_status": status,
                        "source_system": "iam_roles",
                        "load_dttm": dt_to_str(load_dttm),
                        "batch_id": args.batch_id,
                    }
                )
                role_counter += 1

            if privileged_flag == "Y" or admin_flag == "Y":
                for _ in range(random.randint(0, 2)):
                    if random.random() < 0.55:
                        access_type_code, access_type_name = random.choice(PRIVILEGED_ACCESS_TYPES)
                        start_dttm = random_datetime(year_start, year_end - timedelta(days=5))
                        duration_hours = random.randint(1, 8)
                        end_dttm = start_dttm + timedelta(hours=duration_hours)

                        priv_rows.append(
                            {
                                "src_priv_event_id": f"PRIV-{priv_counter:07d}",
                                "employee_src_id": e.employee_src_id,
                                "src_account_id": account["src_account_id"],
                                "system_code": system_code,
                                "access_type_code": access_type_code,
                                "access_type_name": access_type_name,
                                "request_id": f"REQ-{priv_counter:07d}",
                                "approved_flag": "Y" if random.random() < 0.85 else "N",
                                "start_dttm": dt_to_str(start_dttm),
                                "end_dttm": dt_to_str(end_dttm),
                                "access_status": "CLOSED" if random.random() < 0.80 else "ACTIVE",
                                "source_system": "pam_system",
                                "load_dttm": dt_to_str(load_dttm),
                                "batch_id": args.batch_id,
                            }
                        )
                        priv_counter += 1

            login_cnt = random.randint(2, 8)
            for _ in range(login_cnt):
                login_dttm = random_datetime(year_start, year_end)
                success = random.random() < 0.92
                country_name, city_name = random.choice(COUNTRIES)
                unusual_geo_flag = "Y" if country_name != "Russia" and random.random() < 0.80 else "N"

                login_rows.append(
                    {
                        "src_login_event_id": f"LOGIN-{login_counter:07d}",
                        "employee_src_id": e.employee_src_id,
                        "src_account_id": account["src_account_id"],
                        "system_code": system_code,
                        "login_dttm": dt_to_str(login_dttm),
                        "login_result": "SUCCESS" if success else "FAILED",
                        "auth_method": random.choice(["PASSWORD", "PASSWORD_MFA", "SSO"]),
                        "ip_address": f"10.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}",
                        "device_id": f"DEV-{random.randint(1000, 9999)}",
                        "country_name": country_name,
                        "city_name": city_name,
                        "unusual_geo_flag": unusual_geo_flag,
                        "source_system": "access_logs",
                        "load_dttm": dt_to_str(load_dttm),
                        "batch_id": args.batch_id,
                    }
                )
                login_counter += 1

            if system_code in ("MAIL", "DWH", "FILE", "CRM"):
                file_cnt = random.randint(0, 4)
                for _ in range(file_cnt):
                    op_code, op_name = random.choice(FILE_OPERATION_TYPES)
                    classification = random.choice(["PUBLIC", "INTERNAL", "CONFIDENTIAL", "STRICTLY_CONFIDENTIAL"])
                    download_flag = "Y" if op_code == "DOWNLOAD" else "N"
                    upload_flag = "Y" if op_code == "UPLOAD" else "N"
                    external_transfer_flag = "Y" if random.random() < 0.08 else "N"

                    file_rows.append(
                        {
                            "src_file_event_id": f"FILE-{file_counter:07d}",
                            "employee_src_id": e.employee_src_id,
                            "src_account_id": account["src_account_id"],
                            "system_code": system_code,
                            "operation_type_code": op_code,
                            "operation_type_name": op_name,
                            "file_classification": classification,
                            "operation_dttm": dt_to_str(random_datetime(year_start, year_end)),
                            "object_path": f"/data/{system_code.lower()}/folder_{random.randint(1, 50)}/file_{random.randint(1, 5000)}.dat",
                            "download_flag": download_flag,
                            "upload_flag": upload_flag,
                            "external_transfer_flag": external_transfer_flag,
                            "source_system": "dlp_monitor",
                            "load_dttm": dt_to_str(load_dttm),
                            "batch_id": args.batch_id,
                        }
                    )
                    file_counter += 1

            if system_code in ("VPN", "AD", "MAIL"):
                network_cnt = random.randint(1, 4)
                for _ in range(network_cnt):
                    destination_type, destination_name = random.choice(DESTINATIONS)
                    blocked_flag = "Y" if destination_type == "EXTERNAL" and random.random() < 0.06 else "N"

                    network_rows.append(
                        {
                            "src_network_event_id": f"NET-{network_counter:07d}",
                            "employee_src_id": e.employee_src_id,
                            "src_account_id": account["src_account_id"],
                            "system_code": system_code,
                            "event_dttm": dt_to_str(random_datetime(year_start, year_end)),
                            "destination_type": destination_type,
                            "destination_name": destination_name,
                            "traffic_mb": round(random.uniform(5, 850), 4),
                            "connection_count": random.randint(1, 20),
                            "blocked_flag": blocked_flag,
                            "vpn_used_flag": "Y" if system_code == "VPN" else ("N" if destination_type == "EXTERNAL" and random.random() < 0.12 else "Y"),
                            "source_system": "network_monitor",
                            "load_dttm": dt_to_str(load_dttm),
                            "batch_id": args.batch_id,
                        }
                    )
                    network_counter += 1

    write_csv(
        output_dir / "access_system_accounts_raw.csv",
        [
            "src_account_id",
            "employee_src_id",
            "system_code",
            "system_name",
            "account_login",
            "account_type_code",
            "account_type_name",
            "privileged_flag",
            "admin_flag",
            "account_status",
            "created_at",
            "disabled_at",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        accounts_rows,
    )

    write_csv(
        output_dir / "access_role_assignments_raw.csv",
        [
            "src_role_assignment_id",
            "employee_src_id",
            "src_account_id",
            "system_code",
            "role_code",
            "role_name",
            "role_group",
            "assigned_at",
            "revoked_at",
            "assignment_status",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        role_rows,
    )

    write_csv(
        output_dir / "access_privileged_access_raw.csv",
        [
            "src_priv_event_id",
            "employee_src_id",
            "src_account_id",
            "system_code",
            "access_type_code",
            "access_type_name",
            "request_id",
            "approved_flag",
            "start_dttm",
            "end_dttm",
            "access_status",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        priv_rows,
    )

    write_csv(
        output_dir / "access_login_events_raw.csv",
        [
            "src_login_event_id",
            "employee_src_id",
            "src_account_id",
            "system_code",
            "login_dttm",
            "login_result",
            "auth_method",
            "ip_address",
            "device_id",
            "country_name",
            "city_name",
            "unusual_geo_flag",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        login_rows,
    )

    write_csv(
        output_dir / "access_file_operations_raw.csv",
        [
            "src_file_event_id",
            "employee_src_id",
            "src_account_id",
            "system_code",
            "operation_type_code",
            "operation_type_name",
            "file_classification",
            "operation_dttm",
            "object_path",
            "download_flag",
            "upload_flag",
            "external_transfer_flag",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        file_rows,
    )

    write_csv(
        output_dir / "access_network_activity_raw.csv",
        [
            "src_network_event_id",
            "employee_src_id",
            "src_account_id",
            "system_code",
            "event_dttm",
            "destination_type",
            "destination_name",
            "traffic_mb",
            "connection_count",
            "blocked_flag",
            "vpn_used_flag",
            "source_system",
            "load_dttm",
            "batch_id",
        ],
        network_rows,
    )

    print("Access raw data generated successfully.")
    print(f"Output directory: {output_dir.resolve()}")
    print(f"Employees used: {len(employees)}")
    print(f"Accounts generated: {len(accounts_rows)}")
    print(f"Role assignments generated: {len(role_rows)}")
    print(f"Privileged access events generated: {len(priv_rows)}")
    print(f"Login events generated: {len(login_rows)}")
    print(f"File operations generated: {len(file_rows)}")
    print(f"Network events generated: {len(network_rows)}")


if __name__ == "__main__":
    main()