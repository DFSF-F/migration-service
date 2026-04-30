from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from helpers.cloud_helper import required_env, run_bq_query


DOMAIN_CONFIG_DIR = Path("/opt/airflow/project/sql")


def sql_string(value: object) -> str:
    if value is None:
        return "NULL"

    text = str(value)
    text = text.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def parse_bq_scalar(result: dict) -> str | None:
    rows = result.get("rows", [])
    if not rows:
        return None

    fields = rows[0].get("f", [])
    if not fields:
        return None

    return fields[0].get("v")


def parse_bq_rows(result: dict) -> list[list[str | None]]:
    rows = result.get("rows", []) or []
    parsed_rows: list[list[str | None]] = []

    for row in rows:
        fields = row.get("f", []) or []
        parsed_rows.append([field.get("v") for field in fields])

    return parsed_rows


def run_bq_scalar(query: str, location: str) -> str | None:
    result = run_bq_query(query=query, location=location)
    return parse_bq_scalar(result)


def load_yaml_file(file_path: Path) -> dict[str, Any]:
    if not file_path.exists():
        raise FileNotFoundError(f"YAML file not found: {file_path}")

    with file_path.open("r", encoding="utf-8") as file:
        payload = yaml.safe_load(file) or {}

    if not isinstance(payload, dict):
        raise ValueError(f"YAML file must contain a mapping object: {file_path}")

    return payload


def find_yaml_file(domain_dir: Path, base_name: str) -> Path | None:
    candidates = [
        domain_dir / f"{base_name}.yml",
        domain_dir / f"{base_name}.yaml",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return None


def discover_domain_dirs() -> list[Path]:
    if not DOMAIN_CONFIG_DIR.exists():
        raise FileNotFoundError(f"Domain config directory not found: {DOMAIN_CONFIG_DIR}")

    domain_dirs = []

    for item in sorted(DOMAIN_CONFIG_DIR.iterdir()):
        if not item.is_dir():
            continue

        metadata_path = find_yaml_file(item, "metadata")
        dependencies_path = find_yaml_file(item, "dependencies")

        if metadata_path and dependencies_path:
            domain_dirs.append(item)

    if not domain_dirs:
        existing = []
        for item in sorted(DOMAIN_CONFIG_DIR.iterdir()):
            if item.is_dir():
                existing.append(
                    {
                        "domain_dir": str(item),
                        "files": sorted(child.name for child in item.iterdir()),
                    }
                )

        raise RuntimeError(
            f"No domain metadata/dependencies files found in {DOMAIN_CONFIG_DIR}. "
            f"Existing domain directories: {existing}"
        )

    return domain_dirs


def load_domain_configs() -> dict[str, dict[str, Any]]:
    configs: dict[str, dict[str, Any]] = {}

    for domain_dir in discover_domain_dirs():
        metadata_path = find_yaml_file(domain_dir, "metadata")
        dependencies_path = find_yaml_file(domain_dir, "dependencies")

        if metadata_path is None:
            raise FileNotFoundError(f"metadata.yml or metadata.yaml not found in {domain_dir}")

        if dependencies_path is None:
            raise FileNotFoundError(f"dependencies.yml or dependencies.yaml not found in {domain_dir}")

        metadata = load_yaml_file(metadata_path)
        dependencies = load_yaml_file(dependencies_path)

        product = metadata.get("product")
        if not product:
            raise ValueError(f"Missing 'product' in {metadata_path}")

        if dependencies.get("product") != product:
            raise ValueError(
                f"Product mismatch between {metadata_path} and {dependencies_path}"
            )

        configs[product] = {
            "product": product,
            "metadata": metadata,
            "dependencies": dependencies,
            "domain_dir": str(domain_dir),
            "metadata_path": str(metadata_path),
            "dependencies_path": str(dependencies_path),
        }

    return configs


def parse_object_ref(object_ref: str) -> tuple[str, str]:
    parts = object_ref.split(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid object reference: {object_ref}")

    layer_name, object_name = parts
    if layer_name not in {"raw", "dds", "dm"}:
        raise ValueError(f"Unsupported layer '{layer_name}' in object reference: {object_ref}")

    if not object_name:
        raise ValueError(f"Empty object name in object reference: {object_ref}")

    return layer_name, object_name


def object_ref_to_bq_target(product: str, object_ref: str) -> dict[str, str]:
    layer_name, object_name = parse_object_ref(object_ref)

    return {
        "product": product,
        "layer_name": layer_name,
        "object_ref": object_ref,
        "dataset_id": f"{product}_{layer_name}",
        "table_id": object_name,
        "object_name": f"{product}_{layer_name}.{object_name}",
    }


def build_metadata_object_index(
    domain_configs: dict[str, dict[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, str]], dict[str, list[dict[str, str]]]]:
    by_product_ref: dict[tuple[str, str], dict[str, str]] = {}
    by_ref: dict[str, list[dict[str, str]]] = {}

    for product, config in domain_configs.items():
        metadata = config["metadata"]
        layers = metadata.get("layers", {})

        if not isinstance(layers, dict):
            raise ValueError(f"Invalid 'layers' section for product '{product}'")

        for layer_name in ("raw", "dds", "dm"):
            object_refs = layers.get(layer_name, []) or []

            if not isinstance(object_refs, list):
                raise ValueError(
                    f"Invalid layer '{layer_name}' in metadata.yml for product '{product}'"
                )

            for object_ref in object_refs:
                parsed_layer, _ = parse_object_ref(object_ref)
                if parsed_layer != layer_name:
                    raise ValueError(
                        f"Object '{object_ref}' is placed in wrong layer '{layer_name}' "
                        f"for product '{product}'"
                    )

                target = object_ref_to_bq_target(product=product, object_ref=object_ref)

                by_product_ref[(product, object_ref)] = target
                by_ref.setdefault(object_ref, []).append(target)

    return by_product_ref, by_ref


def resolve_object_ref(
    current_product: str,
    object_ref: str,
    by_product_ref: dict[tuple[str, str], dict[str, str]],
    by_ref: dict[str, list[dict[str, str]]],
) -> dict[str, str] | None:
    same_product_target = by_product_ref.get((current_product, object_ref))
    if same_product_target:
        return same_product_target

    candidates = by_ref.get(object_ref, [])
    if len(candidates) == 1:
        return candidates[0]

    return None


def should_check_not_empty(table_id: str) -> bool:
    return not table_id.startswith("v_")


def ensure_validation_storage() -> None:
    project_id = required_env("GCP_PROJECT_ID")
    location = required_env("GCP_REGION")

    create_dataset_sql = f"""
    CREATE SCHEMA IF NOT EXISTS `{project_id}.migration_meta`
    OPTIONS(location = "{location}");
    """

    create_check_results_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.migration_meta.validation_check_results` (
        validation_run_id STRING NOT NULL,
        dag_id STRING NOT NULL,
        dag_run_id STRING NOT NULL,
        validation_dttm TIMESTAMP NOT NULL,

        product STRING NOT NULL,
        layer_name STRING NOT NULL,
        object_name STRING NOT NULL,

        check_name STRING NOT NULL,
        check_description STRING,

        source_system STRING,
        target_system STRING,

        source_value STRING,
        target_value STRING,

        is_correct BOOL NOT NULL,

        details STRING
    );
    """

    create_summary_sql = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.migration_meta.validation_run_summary` (
        validation_run_id STRING NOT NULL,
        dag_id STRING NOT NULL,
        dag_run_id STRING NOT NULL,
        validation_dttm TIMESTAMP NOT NULL,

        total_checks INT64 NOT NULL,
        passed_checks INT64 NOT NULL,
        failed_checks INT64 NOT NULL,

        is_correct BOOL NOT NULL
    );
    """

    create_last_failed_view_sql = f"""
    CREATE OR REPLACE VIEW `{project_id}.migration_meta.v_last_failed_validation_checks` AS
    WITH last_run AS (
        SELECT validation_run_id
        FROM `{project_id}.migration_meta.validation_run_summary`
        ORDER BY validation_dttm DESC
        LIMIT 1
    )
    SELECT r.*
    FROM `{project_id}.migration_meta.validation_check_results` r
    JOIN last_run l
      ON r.validation_run_id = l.validation_run_id
    WHERE r.is_correct = FALSE
    ORDER BY
        r.product,
        r.layer_name,
        r.object_name,
        r.check_name;
    """

    run_bq_query(create_dataset_sql, location=location)
    run_bq_query(create_check_results_sql, location=location)
    run_bq_query(create_summary_sql, location=location)
    run_bq_query(create_last_failed_view_sql, location=location)


def build_validation_result_row(
    validation_run_id: str,
    dag_id: str,
    dag_run_id: str,
    product: str,
    layer_name: str,
    object_name: str,
    check_name: str,
    check_description: str | None,
    source_system: str | None,
    target_system: str | None,
    source_value: object,
    target_value: object,
    is_correct: bool,
    details: object = None,
) -> dict[str, object]:
    if isinstance(details, (dict, list)):
        details_value = json.dumps(details, ensure_ascii=False)
    elif details is None:
        details_value = None
    else:
        details_value = str(details)

    return {
        "validation_run_id": validation_run_id,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "product": product,
        "layer_name": layer_name,
        "object_name": object_name,
        "check_name": check_name,
        "check_description": check_description,
        "source_system": source_system,
        "target_system": target_system,
        "source_value": source_value,
        "target_value": target_value,
        "is_correct": is_correct,
        "details": details_value,
    }


def insert_validation_check_results(rows: list[dict[str, object]], batch_size: int = 250) -> None:
    if not rows:
        return

    project_id = required_env("GCP_PROJECT_ID")
    location = required_env("GCP_REGION")

    for start_idx in range(0, len(rows), batch_size):
        batch = rows[start_idx : start_idx + batch_size]

        values_sql = []
        for row in batch:
            values_sql.append(
                "("
                f"{sql_string(row['validation_run_id'])}, "
                f"{sql_string(row['dag_id'])}, "
                f"{sql_string(row['dag_run_id'])}, "
                "CURRENT_TIMESTAMP(), "
                f"{sql_string(row['product'])}, "
                f"{sql_string(row['layer_name'])}, "
                f"{sql_string(row['object_name'])}, "
                f"{sql_string(row['check_name'])}, "
                f"{sql_string(row['check_description'])}, "
                f"{sql_string(row['source_system'])}, "
                f"{sql_string(row['target_system'])}, "
                f"{sql_string(row['source_value'])}, "
                f"{sql_string(row['target_value'])}, "
                f"{str(bool(row['is_correct'])).upper()}, "
                f"{sql_string(row['details'])}"
                ")"
            )

        query = f"""
        INSERT INTO `{project_id}.migration_meta.validation_check_results` (
            validation_run_id,
            dag_id,
            dag_run_id,
            validation_dttm,

            product,
            layer_name,
            object_name,

            check_name,
            check_description,

            source_system,
            target_system,

            source_value,
            target_value,

            is_correct,
            details
        )
        VALUES
            {", ".join(values_sql)};
        """

        run_bq_query(query=query, location=location)


def bq_existing_tables_by_dataset(
    project_id: str,
    dataset_ids: set[str],
    location: str,
) -> dict[str, set[str]]:
    existing: dict[str, set[str]] = {}

    for dataset_id in sorted(dataset_ids):
        query = f"""
        SELECT table_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        """

        result = run_bq_query(query=query, location=location)
        rows = parse_bq_rows(result)

        existing[dataset_id] = {row[0] for row in rows if row and row[0]}

    return existing


def bq_counts_by_dataset(
    project_id: str,
    objects_by_dataset: dict[str, set[str]],
    location: str,
    union_batch_size: int = 40,
) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = {}

    for dataset_id, table_ids in sorted(objects_by_dataset.items()):
        sorted_tables = sorted(table_ids)

        for start_idx in range(0, len(sorted_tables), union_batch_size):
            batch = sorted_tables[start_idx : start_idx + union_batch_size]

            selects = []
            for table_id in batch:
                selects.append(
                    f"""
                    SELECT
                        {sql_string(dataset_id)} AS dataset_id,
                        {sql_string(table_id)} AS table_id,
                        COUNT(*) AS row_count
                    FROM `{project_id}.{dataset_id}.{table_id}`
                    """
                )

            query = "\nUNION ALL\n".join(selects)

            result = run_bq_query(query=query, location=location)
            rows = parse_bq_rows(result)

            for row in rows:
                if len(row) < 3:
                    continue

                row_dataset_id = row[0]
                row_table_id = row[1]
                row_count = int(row[2] or 0)

                counts[(row_dataset_id, row_table_id)] = row_count

    return counts


def delete_previous_results_for_run(validation_run_id: str) -> None:
    project_id = required_env("GCP_PROJECT_ID")
    location = required_env("GCP_REGION")

    delete_details_sql = f"""
    DELETE FROM `{project_id}.migration_meta.validation_check_results`
    WHERE validation_run_id = {sql_string(validation_run_id)};
    """

    delete_summary_sql = f"""
    DELETE FROM `{project_id}.migration_meta.validation_run_summary`
    WHERE validation_run_id = {sql_string(validation_run_id)};
    """

    run_bq_query(delete_details_sql, location=location)
    run_bq_query(delete_summary_sql, location=location)


def build_validation_run_summary(validation_run_id: str, dag_id: str, dag_run_id: str) -> None:
    project_id = required_env("GCP_PROJECT_ID")
    location = required_env("GCP_REGION")

    query = f"""
    INSERT INTO `{project_id}.migration_meta.validation_run_summary` (
        validation_run_id,
        dag_id,
        dag_run_id,
        validation_dttm,
        total_checks,
        passed_checks,
        failed_checks,
        is_correct
    )
    SELECT
        {sql_string(validation_run_id)} AS validation_run_id,
        {sql_string(dag_id)} AS dag_id,
        {sql_string(dag_run_id)} AS dag_run_id,
        CURRENT_TIMESTAMP() AS validation_dttm,
        COUNT(*) AS total_checks,
        COUNTIF(is_correct = TRUE) AS passed_checks,
        COUNTIF(is_correct = FALSE) AS failed_checks,
        COUNTIF(is_correct = FALSE) = 0 AS is_correct
    FROM `{project_id}.migration_meta.validation_check_results`
    WHERE validation_run_id = {sql_string(validation_run_id)};
    """

    run_bq_query(query=query, location=location)