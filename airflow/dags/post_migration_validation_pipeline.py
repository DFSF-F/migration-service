from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from helpers.cloud_helper import required_env
from helpers.validation_helper import (
    bq_counts_by_dataset,
    bq_existing_tables_by_dataset,
    build_metadata_object_index,
    build_validation_result_row,
    build_validation_run_summary,
    delete_previous_results_for_run,
    ensure_validation_storage,
    insert_validation_check_results,
    load_domain_configs,
    object_ref_to_bq_target,
    resolve_object_ref,
    should_check_not_empty,
)


logger = logging.getLogger(__name__)


def get_validation_context(context: dict) -> dict[str, str]:
    dag_id = context["dag"].dag_id
    dag_run_id = context["dag_run"].run_id
    validation_run_id = f"{dag_id}__{dag_run_id}"

    return {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "validation_run_id": validation_run_id,
    }


def init_validation_storage(**context) -> None:
    validation_context = get_validation_context(context)

    logger.info(
        "Initializing validation storage. validation_run_id=%s",
        validation_context["validation_run_id"],
    )

    ensure_validation_storage()

    logger.info(
        "Deleting previous validation results for current run. validation_run_id=%s",
        validation_context["validation_run_id"],
    )

    delete_previous_results_for_run(validation_context["validation_run_id"])

    logger.info("Validation storage initialized successfully.")


def validate_raw_counts_from_metadata(**context) -> None:
    validation_context = get_validation_context(context)

    project_id = required_env("GCP_PROJECT_ID")
    location = required_env("GCP_REGION")

    logger.info(
        "Starting raw row count validation. project_id=%s, location=%s, validation_run_id=%s",
        project_id,
        location,
        validation_context["validation_run_id"],
    )

    pg_hook = PostgresHook(postgres_conn_id="greenplum_dwh")
    domain_configs = load_domain_configs()

    raw_targets = []
    raw_datasets = set()

    for product, config in domain_configs.items():
        metadata = config["metadata"]
        raw_objects = metadata.get("layers", {}).get("raw", []) or []

        for object_ref in raw_objects:
            target = object_ref_to_bq_target(product=product, object_ref=object_ref)
            raw_targets.append((product, object_ref, target))
            raw_datasets.add(target["dataset_id"])

    logger.info(
        "Prepared raw targets. raw_tables=%s raw_datasets=%s",
        len(raw_targets),
        sorted(raw_datasets),
    )

    logger.info("Loading existing BigQuery raw tables by dataset.")
    existing_tables = bq_existing_tables_by_dataset(
        project_id=project_id,
        dataset_ids=raw_datasets,
        location=location,
    )

    count_objects_by_dataset = {}
    for _, _, target in raw_targets:
        dataset_id = target["dataset_id"]
        table_id = target["table_id"]

        if table_id in existing_tables.get(dataset_id, set()):
            count_objects_by_dataset.setdefault(dataset_id, set()).add(table_id)

    logger.info(
        "Loading BigQuery raw counts in batches. datasets=%s",
        sorted(count_objects_by_dataset.keys()),
    )
    bq_counts = bq_counts_by_dataset(
        project_id=project_id,
        objects_by_dataset=count_objects_by_dataset,
        location=location,
    )

    results = []
    total_checks = 0
    failed_checks = 0

    for idx, (product, object_ref, target) in enumerate(raw_targets, start=1):
        source_table = object_ref
        dataset_id = target["dataset_id"]
        table_id = target["table_id"]
        target_object_name = target["object_name"]

        logger.info(
            "Raw count check [%s/%s]. product=%s source=%s target=%s",
            idx,
            len(raw_targets),
            product,
            source_table,
            target_object_name,
        )

        check_description = (
            "Сверка количества строк между raw-таблицей Greenplum "
            "и raw-таблицей BigQuery после миграции"
        )

        try:
            source_count = pg_hook.get_first(f"SELECT COUNT(*) FROM {source_table};")[0]
            target_exists = table_id in existing_tables.get(dataset_id, set())

            if not target_exists:
                is_correct = False
                target_value = "table_missing"

                logger.warning(
                    "Raw count failed: target table missing. source=%s target=%s source_count=%s",
                    source_table,
                    target_object_name,
                    source_count,
                )
            else:
                target_count = bq_counts.get((dataset_id, table_id))
                target_value = target_count
                is_correct = int(source_count) == int(target_count)

                logger.info(
                    "Raw count result. object=%s source_count=%s target_count=%s is_correct=%s",
                    target_object_name,
                    source_count,
                    target_count,
                    is_correct,
                )

            total_checks += 1
            failed_checks += 0 if is_correct else 1

            results.append(
                build_validation_result_row(
                    validation_run_id=validation_context["validation_run_id"],
                    dag_id=validation_context["dag_id"],
                    dag_run_id=validation_context["dag_run_id"],
                    product=product,
                    layer_name="raw",
                    object_name=target_object_name,
                    check_name="raw_row_count_match",
                    check_description=check_description,
                    source_system="Greenplum",
                    target_system="BigQuery",
                    source_value=source_count,
                    target_value=target_value,
                    is_correct=is_correct,
                    details=None if is_correct else f"Source={source_count}, target={target_value}",
                )
            )

        except Exception as e:
            total_checks += 1
            failed_checks += 1

            logger.exception(
                "Raw count check failed with exception. product=%s source=%s target=%s",
                product,
                source_table,
                target_object_name,
            )

            results.append(
                build_validation_result_row(
                    validation_run_id=validation_context["validation_run_id"],
                    dag_id=validation_context["dag_id"],
                    dag_run_id=validation_context["dag_run_id"],
                    product=product,
                    layer_name="raw",
                    object_name=target_object_name,
                    check_name="raw_row_count_match",
                    check_description=check_description,
                    source_system="Greenplum",
                    target_system="BigQuery",
                    source_value="query_failed",
                    target_value="query_failed",
                    is_correct=False,
                    details=str(e),
                )
            )

    logger.info(
        "Inserting raw validation results. rows=%s total_checks=%s failed_checks=%s",
        len(results),
        total_checks,
        failed_checks,
    )
    insert_validation_check_results(results)

    logger.info(
        "Finished raw row count validation. total_checks=%s failed_checks=%s",
        total_checks,
        failed_checks,
    )


def validate_objects_from_metadata(**context) -> None:
    validation_context = get_validation_context(context)

    project_id = required_env("GCP_PROJECT_ID")
    location = required_env("GCP_REGION")

    logger.info(
        "Starting metadata object validation. project_id=%s, location=%s, validation_run_id=%s",
        project_id,
        location,
        validation_context["validation_run_id"],
    )

    domain_configs = load_domain_configs()

    targets = []
    datasets = set()

    for product, config in domain_configs.items():
        metadata = config["metadata"]
        layers = metadata.get("layers", {})

        for layer_name in ("raw", "dds", "dm"):
            object_refs = layers.get(layer_name, []) or []

            for object_ref in object_refs:
                target = object_ref_to_bq_target(product=product, object_ref=object_ref)
                targets.append((product, layer_name, object_ref, target))
                datasets.add(target["dataset_id"])

    logger.info(
        "Prepared metadata validation targets. objects=%s datasets=%s",
        len(targets),
        sorted(datasets),
    )

    logger.info("Loading existing BigQuery tables by dataset.")
    existing_tables = bq_existing_tables_by_dataset(
        project_id=project_id,
        dataset_ids=datasets,
        location=location,
    )

    count_objects_by_dataset = {}
    for _, _, _, target in targets:
        dataset_id = target["dataset_id"]
        table_id = target["table_id"]

        if table_id in existing_tables.get(dataset_id, set()) and should_check_not_empty(table_id):
            count_objects_by_dataset.setdefault(dataset_id, set()).add(table_id)

    logger.info(
        "Loading BigQuery object row counts in batches. datasets=%s objects_for_count=%s",
        sorted(count_objects_by_dataset.keys()),
        sum(len(items) for items in count_objects_by_dataset.values()),
    )
    bq_counts = bq_counts_by_dataset(
        project_id=project_id,
        objects_by_dataset=count_objects_by_dataset,
        location=location,
    )

    results = []
    total_checks = 0
    failed_checks = 0

    for idx, (product, layer_name, object_ref, target) in enumerate(targets, start=1):
        dataset_id = target["dataset_id"]
        table_id = target["table_id"]
        object_name = target["object_name"]

        logger.info(
            "Metadata object validation [%s/%s]. product=%s layer=%s object=%s",
            idx,
            len(targets),
            product,
            layer_name,
            object_name,
        )

        exists = table_id in existing_tables.get(dataset_id, set())

        total_checks += 1
        failed_checks += 0 if exists else 1

        logger.info(
            "Object exists result. object=%s exists=%s",
            object_name,
            exists,
        )

        results.append(
            build_validation_result_row(
                validation_run_id=validation_context["validation_run_id"],
                dag_id=validation_context["dag_id"],
                dag_run_id=validation_context["dag_run_id"],
                product=product,
                layer_name=layer_name,
                object_name=object_name,
                check_name="metadata_object_exists",
                check_description="Проверка наличия объекта, заявленного в metadata.yml, в BigQuery",
                source_system="metadata.yml",
                target_system="BigQuery",
                source_value=object_ref,
                target_value="exists" if exists else "missing",
                is_correct=exists,
                details=None,
            )
        )

        if not exists:
            logger.warning("Skipping not_empty check because object does not exist. object=%s", object_name)
            continue

        if not should_check_not_empty(table_id):
            logger.info("Skipping not_empty check for view-like object. object=%s", object_name)
            continue

        row_count = bq_counts.get((dataset_id, table_id), 0)
        is_not_empty = row_count > 0

        total_checks += 1
        failed_checks += 0 if is_not_empty else 1

        logger.info(
            "Object not_empty result. object=%s row_count=%s is_correct=%s",
            object_name,
            row_count,
            is_not_empty,
        )

        results.append(
            build_validation_result_row(
                validation_run_id=validation_context["validation_run_id"],
                dag_id=validation_context["dag_id"],
                dag_run_id=validation_context["dag_run_id"],
                product=product,
                layer_name=layer_name,
                object_name=object_name,
                check_name="metadata_object_not_empty",
                check_description="Проверка, что объект, заявленный в metadata.yml, содержит данные",
                source_system="metadata.yml",
                target_system="BigQuery",
                source_value="row_count > 0",
                target_value=row_count,
                is_correct=is_not_empty,
                details=None,
            )
        )

    logger.info(
        "Inserting metadata object validation results. rows=%s total_checks=%s failed_checks=%s",
        len(results),
        total_checks,
        failed_checks,
    )
    insert_validation_check_results(results)

    logger.info(
        "Finished metadata object validation. total_checks=%s failed_checks=%s",
        total_checks,
        failed_checks,
    )


def validate_dependencies_from_metadata(**context) -> None:
    validation_context = get_validation_context(context)

    project_id = required_env("GCP_PROJECT_ID")
    location = required_env("GCP_REGION")

    logger.info(
        "Starting dependency validation. project_id=%s, location=%s, validation_run_id=%s",
        project_id,
        location,
        validation_context["validation_run_id"],
    )

    domain_configs = load_domain_configs()
    by_product_ref, by_ref = build_metadata_object_index(domain_configs)

    dependency_targets = []
    datasets = set()

    for product, config in domain_configs.items():
        dependencies = config["dependencies"]
        object_dependencies = dependencies.get("object_dependencies", []) or []

        for dependency_rule in object_dependencies:
            target_ref = dependency_rule.get("target")
            depends_on = dependency_rule.get("depends_on", []) or []

            resolved_target = resolve_object_ref(
                current_product=product,
                object_ref=target_ref,
                by_product_ref=by_product_ref,
                by_ref=by_ref,
            )

            if resolved_target:
                datasets.add(resolved_target["dataset_id"])

            dependency_targets.append(
                {
                    "product": product,
                    "target_ref": target_ref,
                    "depends_on": depends_on,
                    "resolved_target": resolved_target,
                }
            )

            for dependency_ref in depends_on:
                resolved_dependency = resolve_object_ref(
                    current_product=product,
                    object_ref=dependency_ref,
                    by_product_ref=by_product_ref,
                    by_ref=by_ref,
                )

                if resolved_dependency:
                    datasets.add(resolved_dependency["dataset_id"])

    logger.info(
        "Prepared dependency validation targets. rules=%s datasets=%s",
        len(dependency_targets),
        sorted(datasets),
    )

    logger.info("Loading existing BigQuery dependency tables by dataset.")
    existing_tables = bq_existing_tables_by_dataset(
        project_id=project_id,
        dataset_ids=datasets,
        location=location,
    )

    count_objects_by_dataset = {}

    for rule in dependency_targets:
        resolved_target = rule["resolved_target"]
        if resolved_target:
            dataset_id = resolved_target["dataset_id"]
            table_id = resolved_target["table_id"]

            if table_id in existing_tables.get(dataset_id, set()) and should_check_not_empty(table_id):
                count_objects_by_dataset.setdefault(dataset_id, set()).add(table_id)

        for dependency_ref in rule["depends_on"]:
            resolved_dependency = resolve_object_ref(
                current_product=rule["product"],
                object_ref=dependency_ref,
                by_product_ref=by_product_ref,
                by_ref=by_ref,
            )

            if not resolved_dependency:
                continue

            dataset_id = resolved_dependency["dataset_id"]
            table_id = resolved_dependency["table_id"]

            if table_id in existing_tables.get(dataset_id, set()) and should_check_not_empty(table_id):
                count_objects_by_dataset.setdefault(dataset_id, set()).add(table_id)

    logger.info(
        "Loading BigQuery dependency row counts in batches. datasets=%s objects_for_count=%s",
        sorted(count_objects_by_dataset.keys()),
        sum(len(items) for items in count_objects_by_dataset.values()),
    )
    bq_counts = bq_counts_by_dataset(
        project_id=project_id,
        objects_by_dataset=count_objects_by_dataset,
        location=location,
    )

    results = []
    total_checks = 0
    failed_checks = 0

    for rule_idx, rule in enumerate(dependency_targets, start=1):
        product = rule["product"]
        target_ref = rule["target_ref"]
        depends_on = rule["depends_on"]
        resolved_target = rule["resolved_target"]

        target_object_name = (
            resolved_target["object_name"] if resolved_target else f"unresolved.{target_ref}"
        )

        logger.info(
            "Dependency rule [%s/%s]. product=%s target=%s dependencies=%s",
            rule_idx,
            len(dependency_targets),
            product,
            target_ref,
            len(depends_on),
        )

        target_resolved = resolved_target is not None
        total_checks += 1
        failed_checks += 0 if target_resolved else 1

        results.append(
            build_validation_result_row(
                validation_run_id=validation_context["validation_run_id"],
                dag_id=validation_context["dag_id"],
                dag_run_id=validation_context["dag_run_id"],
                product=product,
                layer_name="dependencies",
                object_name=target_object_name,
                check_name="target_resolved_in_metadata",
                check_description="Проверка, что target из dependencies.yml описан в metadata.yml",
                source_system="dependencies.yml",
                target_system="metadata.yml",
                source_value=target_ref,
                target_value=target_object_name if resolved_target else "unresolved",
                is_correct=target_resolved,
                details=None,
            )
        )

        if resolved_target:
            target_exists = resolved_target["table_id"] in existing_tables.get(
                resolved_target["dataset_id"],
                set(),
            )

            total_checks += 1
            failed_checks += 0 if target_exists else 1

            logger.info(
                "Target exists result. target=%s exists=%s",
                resolved_target["object_name"],
                target_exists,
            )

            results.append(
                build_validation_result_row(
                    validation_run_id=validation_context["validation_run_id"],
                    dag_id=validation_context["dag_id"],
                    dag_run_id=validation_context["dag_run_id"],
                    product=product,
                    layer_name="dependencies",
                    object_name=resolved_target["object_name"],
                    check_name="target_exists_in_bigquery",
                    check_description="Проверка, что target из dependencies.yml существует в BigQuery",
                    source_system="dependencies.yml",
                    target_system="BigQuery",
                    source_value=target_ref,
                    target_value="exists" if target_exists else "missing",
                    is_correct=target_exists,
                    details=None,
                )
            )

        for dep_idx, dependency_ref in enumerate(depends_on, start=1):
            resolved_dependency = resolve_object_ref(
                current_product=product,
                object_ref=dependency_ref,
                by_product_ref=by_product_ref,
                by_ref=by_ref,
            )

            dependency_object_name = (
                resolved_dependency["object_name"]
                if resolved_dependency
                else f"unresolved.{dependency_ref}"
            )

            logger.info(
                "Dependency check [%s/%s]. target=%s dependency=%s resolved=%s",
                dep_idx,
                len(depends_on),
                target_ref,
                dependency_ref,
                dependency_object_name,
            )

            dependency_resolved = resolved_dependency is not None
            total_checks += 1
            failed_checks += 0 if dependency_resolved else 1

            results.append(
                build_validation_result_row(
                    validation_run_id=validation_context["validation_run_id"],
                    dag_id=validation_context["dag_id"],
                    dag_run_id=validation_context["dag_run_id"],
                    product=product,
                    layer_name="dependencies",
                    object_name=target_object_name,
                    check_name="dependency_resolved_in_metadata",
                    check_description="Проверка, что зависимость из dependencies.yml описана в metadata.yml",
                    source_system="dependencies.yml",
                    target_system="metadata.yml",
                    source_value=dependency_ref,
                    target_value=dependency_object_name if resolved_dependency else "unresolved",
                    is_correct=dependency_resolved,
                    details={"target": target_ref, "dependency": dependency_ref},
                )
            )

            if not resolved_dependency:
                continue

            dependency_exists = resolved_dependency["table_id"] in existing_tables.get(
                resolved_dependency["dataset_id"],
                set(),
            )

            total_checks += 1
            failed_checks += 0 if dependency_exists else 1

            results.append(
                build_validation_result_row(
                    validation_run_id=validation_context["validation_run_id"],
                    dag_id=validation_context["dag_id"],
                    dag_run_id=validation_context["dag_run_id"],
                    product=product,
                    layer_name="dependencies",
                    object_name=target_object_name,
                    check_name="dependency_exists_in_bigquery",
                    check_description="Проверка, что зависимость из dependencies.yml существует в BigQuery",
                    source_system="dependencies.yml",
                    target_system="BigQuery",
                    source_value=dependency_ref,
                    target_value="exists" if dependency_exists else "missing",
                    is_correct=dependency_exists,
                    details={
                        "target": target_ref,
                        "dependency": dependency_ref,
                        "resolved_dependency": resolved_dependency["object_name"],
                    },
                )
            )

            if not dependency_exists:
                continue

            if not should_check_not_empty(resolved_dependency["table_id"]):
                logger.info(
                    "Skipping dependency not_empty check for view-like object. dependency=%s",
                    resolved_dependency["object_name"],
                )
                continue

            dependency_count = bq_counts.get(
                (resolved_dependency["dataset_id"], resolved_dependency["table_id"]),
                0,
            )
            dependency_not_empty = dependency_count > 0

            total_checks += 1
            failed_checks += 0 if dependency_not_empty else 1

            results.append(
                build_validation_result_row(
                    validation_run_id=validation_context["validation_run_id"],
                    dag_id=validation_context["dag_id"],
                    dag_run_id=validation_context["dag_run_id"],
                    product=product,
                    layer_name="dependencies",
                    object_name=target_object_name,
                    check_name="dependency_not_empty",
                    check_description="Проверка, что зависимость из dependencies.yml содержит данные",
                    source_system="dependencies.yml",
                    target_system="BigQuery",
                    source_value=dependency_ref,
                    target_value=dependency_count,
                    is_correct=dependency_not_empty,
                    details={
                        "target": target_ref,
                        "dependency": dependency_ref,
                        "resolved_dependency": resolved_dependency["object_name"],
                    },
                )
            )

    logger.info(
        "Inserting dependency validation results. rows=%s total_checks=%s failed_checks=%s",
        len(results),
        total_checks,
        failed_checks,
    )
    insert_validation_check_results(results)

    logger.info(
        "Finished dependency validation. total_checks=%s failed_checks=%s",
        total_checks,
        failed_checks,
    )


def build_summary(**context) -> None:
    validation_context = get_validation_context(context)

    logger.info(
        "Building validation run summary. validation_run_id=%s",
        validation_context["validation_run_id"],
    )

    build_validation_run_summary(
        validation_run_id=validation_context["validation_run_id"],
        dag_id=validation_context["dag_id"],
        dag_run_id=validation_context["dag_run_id"],
    )

    logger.info("Validation run summary built successfully.")


with DAG(
    dag_id="post_migration_validation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["validation", "migration", "metadata", "bigquery", "greenplum"],
) as dag:
    start = EmptyOperator(task_id="start")

    init_storage = PythonOperator(
        task_id="init_validation_storage",
        python_callable=init_validation_storage,
    )

    validate_raw_counts = PythonOperator(
        task_id="validate_raw_counts_from_metadata",
        python_callable=validate_raw_counts_from_metadata,
    )

    validate_objects = PythonOperator(
        task_id="validate_objects_from_metadata",
        python_callable=validate_objects_from_metadata,
    )

    validate_dependencies = PythonOperator(
        task_id="validate_dependencies_from_metadata",
        python_callable=validate_dependencies_from_metadata,
    )

    build_run_summary = PythonOperator(
        task_id="build_validation_run_summary",
        python_callable=build_summary,
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> init_storage
        >> validate_raw_counts
        >> validate_objects
        >> validate_dependencies
        >> build_run_summary
        >> finish
    )