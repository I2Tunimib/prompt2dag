import json
import os
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from dagster import Config, In, Out, OpDefinition, Output, job, op, get_dagster_logger


class LoadAndModifyConfig(Config):
    """Configuration for the load_and_modify step."""

    data_dir: str = "/app/data"
    dataset_id: int = 2
    date_column: str = "Fecha_id"
    table_name_template: str = "JOT_{dataset_id}"


@op(
    out=Out(List[Path]),
    config_schema=LoadAndModifyConfig,
    description="Ingest CSV files and convert them to JSON format.",
)
def load_and_modify(context) -> List[Path]:
    cfg: LoadAndModifyConfig = context.op_config
    data_dir = Path(cfg.data_dir)
    logger = get_dagster_logger()

    if not data_dir.is_dir():
        raise FileNotFoundError(f"DATA_DIR not found: {data_dir}")

    json_paths: List[Path] = []
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("No CSV files found in %s", data_dir)

    for csv_path in csv_files:
        df = pd.read_csv(csv_path)
        # Ensure date column exists; if not, just continue
        if cfg.date_column not in df.columns:
            logger.warning(
                "Date column %s not found in %s; skipping date handling",
                cfg.date_column,
                csv_path.name,
            )
        # Convert to JSON
        json_name = f"table_data_{cfg.dataset_id}.json"
        json_path = data_dir / json_name
        df.to_json(json_path, orient="records", lines=True, force_ascii=False)
        json_paths.append(json_path)
        logger.info("Converted %s to %s", csv_path.name, json_path.name)

    return json_paths


class ReconciliationConfig(Config):
    """Configuration for the data reconciliation step."""

    data_dir: str = "/app/data"
    primary_column: str = "City"
    optional_columns: List[str] = ["County", "Country"]
    reconciliator_id: str = "geocodingHere"


@op(
    out=Out(List[Path]),
    ins={"json_paths": In(List[Path])},
    config_schema=ReconciliationConfig,
    description="Standardize and reconcile city names using a geocoding service.",
)
def data_reconciliation(context, json_paths: List[Path]) -> List[Path]:
    cfg: ReconciliationConfig = context.op_config
    data_dir = Path(cfg.data_dir)
    logger = get_dagster_logger()
    reconciled_paths: List[Path] = []

    for json_path in json_paths:
        with json_path.open("r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f]

        # Dummy reconciliation: just copy the primary column value to a new field
        for rec in records:
            city = rec.get(cfg.primary_column, "")
            rec["reconciled_city"] = city.upper()  # placeholder logic

        out_name = f"reconciled_table_{json_path.stem.split('_')[-1]}.json"
        out_path = data_dir / out_name
        with out_path.open("w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        reconciled_paths.append(out_path)
        logger.info("Reconciled %s -> %s", json_path.name, out_path.name)

    return reconciled_paths


class OpenMeteoConfig(Config):
    """Configuration for the OpenMeteo data extension step."""

    data_dir: str = "/app/data"
    date_separator: str = "-"  # placeholder for configurable format


@op(
    out=Out(List[Path]),
    ins={"json_paths": In(List[Path])},
    config_schema=OpenMeteoConfig,
    description="Enrich dataset with weather information.",
)
def open_meteo_extension(context, json_paths: List[Path]) -> List[Path]:
    cfg: OpenMeteoConfig = context.op_config
    data_dir = Path(cfg.data_dir)
    logger = get_dagster_logger()
    extended_paths: List[Path] = []

    for json_path in json_paths:
        with json_path.open("r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f]

        # Dummy weather data addition
        for rec in records:
            rec["apparent_temperature_max"] = 30.0
            rec["apparent_temperature_min"] = 15.0
            rec["precipitation_sum"] = 5.0
            rec["precipitation_hours"] = 2.0

        out_name = f"open_meteo_{json_path.stem.split('_')[-1]}.json"
        out_path = data_dir / out_name
        with out_path.open("w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        extended_paths.append(out_path)
        logger.info("Added OpenMeteo data %s -> %s", json_path.name, out_path.name)

    return extended_paths


class ColumnExtensionConfig(Config):
    """Configuration for the column extension step."""

    data_dir: str = "/app/data"
    extender_id: str = "reconciledColumnExt"


@op(
    out=Out(List[Path]),
    ins={"json_paths": In(List[Path])},
    config_schema=ColumnExtensionConfig,
    description="Append additional data properties to the dataset.",
)
def column_extension(context, json_paths: List[Path]) -> List[Path]:
    cfg: ColumnExtensionConfig = context.op_config
    data_dir = Path(cfg.data_dir)
    logger = get_dagster_logger()
    extended_paths: List[Path] = []

    for json_path in json_paths:
        with json_path.open("r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f]

        # Dummy column addition
        for idx, rec in enumerate(records, start=1):
            rec["id"] = idx
            rec["name"] = f"record_{idx}"

        out_name = f"column_extended_{json_path.stem.split('_')[-1]}.json"
        out_path = data_dir / out_name
        with out_path.open("w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        extended_paths.append(out_path)
        logger.info("Extended columns %s -> %s", json_path.name, out_path.name)

    return extended_paths


class SaveFinalConfig(Config):
    """Configuration for the final save step."""

    data_dir: str = "/app/data"


@op(
    ins={"json_paths": In(List[Path])},
    config_schema=SaveFinalConfig,
    description="Consolidate and export the fully enriched dataset as CSV.",
)
def save_final_data(context, json_paths: List[Path]) -> None:
    cfg: SaveFinalConfig = context.op_config
    data_dir = Path(cfg.data_dir)
    logger = get_dagster_logger()
    all_records: List[dict] = []

    for json_path in json_paths:
        with json_path.open("r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f]
            all_records.extend(records)

    if not all_records:
        logger.warning("No records to save.")
        return

    df = pd.DataFrame(all_records)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    csv_name = f"enriched_data_{timestamp}.csv"
    csv_path = data_dir / csv_name
    df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info("Saved final enriched data to %s", csv_path.name)


@job(description="Sequential data processing pipeline converting CSV to enriched CSV.")
def enrichment_pipeline():
    loaded = load_and_modify()
    reconciled = data_reconciliation(loaded)
    meteo_extended = open_meteo_extension(reconciled)
    column_extended = column_extension(meteo_extended)
    save_final_data(column_extended)


if __name__ == "__main__":
    result = enrichment_pipeline.execute_in_process()
    if result.success:
        print("Pipeline executed successfully.")
    else:
        print("Pipeline failed.")