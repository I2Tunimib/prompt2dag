import os
import json
import glob
from typing import List

import pandas as pd
from dagster import op, job, In, Out, Nothing, ConfigurableResource, ResourceDefinition, InitResourceContext, get_dagster_logger


class DataDirResource(ConfigurableResource):
    """Resource providing the path to the shared data directory."""

    data_dir: str = "./data"

    def get_path(self) -> str:
        return self.data_dir


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


@op(
    config_schema={
        "dataset_id": int,
        "date_column": str,
        "table_name_template": str,
    },
    required_resource_keys={"data_dir"},
    out=Out(List[str]),
)
def load_and_modify(context) -> List[str]:
    """Ingest CSV files and convert them to JSON format."""
    logger = get_dagster_logger()
    cfg = context.op_config
    data_dir = context.resources.data_dir.get_path()
    csv_pattern = os.path.join(data_dir, "*.csv")
    csv_files = glob.glob(csv_pattern)

    if not csv_files:
        logger.warning("No CSV files found in %s", data_dir)
        return []

    output_files = []
    for csv_path in csv_files:
        table_name = cfg["table_name_template"].format(os.path.splitext(os.path.basename(csv_path))[0])
        json_filename = f"table_data_{table_name}.json"
        json_path = os.path.join(data_dir, json_filename)

        try:
            df = pd.read_csv(csv_path)
            # Placeholder transformation: add dataset_id column
            df["dataset_id"] = cfg["dataset_id"]
            # Ensure date column exists
            if cfg["date_column"] not in df.columns:
                df[cfg["date_column"]] = pd.NaT
            df.to_json(json_path, orient="records", lines=True)
            output_files.append(json_path)
            logger.info("Converted %s to %s", csv_path, json_path)
        except Exception as exc:
            logger.error("Failed to process %s: %s", csv_path, exc)
            raise

    return output_files


@op(
    config_schema={
        "primary_column": str,
        "optional_columns": List[str],
        "reconciliator_id": str,
    },
    required_resource_keys={"data_dir"},
    ins={"input_files": In(List[str])},
    out=Out(List[str]),
)
def data_reconciliation(context, input_files: List[str]) -> List[str]:
    """Standardize city names using a placeholder reconciliation step."""
    logger = get_dagster_logger()
    cfg = context.op_config
    data_dir = context.resources.data_dir.get_path()

    output_files = []
    for json_path in input_files:
        base_name = os.path.splitext(os.path.basename(json_path))[0].replace("table_data_", "")
        out_name = f"reconciled_table_{base_name}.json"
        out_path = os.path.join(data_dir, out_name)

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                records = [json.loads(line) for line in f]

            # Placeholder reconciliation: uppercase primary column
            for rec in records:
                if cfg["primary_column"] in rec and isinstance(rec[cfg["primary_column"]], str):
                    rec[cfg["primary_column"]] = rec[cfg["primary_column"]].upper()
                for col in cfg["optional_columns"]:
                    if col in rec and isinstance(rec[col], str):
                        rec[col] = rec[col].title()

            with open(out_path, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")

            output_files.append(out_path)
            logger.info("Reconciled %s -> %s", json_path, out_path)
        except Exception as exc:
            logger.error("Reconciliation failed for %s: %s", json_path, exc)
            raise

    return output_files


@op(
    config_schema={
        "weather_attributes": List[str],
        "date_separator": str,
    },
    required_resource_keys={"data_dir"},
    ins={"input_files": In(List[str])},
    out=Out(List[str]),
)
def openmeteo_extension(context, input_files: List[str]) -> List[str]:
    """Enrich data with placeholder weather information."""
    logger = get_dagster_logger()
    cfg = context.op_config
    data_dir = context.resources.data_dir.get_path()

    output_files = []
    for json_path in input_files:
        base_name = os.path.splitext(os.path.basename(json_path))[0].replace("reconciled_table_", "")
        out_name = f"open_meteo_{base_name}.json"
        out_path = os.path.join(data_dir, out_name)

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                records = [json.loads(line) for line in f]

            # Add dummy weather attributes
            for rec in records:
                for attr in cfg["weather_attributes"]:
                    rec[attr] = None  # placeholder for real data

            with open(out_path, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")

            output_files.append(out_path)
            logger.info("Extended %s with weather data -> %s", json_path, out_path)
        except Exception as exc:
            logger.error("OpenMeteo extension failed for %s: %s", json_path, exc)
            raise

    return output_files


@op(
    config_schema={"extender_id": str},
    required_resource_keys={"data_dir"},
    ins={"input_files": In(List[str])},
    out=Out(List[str]),
)
def column_extension(context, input_files: List[str]) -> List[str]:
    """Append additional columns as a placeholder operation."""
    logger = get_dagster_logger()
    cfg = context.op_config
    data_dir = context.resources.data_dir.get_path()

    output_files = []
    for json_path in input_files:
        base_name = os.path.splitext(os.path.basename(json_path))[0].replace("open_meteo_", "")
        out_name = f"column_extended_{base_name}.json"
        out_path = os.path.join(data_dir, out_name)

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                records = [json.loads(line) for line in f]

            # Add dummy columns
            for rec in records:
                rec["id"] = rec.get("id", None)
                rec["name"] = rec.get("name", None)

            with open(out_path, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")

            output_files.append(out_path)
            logger.info("Column extended %s -> %s (extender %s)", json_path, out_path, cfg["extender_id"])
        except Exception as exc:
            logger.error("Column extension failed for %s: %s", json_path, exc)
            raise

    return output_files


@op(
    config_schema={"output_prefix": str},
    required_resource_keys={"data_dir"},
    ins={"input_files": In(List[str])},
    out=Out(Nothing),
)
def save_final_data(context, input_files: List[str]) -> Nothing:
    """Consolidate JSON files and write a final CSV."""
    logger = get_dagster_logger()
    cfg = context.op_config
    data_dir = context.resources.data_dir.get_path()
    output_csv = os.path.join(data_dir, f"{cfg['output_prefix']}_enriched_data.csv")

    all_records = []
    for json_path in input_files:
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                records = [json.loads(line) for line in f]
                all_records.extend(records)
            logger.info("Loaded %d records from %s", len(records), json_path)
        except Exception as exc:
            logger.error("Failed to read %s: %s", json_path, exc)
            raise

    if not all_records:
        logger.warning("No records to write to final CSV.")
        return Nothing

    try:
        df = pd.DataFrame(all_records)
        _ensure_dir(data_dir)
        df.to_csv(output_csv, index=False)
        logger.info("Final CSV written to %s (%d rows)", output_csv, len(df))
    except Exception as exc:
        logger.error("Failed to write final CSV: %s", exc)
        raise

    return Nothing


@job(
    resource_defs={"data_dir": ResourceDefinition.hardcoded_resource(DataDirResource(data_dir="./data"))}
)
def enrichment_pipeline():
    """Sequential job that runs the full enrichment pipeline."""
    loaded = load_and_modify()
    reconciled = data_reconciliation(loaded)
    extended_weather = openmeteo_extension(reconciled)
    extended_columns = column_extension(extended_weather)
    save_final_data(extended_columns)


if __name__ == "__main__":
    result = enrichment_pipeline.execute_in_process(
        run_config={
            "ops": {
                "load_and_modify": {
                    "config": {
                        "dataset_id": 2,
                        "date_column": "Fecha_id",
                        "table_name_template": "JOT_{}",
                    }
                },
                "data_reconciliation": {
                    "config": {
                        "primary_column": "City",
                        "optional_columns": ["County", "Country"],
                        "reconciliator_id": "geocodingHere",
                    }
                },
                "openmeteo_extension": {
                    "config": {
                        "weather_attributes": [
                            "apparent_temperature_max",
                            "apparent_temperature_min",
                            "precipitation_sum",
                            "precipitation_hours",
                        ],
                        "date_separator": "-",
                    }
                },
                "column_extension": {
                    "config": {"extender_id": "reconciledColumnExt"}
                },
                "save_final_data": {
                    "config": {"output_prefix": "enriched_data"}
                },
            }
        }
    )
    if result.success:
        print("Pipeline completed successfully.")
    else:
        print("Pipeline failed.")