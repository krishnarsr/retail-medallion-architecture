"""Command-line orchestration for the complete medallion pipeline."""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date

from src.common.config import ensure_directories, load_settings
from src.common.logging_utils import configure_logging
from src.generate_data import generate


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retail medallion lakehouse pipeline")
    parser.add_argument(
        "command",
        choices=("generate", "bronze", "silver", "gold", "run-all"),
        nargs="?",
        default="run-all",
    )
    parser.add_argument("--batch-date", default=date.today().isoformat())
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    log = logging.getLogger("pipeline")
    settings = load_settings()
    ensure_directories(settings)
    summary: dict = {"command": args.command, "batch_date": args.batch_date}

    if args.command in ("generate", "run-all"):
        summary["generated_batch"] = generate(settings, args.batch_date)
        log.info("Source generation complete")
    if args.command == "generate":
        print(json.dumps(summary, indent=2))
        return

    from src.common.spark import build_spark
    from src.pipelines import bronze, gold, silver

    spark = build_spark()
    try:
        if args.command in ("bronze", "run-all"):
            summary["bronze"] = bronze.run(spark, settings, args.batch_date)
            log.info("Bronze ingestion complete")
        if args.command in ("silver", "run-all"):
            summary["silver"] = silver.run(spark, settings)
            log.info("Silver transformation complete")
        if args.command in ("gold", "run-all"):
            summary["gold"] = gold.run(spark, settings)
            log.info("Gold marts complete")
        print(json.dumps(summary, indent=2, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
