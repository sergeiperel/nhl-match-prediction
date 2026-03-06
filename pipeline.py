import logging
from datetime import date, datetime
from pathlib import Path

import hydra
from omegaconf import DictConfig

from nhl_match_prediction.collector.collect_nhl_raw import collect_season
from nhl_match_prediction.collector.collect_standings import collect_standings
from nhl_match_prediction.etl_pipeline.build_match_features import build_match_features
from nhl_match_prediction.etl_pipeline.export_match_features import (
    main as export_match_features_main,
)
from nhl_match_prediction.etl_pipeline.json_to_csv import main as json_to_csv_main
from nhl_match_prediction.etl_pipeline.load_to_db import main as load_sqlite_main
from nhl_match_prediction.etl_pipeline.upcoming_match_features import upcoming_match_features
from nhl_match_prediction.features.build_features import build_play_by_play_dataset
from pipeline_runner import PipelineRunner


def setup_logging(level: str = "INFO"):
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    log_file = logs_dir / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, level))

    # ---- FILE (детальный лог)
    file_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(file_formatter)

    # ---- CONSOLE (чистый вывод)
    console_formatter = logging.Formatter("%(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)

    root.addHandler(file_handler)
    root.addHandler(console_handler)

    # подавляем шум библиотек
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("pandas").setLevel(logging.WARNING)


def str_to_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


@hydra.main(config_path="configs/collect_data", config_name="config", version_base=None)
def main(cfg: DictConfig):
    setup_logging(cfg.pipeline.log_level)
    logger = logging.getLogger(__name__)

    start_date = str_to_date(cfg.date.start)
    end_date = str_to_date(cfg.date.end)

    logger.info("================================================")
    logger.info(f"🚀 NHL Pipeline | {start_date} → {end_date}")
    logger.info("================================================")

    runner = PipelineRunner(fail_fast=cfg.pipeline.fail_fast)

    runner.start_pipeline()

    runner.run_step(
        name="Collect Raw Data",
        func=collect_season,
        enabled=cfg.steps.collect_raw,
        start_date=start_date,
        end_date=end_date,
    )

    runner.run_step(
        name="Collect Standings",
        func=collect_standings,
        enabled=cfg.steps.collect_standings,
        start_date=start_date,
        end_date=end_date,
    )

    runner.run_step(
        name="Build Features",
        func=build_play_by_play_dataset,
        enabled=cfg.steps.build_features,
    )

    runner.run_step(
        name="JSON → CSV",
        func=json_to_csv_main,
        enabled=cfg.steps.json_to_csv,
    )

    runner.run_step(
        name="Load to SQLite",
        func=load_sqlite_main,
        enabled=cfg.steps.load_sqlite,
    )

    runner.run_step(
        name="Build Match Features",
        func=build_match_features,
        enabled=cfg.steps.build_match_features,
    )

    runner.run_step(
        name="Build Upcoming Match Features",
        func=upcoming_match_features,
        enabled=cfg.steps.build_match_features,
    )

    runner.run_step(
        name="Match Features → CSV",
        func=export_match_features_main,
        enabled=cfg.steps.export_match_features,
    )

    logger.info("✅ Pipeline procedure finished")

    runner.finish_pipeline()


if __name__ == "__main__":
    main()
