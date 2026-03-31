# nhl_match_prediction/modeling/train.py

import logging
from pathlib import Path

import hydra
import joblib
from omegaconf import DictConfig

from nhl_match_prediction.modeling.factory import get_model
from nhl_match_prediction.modeling.models.logistic import (
    evaluate_model,
    load_dataset,
    prepare_data,
    time_split,
)


@hydra.main(version_base=None, config_path="../../configs/modeling", config_name="config")
def main(cfg: DictConfig):
    root = Path(hydra.utils.get_original_cwd())
    log_dir = root / "logs" / cfg.model.name
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "train.log"

    logger = logging.getLogger("train_logger")
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    file_handler = logging.FileHandler(str(log_file.resolve()))
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info("=== Training step ===")

    data_path = root / cfg.data.path
    logger.info(f"Loading data from {data_path}")
    df = load_dataset(data_path)
    x, y = prepare_data(df)

    x_train, x_test, y_train, y_test = time_split(x, y, cfg.data.test_size)
    logger.info(f"Train size: {len(x_train)}, test size: {len(x_test)}")

    model = get_model(cfg.model.name, cfg.model.params)
    logger.info(f"Model: {cfg.model.name}, parameters: {cfg.model.params}")

    logger.info("Model training...")
    model.fit(x_train, y_train)

    logger.info("Model evaluation on test data...")
    metrics = evaluate_model(model, x_test, y_test)

    for metric_name, metric_value in metrics.items():
        logger.info(f"{metric_name}: {metric_value:.4f}")

    model_file = log_dir / "model2.joblib"
    joblib.dump(model, model_file)
    logger.info(f"Model saved to {model_file.resolve()}")

    logger.info("=== Training step done ===")


if __name__ == "__main__":
    main()
