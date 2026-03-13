import logging
from pathlib import Path

import hydra
import joblib
import pandas as pd
from omegaconf import DictConfig
from sqlalchemy import create_engine

from nhl_match_prediction.modeling.models.logistic import prepare_data

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"


def load_upcoming_matches() -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{DB_PATH}")

    # query = """
    # SELECT *
    # FROM upcoming_match_features
    # """

    query = """
        SELECT *
        FROM match_features
        WHERE game_date >= "2026-01-01"
    """

    return pd.read_sql(query, engine)


@hydra.main(version_base=None, config_path="../../configs/modeling", config_name="config")
def main(cfg: DictConfig):
    root = Path(hydra.utils.get_original_cwd())

    logger = logging.getLogger("predict_logger")
    logger.setLevel(logging.INFO)

    logger.info("=== Prediction step ===")

    logger.info("Loading upcoming matches from SQL")

    df = load_upcoming_matches()

    meta_cols = [
        "game_id",
        "game_date",
        "home_team_abbr",
        "away_team_abbr",
    ]

    meta = df[meta_cols]

    x, _ = prepare_data(df)

    logger.info(f"Upcoming matches: {len(x)}")

    models = ["logistic", "random_forest"]

    results = meta.copy()

    for model_name in models:
        model_file = root / "logs" / model_name / "model.joblib"

        logger.info(f"Loading model {model_name} from {model_file}")

        model = joblib.load(model_file)

        proba = model.predict_proba(x)[:, 1]

        results[f"{model_name}_proba"] = proba

    results["avg_proba"] = results[[f"{m}_proba" for m in models]].mean(axis=1)

    output_dir = root / "predictions"
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / "upcoming_predictions.csv"

    results.to_csv(output_file, index=False)

    logger.info(f"Predictions saved to {output_file}")

    logger.info("=== Prediction step done ===")


if __name__ == "__main__":
    main()
