from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]

LOG_PATH = ROOT / "logs" / "xg_model"
LOG_PATH.mkdir(parents=True, exist_ok=True)
PLOTS_PATH = LOG_PATH / "plots"
PLOTS_PATH.mkdir(parents=True, exist_ok=True)

PBP_PATH = ROOT / "data" / "raw" / "playbyplay"
PROCESSED_PATH = ROOT / "data" / "processed" / "xg"
MODEL_PATH = ROOT / "models" / "xg"

PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
MODEL_PATH.mkdir(parents=True, exist_ok=True)

# datasets
XG_DATASET_PATH = PROCESSED_PATH / "xg_team_dataset.csv"
XG_SHOTS_DATASET_PATH = PROCESSED_PATH / "xg_shots_dataset.csv"

# model
MODEL_FILE = MODEL_PATH / "xg_model_20260425_0357.cbm"
MODEL_META = MODEL_PATH / "xg_model_20260425_0357.json"

VALID_EVENTS = ["shot-on-goal", "goal", "missed-shot"]

SPLIT_DATE = "2023-04-11"

RANDOM_STATE = 42
