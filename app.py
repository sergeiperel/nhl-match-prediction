from datetime import date
from pathlib import Path

import joblib
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from redis import Redis

from nhl_match_prediction.etl_pipeline.get_upcoming_matches import get_upcoming_matches
from nhl_match_prediction.modeling.models.logistic import prepare_data

app = FastAPI()
client = Redis(host="db")

MODEL_PATH = Path(__file__).parent / "logs/logistic/model.joblib"
CONTENT_DIR = Path(__file__).parent / "content"
THRESHOLD = 0.5
model = joblib.load(MODEL_PATH)


app.mount("/js", StaticFiles(directory=CONTENT_DIR / "js"), name="js")


@app.get("/health")
def health():
    return {"message": "Healthy!"}


# @app.get("/")
# def test():
#     return {"message": "Hello, World!"}


@app.get("/api/ok")
def get_api():
    return {"API": "OK!"}


@app.get("/", response_class=HTMLResponse)
def home():
    with (CONTENT_DIR / "index.html").open(encoding="utf-8") as f:
        return f.read()


@app.get("/matches", response_class=HTMLResponse)
def matches_page():
    with (CONTENT_DIR / "matches.html").open(encoding="utf-8") as f:
        return f.read()


@app.get("/api/redis/put/{x}")
def put_redis(x: str):
    client.set(x, "1")
    return {"message": "OK!"}


@app.get("/get_upcoming_matches")
def get_upcoming_matches_api():
    df = get_upcoming_matches()
    return df.to_dict(orient="records")


@app.get("/predict_upcoming")
def predict_upcoming():
    df = get_upcoming_matches()
    if df.empty:
        return {"message": "No upcoming games"}

    df = df.rename(columns={"home_logo": "home_team_logo", "away_logo": "away_team_logo"})

    df["game_date"] = pd.to_datetime(df["game_date"], utc=True)
    df["game_date"] = df["game_date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    df = df.reset_index(drop=True)

    x, _ = prepare_data(df.drop(columns=["home_team_logo", "away_team_logo"]))
    probs = model.predict_proba(x)
    df["prediction_prob"] = probs[:, 1]
    df["prediction"] = (df["prediction_prob"] > THRESHOLD).astype(int)

    return df[
        [
            "game_id",
            "game_date",
            "home_team_abbr",
            "away_team_abbr",
            "home_team_logo",
            "away_team_logo",
            # "home_score",
            # "away_score",
            "prediction",
            "prediction_prob",
        ]
    ].to_dict(orient="records")


@app.get("/predict_today")
def predict_today():
    df = get_upcoming_matches()
    if df.empty:
        return {"message": "No upcoming games"}

    df = df.rename(columns={"home_logo": "home_team_logo", "away_logo": "away_team_logo"})
    df["game_date"] = pd.to_datetime(df["game_date"])

    today = pd.Timestamp(date.today())
    df_today = df[df["game_date"].dt.date == today.date()]

    if df_today.empty:
        return {"message": "No games today"}

    x, _ = prepare_data(df_today.drop(columns=["home_team_logo", "away_team_logo"]))
    probs = model.predict_proba(x)
    df_today["prediction_prob"] = probs[:, 1]
    df_today["prediction"] = (df_today["prediction_prob"] > THRESHOLD).astype(int)

    df["game_date"] = pd.to_datetime(df["game_date"], utc=True)
    df["game_date"] = df["game_date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return df_today[
        [
            "game_id",
            "game_date",
            "home_team_abbr",
            "away_team_abbr",
            "home_team_logo",
            "away_team_logo",
            # "home_score",
            # "away_score",
            "prediction",
            "prediction_prob",
        ]
    ].to_dict(orient="records")
