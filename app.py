from datetime import date
from pathlib import Path

import joblib
import pandas as pd
import shap
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from redis import Redis

from nhl_match_prediction.modeling.models.logistic import prepare_data
from nhl_match_prediction.upcoming_features.build_upcoming_matches import get_upcoming_matches
from nhl_match_prediction.visualization.daily_accuracy import load_data, prepare_predictions
from scripts.tasks import router as tasks_router

# INIT
app = FastAPI()
app.include_router(tasks_router)

client = Redis(host="redis")

MODEL_PATH = Path(__file__).parent / "logs/logistic/model2.joblib"
CONTENT_DIR = Path(__file__).parent / "content"
THRESHOLD = 0.5

model = joblib.load(MODEL_PATH)


# INIT
app = FastAPI()
app.include_router(tasks_router)

client = Redis(host="redis")

MODEL_PATH = Path(__file__).parent / "logs/logistic/model2.joblib"
CONTENT_DIR = Path(__file__).parent / "content"
THRESHOLD = 0.5

model = joblib.load(MODEL_PATH)


# SHAP INIT
sample_df = get_upcoming_matches()

if not sample_df.empty:
    sample_df = sample_df.rename(
        columns={"home_logo": "home_team_logo", "away_logo": "away_team_logo"}
    )
    x_sample, _ = prepare_data(sample_df.drop(columns=["home_team_logo", "away_team_logo"]))
    if not x_sample.empty:
        x_sample = x_sample.sample(min(50, len(x_sample)))
    else:
        raise ValueError("No data for SHAP background")
else:
    x_sample = pd.DataFrame()

model_estimator = model.named_steps["logreg"] if hasattr(model, "named_steps") else model

explainer = shap.LinearExplainer(model_estimator, x_sample)


# STATIC
app.mount("/js", StaticFiles(directory=CONTENT_DIR / "js"), name="js")


def predict_with_explain(df: pd.DataFrame):
    df = df.copy()

    x, _ = prepare_data(df.drop(columns=["home_team_logo", "away_team_logo"]))

    probs = model.predict_proba(x)[:, 1]
    shap_values = explainer.shap_values(x)

    results = []

    for i in range(len(df)):
        feature_impacts = dict(zip(x.columns, shap_values[i], strict=False))

        top_features = sorted(feature_impacts.items(), key=lambda x: abs(x[1]), reverse=True)[:10]

        total = sum(abs(v) for _, v in top_features) or 1

        explanation = [
            {
                "feature": f,
                "impact": float(v),
                "impact_percent": round(abs(v) / total * 100, 1),
                "direction": "positive" if v > 0 else "negative",
            }
            for f, v in top_features
        ]

        results.append(
            {
                "probability": float(probs[i]),
                "prediction": int(probs[i] > THRESHOLD),
                "explanation": explanation,
            }
        )

    return results


def format_response(df: pd.DataFrame, preds: list):
    output = []

    for i, row in df.iterrows():
        output.append(
            {
                "game_id": row["game_id"],
                "game_date": row["game_date"],
                "home_team_abbr": row["home_team_abbr"],
                "away_team_abbr": row["away_team_abbr"],
                "home_team_logo": row["home_team_logo"],
                "away_team_logo": row["away_team_logo"],
                "arena": row["arena"],
                "prediction": preds[i]["prediction"],
                "prediction_prob": preds[i]["probability"],
                "explanation": preds[i]["explanation"],
            }
        )

    return output


def prepare_matches(df: pd.DataFrame):
    df = df.rename(columns={"home_logo": "home_team_logo", "away_logo": "away_team_logo"})

    df["game_date"] = pd.to_datetime(df["game_date"], utc=True)
    df["game_date"] = df["game_date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return df.reset_index(drop=True)


# ROUTES
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

    df = prepare_matches(df)

    preds = predict_with_explain(df)

    return format_response(df, preds)


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

    df_today = prepare_matches(df_today)

    preds = predict_with_explain(df_today)

    return format_response(df_today, preds)


@app.get("/predict_feed")
def predict_feed():
    df = get_upcoming_matches()

    if df.empty:
        return {"message": "No upcoming games"}

    df = df.rename(columns={"home_logo": "home_team_logo", "away_logo": "away_team_logo"})
    df["game_date"] = pd.to_datetime(df["game_date"], utc=True)

    now = pd.Timestamp.utcnow()

    today = now.date()
    tomorrow = (now + pd.Timedelta(days=1)).date()

    # --- LIVE (начался, но считаем что ещё идёт)
    df_live = df[
        (df["game_date"].dt.date == today)
        & (df["game_date"] <= now)
        & (df["game_date"] >= now - pd.Timedelta(hours=3))
    ]

    # --- TODAY upcoming
    df_today_upcoming = df[(df["game_date"].dt.date == today) & (df["game_date"] > now)]

    # --- TOMORROW
    df_tomorrow = df[df["game_date"].dt.date == tomorrow]

    def process(df_part):
        if df_part.empty:
            return []

        df_part = prepare_matches(df_part)
        preds = predict_with_explain(df_part)
        return format_response(df_part, preds)

    return {
        "live": process(df_live),
        "today_upcoming": process(df_today_upcoming),
        "tomorrow": process(df_tomorrow),
    }


@app.get("/upcoming_preview")
def upcoming_preview():
    df = get_upcoming_matches()

    if df.empty:
        return []

    df = df.rename(columns={"home_logo": "home_team_logo", "away_logo": "away_team_logo"})
    df["game_date"] = pd.to_datetime(df["game_date"], utc=True)

    now = pd.Timestamp.utcnow()
    df = df[df["game_date"] > now]
    df = df.sort_values("game_date")

    df = df.head(2)
    if df.empty:
        return []

    df = prepare_matches(df)

    preds = predict_with_explain(df)

    return format_response(df, preds)


@app.get("/accuracy")
def get_accuracy():
    df = load_data()

    df["game_day"] = pd.to_datetime(df["game_day"])

    cutoff = df["game_day"].max() - pd.Timedelta(days=30)
    df = df[df["game_day"] >= cutoff]

    if df.empty:
        return {"accuracy": 0}

    df = prepare_predictions(df, THRESHOLD)

    accuracy = (df["correct"].sum() / len(df)) * 100

    return {"accuracy": round(accuracy, 1)}


@app.get("/subscribers")
def get_subscribers():
    subs = client.smembers("subscribers")
    return list(map(int, subs))


@app.post("/subscribe/{chat_id}")
def subscribe(chat_id: int):
    client.sadd("subscribers", chat_id)
    return {"status": "ok"}


@app.post("/unsubscribe/{chat_id}")
def unsubscribe(chat_id: int):
    client.srem("subscribers", chat_id)
    return {"status": "ok"}
