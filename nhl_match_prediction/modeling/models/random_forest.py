# nhl_match_prediction/modeling/models/random_forest.py

from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline


def build_model(params: dict) -> Pipeline:
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("rf", RandomForestClassifier(**params)),
        ]
    )
