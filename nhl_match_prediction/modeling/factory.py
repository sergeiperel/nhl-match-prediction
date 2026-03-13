from nhl_match_prediction.modeling.models.logistic import build_model as build_logistic
from nhl_match_prediction.modeling.models.random_forest import build_model as build_rf


def get_model(model_name: str, params: dict):
    if model_name == "logistic":
        return build_logistic(params)
    if model_name == "random_forest":
        return build_rf(params)
    raise ValueError(f"Unknown model: {model_name}")
