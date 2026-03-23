from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from src.model.predict_model import predict
from src.rules.rule_engine import apply_rules, load_blacklist


LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "event_time": "",
    "step": 0,
    "type": "PAYMENT",
    "amount": 0.0,
    "nameOrig": "unknown_source",
    "oldbalanceOrg": 0.0,
    "newbalanceOrig": 0.0,
    "nameDest": "unknown_dest",
    "oldbalanceDest": 0.0,
    "newbalanceDest": 0.0,
    "login_country": "VN",
    "user_home_country": "VN",
    "device_change": 0,
}


@dataclass
class FraudDetector:
    """Production-style detector that caches model and blacklist resources."""

    blacklist_accounts: set[str]
    large_amount_threshold: float = 200000.0

    @classmethod
    def build(cls) -> "FraudDetector":
        blacklist = load_blacklist()
        # Warm-up model loading at startup (model is cached by predict_model module).
        _ = predict(pd.DataFrame([REQUIRED_COLUMNS]))
        return cls(blacklist_accounts={str(v) for v in blacklist})

    def _sanitize_input(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        for col, default_value in REQUIRED_COLUMNS.items():
            if col not in result.columns:
                result[col] = default_value

        numeric_cols = [
            "step",
            "amount",
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
            "device_change",
        ]
        for col in numeric_cols:
            result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)

        result["event_time"] = pd.to_datetime(result["event_time"], errors="coerce")
        result["event_time"] = result["event_time"].fillna(pd.Timestamp("1970-01-01"))

        text_cols = ["type", "nameOrig", "nameDest", "login_country", "user_home_country"]
        for col in text_cols:
            result[col] = result[col].fillna("unknown").astype(str)

        result["device_change"] = result["device_change"].astype(int)
        return result

    def _fallback_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fallback path in case external rule engine fails at runtime."""
        result = df.copy()
        result["rule_blacklist"] = result["nameDest"].astype(str).isin(self.blacklist_accounts).astype(int)
        result["rule_large_amount"] = (result["amount"] > self.large_amount_threshold).astype(int)
        result["rule_rapid_transfers"] = 0

        work = result[["nameOrig", "event_time"]].copy()
        work = work.sort_values(["nameOrig", "event_time"])
        counts = pd.Series(0.0, index=work.index)
        for _, group_idx in work.groupby("nameOrig").groups.items():
            group = work.loc[group_idx]
            marker = pd.Series(1, index=group["event_time"])
            rolling_counts = marker.rolling("5min").sum().values
            counts.loc[group.index] = rolling_counts
        result.loc[counts.index, "rule_rapid_transfers"] = (counts >= 3).astype(int)

        result["rule_flag"] = (
            (result["rule_blacklist"] == 1)
            | (result["rule_large_amount"] == 1)
            | (result["rule_rapid_transfers"] == 1)
        ).astype(int)
        return result

    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        result = self._sanitize_input(df)

        try:
            result["ml_prediction"] = predict(result).astype(int)
        except Exception as exc:
            LOGGER.exception("ML prediction failed. Falling back to rule-only mode: %s", exc)
            result["ml_prediction"] = 0

        try:
            result = apply_rules(result, self.blacklist_accounts, self.large_amount_threshold)
        except Exception as exc:
            LOGGER.exception("Rule engine failed. Using fallback rules: %s", exc)
            result = self._fallback_rules(result)

        if "rule_flag" not in result.columns:
            result["rule_flag"] = 0

        result["final_flag"] = ((result["ml_prediction"] == 1) | (result["rule_flag"] == 1)).astype(int)
        result = self._attach_explanations(result)
        return result

    def _attach_explanations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create human-readable fraud reason columns for logs and dashboard."""
        result = df.copy()

        def reasons_for_row(row: pd.Series) -> str:
            reasons: list[str] = []
            if int(row.get("ml_prediction", 0)) == 1:
                reasons.append("ml_high_risk")
            if int(row.get("rule_blacklist", 0)) == 1:
                reasons.append("blacklist_account")
            if int(row.get("rule_large_amount", 0)) == 1:
                reasons.append("large_amount")
            if int(row.get("rule_rapid_transfers", 0)) == 1:
                reasons.append("rapid_repeated_transfers")
            if int(row.get("rule_unusual_login", 0)) == 1:
                reasons.append("unusual_login")
            if int(row.get("rule_high_risk", 0)) == 1:
                reasons.append("high_risk_type_plus_large_amount")
            return "|".join(reasons) if reasons else "none"

        result["fraud_reasons"] = result.apply(reasons_for_row, axis=1)
        result["explain_ml"] = result["ml_prediction"].apply(lambda x: "fraud" if int(x) == 1 else "normal")
        result["explain_rule"] = result["rule_flag"].apply(lambda x: "triggered" if int(x) == 1 else "not_triggered")
        result["explain_final"] = result["final_flag"].apply(lambda x: "fraud" if int(x) == 1 else "normal")
        return result


_DETECTOR: FraudDetector | None = None


def get_detector() -> FraudDetector:
    global _DETECTOR
    if _DETECTOR is None:
        _DETECTOR = FraudDetector.build()
    return _DETECTOR


def detect_fraud(df: pd.DataFrame) -> pd.DataFrame:
    return get_detector().detect(df)

if __name__ == "__main__":
    sample_data = pd.DataFrame([
        {
            "event_time": "2025-01-01 09:00:00",
            "type": "TRANSFER",
            "amount": 500000,
            "nameOrig": "C123",
            "nameDest": "M1979787155",
            "oldbalanceOrg": 700000,
            "newbalanceOrig": 200000,
            "oldbalanceDest": 0,
            "newbalanceDest": 500000,
            "login_country": "US",
            "user_home_country": "VN",
            "device_change": 1,
        },
        {
            "event_time": "2025-01-01 09:10:00",
            "type": "PAYMENT",
            "amount": 5000,
            "nameOrig": "C456",
            "nameDest": "M9999999999",
            "oldbalanceOrg": 10000,
            "newbalanceOrig": 5000,
            "oldbalanceDest": 1000,
            "newbalanceDest": 6000,
            "login_country": "VN",
            "user_home_country": "VN",
            "device_change": 0,
        }
    ])

    result = detect_fraud(sample_data)
    print(result[[
        "type", "amount", "nameDest",
        "ml_prediction", "rule_flag", "final_flag"
    ]])