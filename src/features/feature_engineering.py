import pandas as pd


def _rapid_transfer_flag(df: pd.DataFrame, window_minutes: int = 5, min_count: int = 3) -> pd.Series:
    """Flag rapid repeated transfers from the same source account in a short time window."""
    if "event_time" not in df.columns or "nameOrig" not in df.columns:
        return pd.Series([0] * len(df), index=df.index)

    work = df[["nameOrig", "event_time"]].copy()
    work["event_time"] = pd.to_datetime(work["event_time"], errors="coerce")
    work = work.dropna(subset=["event_time"]).sort_values(["nameOrig", "event_time"])

    counts = pd.Series(0.0, index=work.index)
    for _, group_idx in work.groupby("nameOrig").groups.items():
        group = work.loc[group_idx].sort_values("event_time")
        marker = pd.Series(1, index=group["event_time"])
        rolling_counts = marker.rolling(f"{window_minutes}min").sum().values
        counts.loc[group.index] = rolling_counts

    flags = (counts >= min_count).astype(int)

    result = pd.Series(0, index=df.index)
    result.loc[flags.index] = flags.astype(int)
    return result


def create_features(df: pd.DataFrame, large_amount_threshold: float = 200000) -> pd.DataFrame:
    result = df.copy()

    result["balanceDiffOrig"] = result["oldbalanceOrg"] - result["newbalanceOrig"]
    result["balanceDiffDest"] = result["newbalanceDest"] - result["oldbalanceDest"]
    result["isLargeAmount"] = (result["amount"] > large_amount_threshold).astype(int)

    has_login_cols = {"login_country", "user_home_country", "device_change"}.issubset(result.columns)
    if has_login_cols:
        login_mismatch = result["login_country"].astype(str) != result["user_home_country"].astype(str)
        device_change = result["device_change"].fillna(0).astype(int) == 1
        result["isUnusualLogin"] = (login_mismatch | device_change).astype(int)
    else:
        result["isUnusualLogin"] = 0

    result["isRapidTransfer"] = _rapid_transfer_flag(result)
    return result