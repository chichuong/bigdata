import pandas as pd

BLACKLIST_PATH = "data/blacklist/blacklist_accounts.csv"

def load_blacklist(path=BLACKLIST_PATH):
    df_blacklist = pd.read_csv(path)
    return set(df_blacklist["account"].astype(str).tolist())

def _rapid_transfer_rule(df: pd.DataFrame, window_minutes: int = 5, min_count: int = 3) -> pd.Series:
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

    out = pd.Series(0, index=df.index)
    out.loc[flags.index] = flags.astype(int)
    return out


def apply_rules(df, blacklist_accounts, large_amount_threshold: float = 200000):
    result = df.copy()

    # Rule 1: receiver account appears in blacklist.
    result["rule_blacklist"] = result["nameDest"].astype(str).isin(blacklist_accounts).astype(int)

    # Rule 2: unusually large transaction.
    result["rule_large_amount"] = (result["amount"] > large_amount_threshold).astype(int)

    # Rule 3: risky transfer types.
    result["rule_risky_type"] = result["type"].isin(["TRANSFER", "CASH_OUT"]).astype(int)

    # Rule 4: unusual login pattern.
    has_login_cols = {"login_country", "user_home_country", "device_change"}.issubset(result.columns)
    if has_login_cols:
        login_mismatch = result["login_country"].astype(str) != result["user_home_country"].astype(str)
        device_change = result["device_change"].fillna(0).astype(int) == 1
        result["rule_unusual_login"] = (login_mismatch | device_change).astype(int)
    else:
        result["rule_unusual_login"] = 0

    # Rule 5: rapid repeated transfers from same source account.
    result["rule_rapid_transfers"] = _rapid_transfer_rule(result)

    result["rule_high_risk"] = (
        (result["rule_large_amount"] == 1)
        & (result["rule_risky_type"] == 1)
    ).astype(int)

    # Final rule flag if any high-risk rule is triggered.
    result["rule_flag"] = (
        (result["rule_blacklist"] == 1)
        | (result["rule_high_risk"] == 1)
        | (result["rule_unusual_login"] == 1)
        | (result["rule_rapid_transfers"] == 1)
    ).astype(int)

    return result

if __name__ == "__main__":
    blacklist = load_blacklist()

    sample_data = pd.DataFrame([
        {
            "event_time": "2025-01-01 09:00:00",
            "type": "TRANSFER",
            "amount": 500000,
            "nameOrig": "C123",
            "nameDest": "M1979787155",
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
            "login_country": "VN",
            "user_home_country": "VN",
            "device_change": 0,
        }
    ])

    result = apply_rules(sample_data, blacklist)
    print(result)