from __future__ import annotations

import argparse
import random
from pathlib import Path

import numpy as np
import pandas as pd


TX_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]
COUNTRIES = ["VN", "SG", "TH", "US", "JP", "KR"]


def _load_blacklist_accounts(path: str) -> list[str]:
    file_path = Path(path)
    if not file_path.exists():
        return []
    df = pd.read_csv(file_path)
    if "account" not in df.columns:
        return []
    return df["account"].dropna().astype(str).tolist()


def generate_synthetic_transactions(
    rows: int = 2000,
    seed: int = 42,
    blacklist_path: str = "data/blacklist/blacklist_accounts.csv",
) -> pd.DataFrame:
    random.seed(seed)
    np.random.seed(seed)

    blacklist_accounts = _load_blacklist_accounts(blacklist_path)
    if not blacklist_accounts:
        blacklist_accounts = ["M0000000001"]

    base_time = pd.Timestamp("2025-01-01 08:00:00")
    users = [f"C{100000 + i}" for i in range(300)]
    merchants = [f"M{200000 + i}" for i in range(400)] + blacklist_accounts

    records: list[dict] = []
    for i in range(rows):
        name_orig = random.choice(users)
        home_country = random.choices(COUNTRIES, weights=[0.75, 0.08, 0.06, 0.04, 0.04, 0.03], k=1)[0]

        is_blacklist_target = random.random() < 0.04
        name_dest = random.choice(blacklist_accounts) if is_blacklist_target else random.choice(merchants)

        tx_type = random.choices(TX_TYPES, weights=[0.38, 0.25, 0.2, 0.1, 0.07], k=1)[0]
        amount = round(float(np.random.lognormal(mean=10.1, sigma=0.8)), 2)
        old_org = round(amount + random.uniform(2000, 600000), 2)
        new_org = max(0.0, round(old_org - amount + random.uniform(-300, 300), 2))
        old_dest = round(random.uniform(0, 900000), 2)
        new_dest = round(old_dest + amount + random.uniform(-200, 200), 2)

        # Burst some transactions from one user to mimic rapid repeated transfers.
        if i % 180 in (0, 1, 2):
            name_orig = users[(i // 180) % len(users)]
            tx_type = "TRANSFER"
            amount = round(random.uniform(180000, 700000), 2)

        login_country = home_country if random.random() > 0.08 else random.choice([c for c in COUNTRIES if c != home_country])
        device_change = 1 if random.random() < 0.1 else 0

        event_time = base_time + pd.Timedelta(seconds=i * random.randint(10, 60))
        step = i + 1

        unusual_login = int((login_country != home_country) or (device_change == 1))
        large_amount = int(amount > 200000)
        rapid_transfer_proxy = int((tx_type in ["TRANSFER", "CASH_OUT"]) and (i % 180 in (0, 1, 2)))
        high_risk_type = int(tx_type in ["TRANSFER", "CASH_OUT"])

        fraud_score = (
            0.45 * is_blacklist_target
            + 0.25 * large_amount
            + 0.15 * unusual_login
            + 0.15 * rapid_transfer_proxy
            + 0.05 * high_risk_type
            + np.random.normal(0, 0.04)
        )
        is_fraud = int(fraud_score > 0.45)

        records.append(
            {
                "event_time": event_time,
                "step": step,
                "type": tx_type,
                "amount": amount,
                "nameOrig": name_orig,
                "oldbalanceOrg": old_org,
                "newbalanceOrig": new_org,
                "nameDest": name_dest,
                "oldbalanceDest": old_dest,
                "newbalanceDest": new_dest,
                "login_country": login_country,
                "user_home_country": home_country,
                "device_change": device_change,
                "isFraud": is_fraud,
            }
        )

    return pd.DataFrame(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a synthetic e-wallet transactions dataset.")
    parser.add_argument("--rows", type=int, default=2000, help="Number of synthetic transactions")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--output", default="data/raw/transactions_synthetic.csv", help="Output CSV path")
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = generate_synthetic_transactions(rows=args.rows, seed=args.seed)
    df.to_csv(output_path, index=False)
    print(f"Generated {len(df)} rows at {output_path}")
    print(df["isFraud"].value_counts(normalize=True).rename("ratio"))


if __name__ == "__main__":
    main()