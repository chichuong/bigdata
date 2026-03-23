from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


OUTPUT_FILE = Path("data/output/fraud_predictions.jsonl")


@st.cache_data(ttl=5)
def load_predictions(path: Path) -> pd.DataFrame:
	if not path.exists() or path.stat().st_size == 0:
		return pd.DataFrame()
	return pd.read_json(path, orient="records", lines=True)


def render_auto_refresh(interval_seconds: int) -> None:
	# Browser-side reload keeps dashboard live during stream demo.
	components.html(
		f"""
		<script>
			setTimeout(function() {{ window.parent.location.reload(); }}, {interval_seconds * 1000});
		</script>
		""",
		height=0,
		width=0,
	)


def main() -> None:
	st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
	st.title("Capstone Demo: Real-time Fraud Detection Dashboard")
	st.caption("Kafka + Spark Structured Streaming + Machine Learning + Rule Engine")

	with st.sidebar:
		st.header("Live Controls")
		auto_refresh = st.toggle("Auto-refresh", value=True)
		refresh_interval = st.slider("Refresh interval (seconds)", min_value=2, max_value=20, value=5)
		if st.button("Refresh now"):
			st.cache_data.clear()

	if auto_refresh:
		render_auto_refresh(refresh_interval)

	df = load_predictions(OUTPUT_FILE)

	if df.empty:
		st.warning("No prediction data yet. Start Kafka, streaming consumer, then producer.")
		return

	total_tx = len(df)
	fraud_tx = int((df["final_flag"] == 1).sum())
	normal_tx = int((df["final_flag"] == 0).sum())
	fraud_rate = (fraud_tx / total_tx) * 100 if total_tx else 0

	c1, c2, c3 = st.columns(3)
	c1.metric("Total Transactions", f"{total_tx:,}")
	c2.metric("Fraud Count", f"{fraud_tx:,}")
	c3.metric("Fraud Rate", f"{fraud_rate:.2f}%")

	st.subheader("Fraud vs Normal")
	compare_df = pd.DataFrame(
		{
			"class": ["fraud", "normal"],
			"count": [fraud_tx, normal_tx],
		}
	)
	st.bar_chart(compare_df.set_index("class")["count"])

	st.subheader("Recent Fraud Transactions")
	fraud_df = df[df["final_flag"] == 1].sort_values("event_time", ascending=False)
	fraud_cols = [
		"event_time",
		"nameOrig",
		"nameDest",
		"type",
		"amount",
		"ml_prediction",
		"rule_flag",
		"fraud_reasons",
		"final_flag",
	]
	for col in fraud_cols:
		if col not in fraud_df.columns:
			fraud_df[col] = None
	st.dataframe(fraud_df[fraud_cols].head(100), use_container_width=True)

	st.subheader("Recent Stream Records")
	st.dataframe(df.sort_values("event_time", ascending=False).head(100), use_container_width=True)


if __name__ == "__main__":
	main()
