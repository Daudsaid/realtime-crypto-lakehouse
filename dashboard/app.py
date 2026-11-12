"""Streamlit dashboard for the gold candles: BTCUSDT and ETHUSDT, 1-minute OHLC and volatility.

Reads gold.ohlc_1m and gold.volatility_1m from Postgres, which the pipeline's serve job keeps up to
date. Deployed on Streamlit Community Cloud against a hosted Postgres (Neon); runs locally against
the Docker Postgres too.

Local run:  pip install -r dashboard/requirements.txt && streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import altair as alt
import pandas as pd
import psycopg
import streamlit as st

REFRESH_SECONDS = 30
WINDOW_HOURS = 3
LIVE_WITHIN_MINUTES = 3  # the newest candle closes a minute behind real time, plus one trigger
DISPLAY_TZ = ZoneInfo("Europe/London")

# Categorical slots 1 and 2 of the validated default palette; colour follows the symbol.
SYMBOL_COLORS = {"BTCUSDT": "#2a78d6", "ETHUSDT": "#eb6834"}
SYMBOL_NAMES = {"BTCUSDT": "Bitcoin (BTCUSDT)", "ETHUSDT": "Ether (ETHUSDT)"}

# Everything within WINDOW_HOURS of the newest candle: the live picture while the pipeline runs,
# the last known picture when it is offline, so the page never goes blank.
QUERY = """
    WITH newest AS (SELECT max(minute) AS ts FROM gold.ohlc_1m)
    SELECT o.symbol, o.minute, o.open, o.high, o.low, o.close, o.volume, o.quote_volume,
           o.trade_count, v.price_range, v.return_pct
    FROM gold.ohlc_1m o
    JOIN gold.volatility_1m v USING (symbol, minute)
    CROSS JOIN newest
    WHERE o.minute > newest.ts - make_interval(hours => %s)
    ORDER BY o.minute
"""


def get_setting(name: str, default: str | None = None) -> str | None:
    """Read a setting from Streamlit secrets (Community Cloud), else from the environment."""
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:
        pass  # no secrets.toml present
    return os.environ.get(name, default)


def connect() -> psycopg.Connection:
    url = get_setting("DATABASE_URL")
    if url:
        return psycopg.connect(url, connect_timeout=10, autocommit=True)
    return psycopg.connect(
        host=get_setting("POSTGRES_HOST", "localhost"),
        port=get_setting("POSTGRES_PORT", "5432"),
        dbname=get_setting("POSTGRES_DB", "crypto"),
        user=get_setting("POSTGRES_USER", "crypto"),
        password=get_setting("POSTGRES_PASSWORD", "crypto"),
        connect_timeout=10,
        autocommit=True,
    )


@st.cache_resource
def get_connection() -> psycopg.Connection:
    return connect()


def run_query() -> pd.DataFrame:
    with get_connection().cursor() as cur:
        cur.execute(QUERY, (WINDOW_HOURS,))
        columns = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def load_candles() -> pd.DataFrame:
    """Query the candles, reconnecting once if the cached connection was closed
    (hosted databases like Neon close idle connections)."""
    try:
        df = run_query()
    except (psycopg.OperationalError, psycopg.InterfaceError):
        get_connection.clear()
        df = run_query()
    if df.empty:
        return df
    numeric = ["open", "high", "low", "close", "volume", "quote_volume", "price_range", "return_pct"]
    df[numeric] = df[numeric].astype(float)
    # gold.minute is a UTC timestamp without time zone: label it UTC, then show London time.
    df["minute"] = pd.to_datetime(df["minute"]).dt.tz_localize("UTC").dt.tz_convert(DISPLAY_TZ)
    df["range_pct"] = df["price_range"] / df["close"] * 100
    return df


def color_scale(symbols: list[str]) -> alt.Scale:
    present = [s for s in SYMBOL_COLORS if s in symbols]
    return alt.Scale(domain=present, range=[SYMBOL_COLORS[s] for s in present])


def price_chart(df: pd.DataFrame, symbol: str) -> alt.Chart:
    """Close price for one symbol. BTC and ETH differ ~35x in price, so each gets its own chart."""
    data = df[df["symbol"] == symbol]
    hover = alt.selection_point(
        name=f"hover_{symbol.lower()}", fields=["minute"], nearest=True, on="pointerover", empty=False
    )
    base = alt.Chart(data).encode(
        x=alt.X("minute:T", title=None, axis=alt.Axis(format="%H:%M", grid=False)),
        y=alt.Y("close:Q", title="Close (USDT)", scale=alt.Scale(zero=False)),
    )
    line = base.mark_line(strokeWidth=2, color=SYMBOL_COLORS[symbol])
    points = base.mark_point(size=60, filled=True, color=SYMBOL_COLORS[symbol]).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0))
    )
    rule = (
        alt.Chart(data)
        .mark_rule(color="#8a8984", strokeWidth=1)
        .encode(
            x="minute:T",
            opacity=alt.condition(hover, alt.value(0.6), alt.value(0)),
            tooltip=[
                alt.Tooltip("minute:T", title="Minute", format="%d %b %H:%M"),
                alt.Tooltip("open:Q", format=",.2f"),
                alt.Tooltip("high:Q", format=",.2f"),
                alt.Tooltip("low:Q", format=",.2f"),
                alt.Tooltip("close:Q", format=",.2f"),
                alt.Tooltip("trade_count:Q", title="Trades", format=","),
            ],
        )
        .add_params(hover)
    )
    return (line + points + rule).properties(height=260, title=SYMBOL_NAMES.get(symbol, symbol))


def symbol_lines(
    df: pd.DataFrame, field: str, axis_title: str, value_format: str, title: str, name: str
) -> alt.Chart:
    """One 2px line per symbol on a shared axis, with a nearest-point hover tooltip."""
    symbols = sorted(df["symbol"].unique())
    hover = alt.selection_point(name=name, fields=["minute"], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(df).encode(
        x=alt.X("minute:T", title=None, axis=alt.Axis(format="%H:%M", grid=False)),
        y=alt.Y(f"{field}:Q", title=axis_title, axis=alt.Axis(format=value_format)),
        color=alt.Color("symbol:N", scale=color_scale(symbols), legend=alt.Legend(title=None, orient="top")),
    )
    lines = base.mark_line(strokeWidth=2)
    points = (
        base.mark_point(size=60, filled=True)
        .encode(
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[
                alt.Tooltip("symbol:N"),
                alt.Tooltip("minute:T", title="Minute", format="%d %b %H:%M"),
                alt.Tooltip(f"{field}:Q", title=axis_title, format=value_format),
            ],
        )
        .add_params(hover)
    )
    return (lines + points).properties(height=240, title=title)


def volume_chart(df: pd.DataFrame) -> alt.Chart:
    """Traded value (quote volume) per minute for each symbol."""
    return symbol_lines(
        df, "quote_volume", "Quote volume (USDT)", ",.3~s", "Traded value per minute", "hover_volume"
    )


def range_chart(df: pd.DataFrame) -> alt.Chart:
    """One-minute high-low range as % of price: comparable across symbols on one axis."""
    return symbol_lines(
        df, "range_pct", "Range (% of price)", ".3f", "Volatility: 1-minute price range", "hover_range"
    )


st.set_page_config(page_title="Crypto Market — Live", layout="wide")
st.title("📈 Crypto Market — Live")
st.caption("Binance → Kafka → Spark Structured Streaming → Delta Lake → Postgres → Streamlit")


@st.fragment(run_every=REFRESH_SECONDS)
def live_view() -> None:
    try:
        df = load_candles()
    except psycopg.Error as exc:
        st.error(f"Couldn't reach the database: {exc}")
        return

    if df.empty:
        st.info("No candles have been recorded yet. Check back later.")
        return

    newest = df["minute"].max()
    age = datetime.now(UTC) - newest.to_pydatetime()
    newest_label = newest.strftime("%d %b %Y, %H:%M")
    if age.total_seconds() <= LIVE_WITHIN_MINUTES * 60:
        st.success(f"🟢 Live. Newest candle {newest_label} (London time).")
    else:
        st.warning(
            f"⏸️ The pipeline is offline. Showing the last {WINDOW_HOURS} hours up to "
            f"**{newest_label}** (London time). The charts update automatically when it is back."
        )

    symbols = [s for s in SYMBOL_COLORS if s in set(df["symbol"])]
    columns = st.columns(len(symbols) * 2)
    for i, symbol in enumerate(symbols):
        data = df[df["symbol"] == symbol]
        first, last = data["close"].iloc[0], data["close"].iloc[-1]
        change = (last - first) / first * 100
        columns[2 * i].metric(SYMBOL_NAMES[symbol], f"{last:,.2f}", f"{change:+.2f}% over window")
        columns[2 * i + 1].metric(
            f"{symbol} trades",
            f"{int(data['trade_count'].sum()):,}",
            f"{data['range_pct'].mean():.3f}% avg 1-min range",
            delta_color="off",
        )

    price_columns = st.columns(len(symbols))
    for column, symbol in zip(price_columns, symbols, strict=True):
        column.altair_chart(price_chart(df, symbol), width="stretch")

    left, right = st.columns(2)
    left.altair_chart(volume_chart(df), width="stretch")
    right.altair_chart(range_chart(df), width="stretch")

    with st.expander("Data table"):
        table = df.sort_values(["minute", "symbol"], ascending=[False, True])
        st.dataframe(
            table[
                [
                    "minute",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "quote_volume",
                    "trade_count",
                    "range_pct",
                ]
            ],
            hide_index=True,
            width="stretch",
            column_config={
                "minute": st.column_config.DatetimeColumn("Minute (London)", format="D MMM HH:mm"),
                "quote_volume": st.column_config.NumberColumn("Quote volume", format="%.0f"),
                "trade_count": st.column_config.NumberColumn("Trades", format="%d"),
                "range_pct": st.column_config.NumberColumn("Range %", format="%.3f"),
            },
        )

    st.caption(
        f"Refreshes every {REFRESH_SECONDS}s. Showing the {WINDOW_HOURS} hours up to the newest candle. "
        "Prices in USDT."
    )


live_view()
