"""Streamlit dashboard for the VBB GTFS-RT delay pipeline.

Silver (`data/silver/stop_updates`) is the source of truth here: it holds one row
per stop update, so everything from stop rankings to delay distributions can be
derived from it. Gold is still built by `spark/gold.py` and shown as a
cross-check, but the dashboard does not depend on it.

Three data traps this dashboard exists to expose rather than hide:

* Over half of all stop updates carry no delay value at all. Averages must use
  the reported subset as their denominator, so coverage is always shown next to
  a mean.
* 32767 is the GTFS-RT int16 maximum and VBB uses it as "no delay info", not as
  a nine-hour delay. Those values are nulled out on load.
* A skipped stop is not an on-time stop, so `scheduleRelationship` is kept and
  skipped rows are excluded from the delay statistics.

Never mutate the frame returned by `load_updates`: Streamlit hands out one
shared instance of it, which is why that loader uses cache_resource.
"""

from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pydeck as pdk
import streamlit as st

st.set_page_config(page_title="VBB realtime delays", layout="wide")

DATA = Path("data")
SILVER = DATA / "silver" / "stop_updates"
GOLD = DATA / "gold" / "otp_by_route_hour"
VALIDATION = DATA / "gold" / "delay_validation"
STATIC = DATA / "gtfs"
ROUTES = STATIC / "routes.txt"
TRIPS = STATIC / "trips.txt"
STOPS = STATIC / "stops.txt"

SENTINEL = 32767  # GTFS-RT int16 max, VBB's "no delay info" marker
LATE = 300  # seconds; "late" means more than five minutes behind schedule
VERY_LATE = 900  # seconds; fifteen minutes

# GTFS extended route types as used in VBB routes.txt
MODES = {
    "3": "Rail",
    "100": "S-Bahn",
    "106": "S-Bahn",
    "109": "S-Bahn",
    "400": "U-Bahn",
    "700": "Bus",
    "900": "Tram",
    "1000": "Tram",
}

# Delay buckets in seconds. An explicit ladder, so the histogram reads the same
# way as every other number on the page.
EDGES = [
    -np.inf,
    -60,
    -30,
    -10,
    0,
    10,
    30,
    60,
    120,
    180,
    300,
    600,
    900,
    1800,
    3600,
    np.inf,
]
BUCKETS = [
    "early >60s",
    "60..30s early",
    "30..10s early",
    "10s early",
    "on time",
    "10..30s",
    "30..60s",
    "1..2 min",
    "2..3 min",
    "3..5 min",
    "5..10 min",
    "10..15 min",
    "15..30 min",
    "30..60 min",
    ">60 min",
]
TONES = ["Early", "On time", "Late"]
TONE_COLORS = ["#6baed6", "#41ab5d", "#d95f0e"]
PERCENTILES = [0.5, 0.75, 0.9, 0.95, 0.99]
PERCENTILE_COLUMNS = ["mean", *[f"p{int(p * 100)}" for p in PERCENTILES]]
STAT_LABELS = ["mean", *[f"P{int(p * 100)}" for p in PERCENTILES]]
TRIP_CAP = 300


def duration(value) -> str:
    """Seconds as a compact human duration, '-' when unknown."""
    if value is None or pd.isna(value):
        return "-"
    sign = "-" if value < 0 else ""
    value = abs(round(value))
    if value < 60:
        return f"{sign}{value}s"
    if value < 3600:
        return f"{sign}{value // 60}m {value % 60:02d}s"
    return f"{sign}{value // 3600}h {value % 3600 // 60:02d}m"


def tone(bucket: str) -> str:
    """Bucket -> Early / On time / Late, from the ladder position not the text."""
    index = BUCKETS.index(bucket)
    if index < 4:
        return "Early"
    return "On time" if index == 4 else "Late"


def clock(epoch) -> str:
    """Epoch seconds as Berlin local HH:MM, or '-' when the feed sent no time."""
    if epoch is None or pd.isna(epoch) or float(epoch) <= 0:
        return "-"
    return pd.Timestamp.fromtimestamp(float(epoch), tz="Europe/Berlin").strftime(
        "%H:%M"
    )


# ------------------------------------------------------------------- data load


@st.cache_data(show_spinner=False, ttl=60)
def load_labels() -> pd.DataFrame:
    """Line number, mode and the line's endpoints, keyed by internal route_id."""
    if not (ROUTES.exists() and TRIPS.exists()):
        return pd.DataFrame(
            columns=["route_id", "route_short_name", "route_type", "route"]
        )

    trips = pd.read_csv(
        TRIPS,
        dtype=str,
        keep_default_na=False,
        usecols=["route_id", "direction_id", "trip_headsign"],
    )
    # "Ersatzverkehr ..." are replacement-service detours, not a regular destination.
    trips = trips[
        (trips.trip_headsign != "")
        & ~trips.trip_headsign.str.contains("Ersatz", case=False)
    ]
    # The most frequent headsign per direction is that direction's destination.
    dest = (
        trips.groupby(["route_id", "direction_id"])["trip_headsign"]
        .agg(lambda s: s.value_counts().idxmax())
        .reset_index(name="ziel")
    )
    # Both directions of a line terminate at each other, so the two headsigns are
    # the line's start and end station.
    ends = (
        dest.groupby("route_id")["ziel"]
        .agg(lambda s: " <-> ".join(dict.fromkeys(s)))
        .reset_index(name="route")
    )

    routes = pd.read_csv(ROUTES, dtype=str, keep_default_na=False)[
        ["route_id", "route_short_name", "route_type"]
    ]
    return routes.merge(ends, on="route_id", how="left")


@st.cache_data(show_spinner=False, ttl=60)
def load_stops() -> pd.DataFrame:
    """Stop names, fare zones and coordinates for the realtime stop ids."""
    columns = ["stop_id", "stop_name", "zone_id", "stop_lat", "stop_lon"]
    if not STOPS.exists():
        return pd.DataFrame(columns=columns)
    stops = pd.read_csv(STOPS, dtype=str, keep_default_na=False, usecols=columns)
    stops["stop_lat"] = pd.to_numeric(stops["stop_lat"], errors="coerce")
    stops["stop_lon"] = pd.to_numeric(stops["stop_lon"], errors="coerce")
    return stops


@st.cache_resource(show_spinner=False, ttl=60)
def load_updates() -> tuple[pd.DataFrame, dict]:
    """Silver stop updates joined to static line and stop labels, plus load stats.

    cache_resource rather than cache_data on purpose: this frame is an order of
    magnitude larger than everything else combined and must not be copied on every
    access. Callers share one instance, so treat it as read-only.
    """
    parts = sorted(SILVER.glob("part-*.parquet"))
    if not parts:
        return pd.DataFrame(), {}

    wanted = [
        "tripId",
        "routeId",
        "stopId",
        "stopSequence",
        "arrival_delay",
        "departure_delay",
        "arrival_time",
        "departure_time",
        "scheduleRelationship",
        "ingested_at",
    ]
    df = pd.concat(
        [pd.read_parquet(p, columns=wanted) for p in parts], ignore_index=True
    )

    meta = {
        "parts": len(parts),
        "rows": len(df),
        "sentinel_departures": int((df["departure_delay"] >= SENTINEL).sum()),
        "sentinel_arrivals": int((df["arrival_delay"] >= SENTINEL).sum()),
    }

    # A sentinel means "unknown", not "nine hours late": null it so it drops out of
    # every average and counts as unreported instead of stretching the scale.
    for column in ("departure_delay", "arrival_delay"):
        df[column] = df[column].where(df[column] < SENTINEL)

    df["stopSequence"] = df["stopSequence"].astype("Int64")
    df["session"] = df["ingested_at"].dt.floor("h")

    labels = load_labels()
    if labels.empty:
        df = df.assign(route_short_name=df["routeId"], route="?", route_mode="Other")
    else:
        df = df.merge(labels, left_on="routeId", right_on="route_id", how="left").drop(
            columns="route_id"
        )
        df["route_short_name"] = df["route_short_name"].fillna(df["routeId"])
        df["route"] = df["route"].fillna("?")
        df["route_mode"] = df["route_type"].map(MODES).fillna("Other")

    stops = load_stops()
    if stops.empty:
        df = df.assign(stop_name=None, zone_id=None, stop_lat=None, stop_lon=None)
    else:
        df = df.merge(stops, left_on="stopId", right_on="stop_id", how="left").drop(
            columns="stop_id"
        )
    df["stop_label"] = df["stop_name"].fillna(df["stopId"])
    meta["stop_names"] = float(df["stop_name"].notna().mean())

    return df, meta


@st.cache_data(show_spinner=False, ttl=60)
def load_gold() -> pd.DataFrame:
    parts = sorted(GOLD.glob("part-*.parquet"))
    if not parts:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)


@st.cache_data(show_spinner=False, ttl=60)
def load_validation() -> pd.DataFrame:
    """A few columns off delay_validation, enough to judge data quality."""
    wanted = [
        "routeId",
        "arrival_dev",
        "departure_dev",
        "delta_to_reported",
        "schedule_matched",
        "stop_match",
    ]
    frames = []
    for part in sorted(VALIDATION.glob("part-*.parquet")):
        available = set(pq.ParquetFile(part).schema.names)
        frames.append(
            pd.read_parquet(part, columns=[c for c in wanted if c in available])
        )
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# --------------------------------------------------------------------- filtering


def filtered(
    df: pd.DataFrame,
    sessions=(),
    modes=(),
    line: str = "",
    zones=(),
    reported_only: bool = False,
    columns=None,
) -> pd.DataFrame:
    """Apply the sidebar filters. An empty selection means "everything".

    `columns` narrows the projection so filter-dependent options (the fare-zone
    list) can be derived without copying the whole frame.
    """
    source = df if columns is None else df[columns]
    mask = pd.Series(True, index=df.index)
    if sessions:
        mask &= df["session"].isin(pd.to_datetime(list(sessions)))
    if modes:
        mask &= df["route_mode"].isin(list(modes))
    if line:
        mask &= df["route_short_name"].str.contains(
            line, case=False, na=False, regex=False
        )
    if zones:
        mask &= df["zone_id"].isin(list(zones))
    if reported_only:
        mask &= df["departure_delay"].notna()
    return source[mask]


# ------------------------------------------------------------------- aggregation


def rank(
    frame: pd.DataFrame, keys: list[str], min_observations: int = 0
) -> pd.DataFrame:
    """Delay statistics per key, sorted worst P90 first.

    Every mean and percentile uses reported delays only. Unreported rows still
    count towards n_updates and n_trips so the coverage columns stay honest.
    """
    grouped = frame.groupby(keys, dropna=False)
    out = pd.DataFrame(
        {
            "n_updates": grouped.size(),
            "n_trips": grouped["tripId"].nunique(),
            "n_reported": grouped["departure_delay"].count(),
        }
    )

    delays = frame[frame["departure_delay"].notna()]
    columns = ["mean_delay", "median_delay", "p90_delay", "max_delay", "share_late"]
    if delays.empty:
        for column in columns:
            out[column] = np.nan
    else:
        reported = delays.groupby(keys, dropna=False)["departure_delay"]
        out["mean_delay"] = reported.mean()
        out["median_delay"] = reported.quantile(0.5)
        out["p90_delay"] = reported.quantile(0.9)
        out["max_delay"] = reported.max()
        late = delays.assign(is_late=delays["departure_delay"] > LATE)
        out["share_late"] = late.groupby(keys, dropna=False)["is_late"].mean()

    out = out.reset_index()
    if min_observations:
        out = out[out["n_reported"] >= min_observations]
    return out.sort_values("p90_delay", ascending=False, ignore_index=True)


def percentile_table(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Count, mean, max and every percentile in PERCENTILES, per key, in seconds."""
    delays = frame[frame["departure_delay"].notna()]
    names = ["n", "mean", "max", *[f"p{int(p * 100)}" for p in PERCENTILES]]
    if delays.empty:
        return pd.DataFrame(columns=[*keys, *names])

    grouped = delays.groupby(keys, dropna=False)["departure_delay"]
    out = grouped.agg(["count", "mean", "max"]).rename(
        columns={"count": "n", "mean": "mean", "max": "max"}
    )
    for percentile in PERCENTILES:
        out[f"p{int(percentile * 100)}"] = grouped.quantile(percentile)
    return out.reset_index()[keys + names]


def bucket_counts(frame: pd.DataFrame, series_columns: dict[str, str]) -> pd.DataFrame:
    """Long-form update counts per delay bucket for one or more delay columns."""
    frames = []
    for label, column in series_columns.items():
        counts = (
            pd.cut(frame[column].dropna(), bins=EDGES, labels=BUCKETS, right=False)
            .value_counts()
            .reindex(BUCKETS)
            .fillna(0)
            .astype(int)
        )
        frames.append(
            pd.DataFrame(
                {"kind": label, "bucket": BUCKETS, "updates": counts.to_numpy()}
            )
        )
    return pd.concat(frames, ignore_index=True)


def bucket_chart(counts: pd.DataFrame, split_kind: bool) -> alt.Chart:
    """Bar chart of bucket counts, coloured by early/on-time/late (and kind)."""
    encoding = {
        "x": alt.X("bucket:N", sort=BUCKETS, title=None, axis=alt.Axis(labelAngle=-35)),
        "y": alt.Y("updates:Q", title="Stop updates"),
        "tooltip": ["bucket:N", "updates:Q"],
    }
    if split_kind:
        encoding["color"] = alt.Color(
            "kind:N",
            title=None,
            scale=alt.Scale(
                domain=list(series_columns_keys(counts)), range=["#6baed6", "#d95f0e"]
            ),
        )
    else:
        encoding["color"] = alt.Color(
            "tone:N",
            title=None,
            scale=alt.Scale(domain=TONES, range=TONE_COLORS),
        )
        encoding["tooltip"] = ["bucket:N", "tone:N", "updates:Q"]
    return alt.Chart(counts).mark_bar().encode(**encoding).properties(height=280)


def series_columns_keys(counts: pd.DataFrame) -> list[str]:
    return list(dict.fromkeys(counts["kind"]))


def stop_map(frame: pd.DataFrame) -> pdk.Deck:
    """Stops on a real basemap: size = reported updates, colour = P90 delay."""
    lat, lon = frame["stop_lat"], frame["stop_lon"]
    span = max(float(lat.max() - lat.min()), float(lon.max() - lon.min()), 1e-3)
    zoom = min(15.0, max(9.0, np.log2(900 / span)))

    low, high = np.array([254, 224, 139]), np.array([215, 48, 39])
    delays = frame["p90_delay"]
    spread = float(delays.max() - delays.min())
    share = (delays - delays.min()) / spread if spread else delays * 0
    fill = [
        [189, 189, 189]
        if pd.isna(t)
        else (low + (high - low) * t).astype(int).tolist()
        for t in share
    ]

    reports = frame["n_reported"].fillna(0)
    radius = 40 + 260 * np.sqrt(reports / max(int(reports.max()), 1))
    data = frame.assign(
        fill=fill,
        radius=radius.round(1),
        hover=(
            frame["stop_label"].astype(str)
            + "<br>zone "
            + frame["zone_id"].fillna("-").astype(str)
            + "<br>P90 "
            + frame["p90_delay"].map(duration)
            + ", mean "
            + frame["mean_delay"].map(duration)
            + "<br>"
            + reports.astype(int).astype(str)
            + " reported updates"
        ),
    )[["stop_lon", "stop_lat", "fill", "radius", "hover"]]

    return pdk.Deck(
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                data=data,
                get_position="[stop_lon, stop_lat]",
                get_fill_color="fill",
                get_radius="radius",
                pickable=True,
                radius_min_pixels=3,
                radius_max_pixels=40,
            )
        ],
        initial_view_state=pdk.ViewState(
            latitude=float(lat.mean()), longitude=float(lon.mean()), zoom=zoom
        ),
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        tooltip={"text": "{hover}"},
    )


def ecdf(
    frame: pd.DataFrame, column: str, label: str, points: int = 200
) -> pd.DataFrame:
    """Sampled empirical CDF: share of reported delays at or below each value."""
    series = frame[column].dropna().sort_values()
    if series.empty:
        return pd.DataFrame(columns=["kind", "delay", "share"])
    step = max(1, len(series) // points)
    sampled = series.iloc[::step]
    share = (
        np.minimum(np.arange(len(sampled)) * step + 1, len(series)) / len(series)
    )
    return pd.DataFrame(
        {
            "kind": label,
            "delay": [*sampled.to_numpy(), series.max()],
            "share": [*share, 1.0],
        }
    )


# ------------------------------------------------------------------- empty state

updates, meta = load_updates()
gold = load_gold()

st.title("VBB realtime delays")
st.caption(
    "GTFS-RT stop updates from the VBB feed, joined to the static GTFS schedule. "
    "Delay means departure delay in seconds."
)

if updates.empty:
    if gold.empty:
        st.info(
            "No data yet. Run: main.py -> bronze -> silver -> gold (see RUN_ORDER.md)."
        )
        st.stop()
    st.warning(
        "Silver holds no stop updates, so only the Gold table is shown. "
        "Run step 4 in RUN_ORDER.md for the full dashboard."
    )
    st.dataframe(
        gold.sort_values(gold.columns[2], ascending=False).head(300),
        width="stretch",
        hide_index=True,
    )
    st.stop()

with st.expander("How to read these numbers"):
    st.markdown(
        f"""
* **Coverage** is the share of stop updates that actually carried a delay value.
  The rest are missing, not zero - so every average uses only the reported subset,
  and coverage is always printed next to it.
* **{meta["sentinel_departures"]}** departure and **{meta["sentinel_arrivals"]}**
  arrival values equalled {SENTINEL}, the GTFS-RT int16 maximum, which VBB uses as
  "no delay info". They are excluded rather than read as nine-hour delays.
* **A skipped stop is not an on-time stop.** Delay statistics only cover
  `scheduleRelationship == SCHEDULED`.
* The distribution is heavily zero-inflated: most updates report 0s and the mean
  sits far below the 90th percentile. Read the percentiles, not the mean.
* *Collection session* is when `main.py` fetched the feed, not the hour of service.
  With one snapshot per run there is no time-of-day profile yet.
"""
    )

# ---------------------------------------------------------------------- filters

with st.sidebar:
    st.header("Filters")

    sessions = sorted(pd.Timestamp(t) for t in updates["session"].dropna().unique())
    current = sessions[-1] if sessions else None
    picked_sessions = (current,) if current is not None else ()
    if current is None:
        st.caption("No collection session in silver.")
    else:
        st.caption(
            f"Collection session: {current:%Y-%m-%d %H:%M} - only the current one "
            "is kept."
        )

    available_modes = sorted(updates["route_mode"].unique())
    picked_modes = st.multiselect(
        "Mode", options=available_modes, default=available_modes
    )

    line_query = st.text_input(
        "Line contains",
        placeholder="e.g. S3, M10, 902",
        help="Substring match on the line number.",
    ).strip()

    reachable = filtered(
        updates, picked_sessions, picked_modes, line_query, columns=["zone_id"]
    )
    available_zones = sorted(z for z in reachable["zone_id"].dropna().unique() if z)
    picked_zones = st.multiselect("Fare zone", options=available_zones)

    reported_only = st.checkbox(
        "Only updates with a reported delay",
        value=False,
        help="Half of all rows carry no delay value. Filtering them out shrinks the "
        "data and pushes coverage to 100%, but hides how much is missing.",
    )
    top_n = st.slider("Rows in rankings", 10, 200, 25, step=5)

view = filtered(
    updates, picked_sessions, picked_modes, line_query, picked_zones, reported_only
)

if view.empty:
    st.warning("No stop updates match these filters.")
    st.stop()

reported = view[view["departure_delay"].notna()]
coverage = len(reported) / len(view)

tab_overview, tab_distribution, tab_lines, tab_stops, tab_quality, tab_trips = st.tabs(
    [
        "Overview",
        "Distribution",
        "Lines",
        "Stops & Zones",
        "Data quality",
        "Trip explorer",
    ]
)

# ===================================================================== overview

with tab_overview:
    row = st.columns(5)
    row[0].metric("Collection sessions", view["session"].nunique())
    row[1].metric("Stop updates", f"{len(view):,}")
    row[2].metric("Trips", f"{view['tripId'].nunique():,}")
    row[3].metric("Lines", view["route_short_name"].nunique())
    row[4].metric("Stops", view["stopId"].nunique())

    st.markdown("**Delay, reported updates only**")
    row = st.columns(5)
    row[0].metric("Coverage", f"{coverage:.1%}")
    row[1].metric("Mean", duration(reported["departure_delay"].mean()))
    row[2].metric("Median", duration(reported["departure_delay"].median()))
    row[3].metric("P90", duration(reported["departure_delay"].quantile(0.9)))
    row[4].metric("P95", duration(reported["departure_delay"].quantile(0.95)))

    st.markdown("**Share of reported delays**")
    shares = {
        "Exactly on time": (reported["departure_delay"] == 0).mean(),
        "Early": (reported["departure_delay"] < 0).mean(),
        f"Late >{LATE // 60} min": (reported["departure_delay"] > LATE).mean(),
        f"Late >{VERY_LATE // 60} min": (
            reported["departure_delay"] > VERY_LATE
        ).mean(),
    }
    row = st.columns(5)
    for slot, (label, value) in zip(row[:4], shares.items()):
        slot.metric(label, f"{value:.1%}" if pd.notna(value) else "-")
    row[4].metric("Worst seen", duration(reported["departure_delay"].max()))

    left, right = st.columns(2)

    with left:
        st.subheader("Percentiles by mode")
        by_mode = percentile_table(view, ["route_mode"])
        melted = by_mode.melt(
            id_vars="route_mode",
            value_vars=PERCENTILE_COLUMNS,
            var_name="stat",
            value_name="seconds",
        )
        melted["stat"] = melted["stat"].map(
            {
                "mean": "mean",
                **{f"p{int(p * 100)}": f"P{int(p * 100)}" for p in PERCENTILES},
            }
        )
        melted["route_mode"] = melted["route_mode"].astype(str)
        melted = melted.dropna(subset=["seconds"])
        st.altair_chart(
            alt.Chart(melted)
            .mark_bar()
            .encode(
                x=alt.X("seconds:Q", title="Delay (s)"),
                y=alt.Y("route_mode:N", sort="-x", title=None),
                color=alt.Color("stat:N", title=None),
                column=alt.Column("stat:N", sort=STAT_LABELS, title=None),
                tooltip=[
                    "route_mode:N",
                    "stat:N",
                    alt.Tooltip("seconds:Q", format=".1f"),
                ],
            )
            .properties(height=250),
            width="stretch",
        )
        display = by_mode.copy()
        display["route_mode"] = display["route_mode"].astype(str)
        for column in [*PERCENTILE_COLUMNS, "max"]:
            display[column] = display[column].round(1)
        st.dataframe(display, width="stretch", hide_index=True)

    with right:
        st.subheader("Current collection session")
        by_session = (
            view.assign(session_label=view["session"].dt.strftime("%Y-%m-%d %H:%M"))
            .groupby("session_label")
            .agg(
                n_updates=("departure_delay", "size"),
                n_reported=("departure_delay", "count"),
                mean=("departure_delay", "mean"),
                p90=("departure_delay", lambda s: s.quantile(0.9)),
            )
            .reset_index()
        )
        by_session["coverage"] = by_session["n_reported"] / by_session["n_updates"]
        order = list(by_session["session_label"])
        long = by_session.melt(
            id_vars="session_label",
            value_vars=["coverage", "mean", "p90"],
            var_name="stat",
            value_name="value",
        )
        st.altair_chart(
            alt.Chart(long[long["stat"] == "coverage"])
            .mark_bar(color="#41ab5d")
            .encode(
                x=alt.X("session_label:N", sort=order, title=None),
                y=alt.Y("value:Q", axis=alt.Axis(format="%"), title="Coverage"),
                tooltip=["session_label:N", alt.Tooltip("value:Q", format=".1%")],
            )
            .properties(height=90),
            width="stretch",
        )
        st.altair_chart(
            alt.Chart(long[long["stat"] != "coverage"])
            .mark_point(filled=True, size=90)
            .encode(
                x=alt.X("session_label:N", sort=order, title=None),
                y=alt.Y("value:Q", title="Delay (s)"),
                color=alt.Color("stat:N", title=None),
                tooltip=[
                    "session_label:N",
                    "stat:N",
                    alt.Tooltip("value:Q", format=".1f"),
                ],
            )
            .properties(height=150),
            width="stretch",
        )
        st.caption(
            "Only the current collection session is kept: each `main.py` run "
            "replaces the previous snapshot, so there is no time-of-day profile."
        )

    st.subheader("Schedule relationship")
    relationship = (
        view["scheduleRelationship"]
        .fillna("MISSING")
        .value_counts()
        .rename_axis("scheduleRelationship")
        .reset_index(name="stop_updates")
    )
    st.altair_chart(
        alt.Chart(relationship)
        .mark_bar(color="#756bb1")
        .encode(
            x=alt.X("stop_updates:Q", title="Stop updates"),
            y=alt.Y("scheduleRelationship:N", sort="-x", title=None),
            tooltip=["scheduleRelationship:N", "stop_updates:Q"],
        )
        .properties(height=170),
        width="stretch",
    )
    st.caption(
        f"{view.loc[view['scheduleRelationship'] == 'SKIPPED', 'tripId'].nunique():,} trips "
        "contain at least one skipped stop. Skipped stops are excluded from every "
        "delay statistic on this page."
    )

# ================================================================== distribution

with tab_distribution:
    st.subheader("Departure delay distribution")
    counts = bucket_counts(view, {"Departure": "departure_delay"})
    counts["tone"] = counts["bucket"].map(tone)
    st.altair_chart(bucket_chart(counts, split_kind=False), width="stretch")
    st.caption(
        f"{len(reported):,} reported of {len(view):,} updates. Bar height is a count "
        "of stop updates, not a share, and the buckets are fixed-width in seconds."
    )

    left, right = st.columns(2)

    with left:
        st.subheader("Arrival vs departure")
        both = bucket_counts(
            view, {"Departure": "departure_delay", "Arrival": "arrival_delay"}
        )
        st.altair_chart(bucket_chart(both, split_kind=True), width="stretch")
        st.caption(
            "Departure is reported slightly more often and is always the later of "
            "the two, so the ranking pages use it throughout."
        )

    with right:
        st.subheader("Cumulative distribution")
        curves = pd.concat(
            [
                ecdf(view, "departure_delay", "Departure"),
                ecdf(view, "arrival_delay", "Arrival"),
            ],
            ignore_index=True,
        )
        st.altair_chart(
            alt.Chart(curves)
            .mark_line()
            .encode(
                x=alt.X("delay:Q", title="Delay (s)"),
                y=alt.Y(
                    "share:Q",
                    axis=alt.Axis(format="%"),
                    title="Share of reported delays",
                ),
                color=alt.Color("kind:N", title=None),
                tooltip=["kind:N", "delay:Q", alt.Tooltip("share:Q", format=".2%")],
            )
            .properties(height=280),
            width="stretch",
        )
        p90 = reported["departure_delay"].quantile(0.9)
        st.caption(
            f"Read it as: at x = {p90:.0f}s, 90% of reported departures were on or below "
            "that delay. The vertical jump at 0 is the mass of on-time reports."
        )

    st.subheader("Percentiles by mode")
    table = percentile_table(view, ["route_mode"])
    for column in ["mean", "max", *[f"p{int(p * 100)}" for p in PERCENTILES]]:
        table[column] = table[column].round(1)
    st.dataframe(table, width="stretch", hide_index=True)

# ========================================================================= lines

with tab_lines:
    metric = st.radio(
        "Rank lines by",
        [
            "P90 delay",
            "Mean delay",
            "Median delay",
            f"Share later than {LATE // 60} min",
            "Reported updates",
        ],
        horizontal=True,
    )
    column = {
        "P90 delay": "p90_delay",
        "Mean delay": "mean_delay",
        "Median delay": "median_delay",
        f"Share later than {LATE // 60} min": "share_late",
        "Reported updates": "n_reported",
    }[metric]

    lines = rank(view, ["route_short_name", "route_mode", "route"])
    if lines.empty or lines["n_reported"].sum() == 0:
        st.info("No reported delays in this selection, so lines cannot be ranked.")
    else:
        for title, frame, order_flag in (
            ("Worst", lines.nlargest(top_n, column), "descending"),
            ("Best", lines.nsmallest(top_n, column), "ascending"),
        ):
            st.subheader(f"{title} {len(frame)} lines by {metric.lower()}")
            shown = frame.assign(
                p90_text=frame["p90_delay"].map(duration),
                late_text=(frame["share_late"] * 100).round(1).astype(str) + " %",
            )
            st.altair_chart(
                alt.Chart(shown)
                .mark_bar()
                .encode(
                    y=alt.Y(
                        "route_short_name:N",
                        sort=alt.SortField(column, order=order_flag),
                        title=None,
                    ),
                    x=alt.X(f"{column}:Q", title=None),
                    color=alt.Color("route_mode:N", title=None),
                    tooltip=[
                        "route_short_name:N",
                        "route:N",
                        "route_mode:N",
                        "p90_text:N",
                        "late_text:N",
                        "n_reported:N",
                    ],
                )
                .properties(height=200 + 18 * len(shown)),
                width="stretch",
            )

    st.subheader("All lines")
    listing = lines.rename(
        columns={
            "route_short_name": "line",
            "route_mode": "mode",
            "n_trips": "trips",
            "n_updates": "updates",
            "n_reported": "reported",
            "median_delay": "median_s",
            "mean_delay": "mean_s",
            "p90_delay": "p90_s",
            "share_late": "late_%",
            "max_delay": "max_s",
        }
    )
    for column in ["median_s", "mean_s", "p90_s", "max_s"]:
        listing[column] = listing[column].round(1)
    listing["late_%"] = (listing["late_%"] * 100).round(1)
    listing["coverage_%"] = (
        listing["reported"] / listing["updates"].replace(0, np.nan) * 100
    ).round(1)
    st.dataframe(listing, width="stretch", hide_index=True, height=420)

    st.subheader("Where a line picks up delay")
    options = lines.nlargest(300, "n_updates")["route_short_name"].dropna().unique()
    if len(options) == 0:
        st.info("No lines in this selection.")
    else:
        if len(lines) > 300:
            st.caption("The 300 busiest lines in the current selection.")
        chosen = st.selectbox("Line", options, label_visibility="collapsed")
        subset = view[
            (view["route_short_name"] == chosen) & view["departure_delay"].notna()
        ]
        profile = (
            subset.groupby("stopSequence", dropna=False)["departure_delay"]
            .agg(n="count", mean="mean", p90=lambda s: s.quantile(0.9))
            .reset_index()
        )
        profile = profile.dropna(subset=["stopSequence"]).sort_values("stopSequence")
        if profile.empty:
            st.info("That line has no reported delays.")
        else:
            st.altair_chart(
                alt.Chart(profile)
                .mark_line(point=True, color="#2b8cbe")
                .encode(
                    x=alt.X("stopSequence:Q", title="Stop sequence"),
                    y=alt.Y("mean:Q", title="Mean reported delay (s)"),
                    tooltip=[
                        alt.Tooltip("stopSequence:Q"),
                        alt.Tooltip("mean:Q", format=".1f"),
                        alt.Tooltip("p90:Q", format=".1f"),
                        alt.Tooltip("n:Q"),
                    ],
                )
                .properties(height=240),
                width="stretch",
            )
            st.caption(
                "Delay along the line's stop sequence. A curve that climbs towards "
                "the end means the delay accumulates on the vehicle rather than at "
                "one bad stop."
            )

# ====================================================================== stops

with tab_stops:
    minimum = st.slider(
        "Minimum reported updates per stop",
        1,
        200,
        20,
        help="Keeps one-off outliers out of the ranking.",
    )
    stop_keys = ["stopId", "stop_label", "zone_id", "stop_lat", "stop_lon"]
    all_stops = rank(view, stop_keys)
    stops = all_stops[all_stops["n_reported"] >= minimum]
    zones = rank(view, ["zone_id"], min_observations=minimum)
    zones = zones[zones["zone_id"].notna() & (zones["zone_id"] != "")]

    left, right = st.columns(2)

    with left:
        st.subheader(f"Worst {min(top_n, len(stops))} stops by P90")
        if stops.empty:
            st.info(
                f"No stop has {minimum} or more reported updates in this selection."
            )
        else:
            shown = stops.head(top_n).assign(
                p90_text=stops.head(top_n)["p90_delay"].map(duration),
                late_text=(stops.head(top_n)["share_late"] * 100).round(1).astype(str)
                + " %",
            )
            st.altair_chart(
                alt.Chart(shown)
                .mark_bar(color="#d95f0e")
                .encode(
                    y=alt.Y(
                        "stop_label:N",
                        sort=alt.SortField("p90_delay", order="descending"),
                        title=None,
                    ),
                    x=alt.X("p90_delay:Q", title="P90 delay (s)"),
                    tooltip=[
                        "stop_label:N",
                        "zone_id:N",
                        "p90_text:N",
                        "late_text:N",
                        "n_trips:N",
                        "n_reported:N",
                    ],
                )
                .properties(height=200 + 18 * len(shown)),
                width="stretch",
            )
            st.caption(
                f"{len(stops):,} of {view['stopId'].nunique():,} stops reach the "
                f"{minimum}-update threshold."
            )

    with right:
        st.subheader("Worst fare zones by P90")
        if zones.empty:
            st.info("No fare zones in this selection.")
        else:
            shown = zones.head(top_n)
            st.altair_chart(
                alt.Chart(shown)
                .mark_bar(color="#8073ac")
                .encode(
                    y=alt.Y(
                        "zone_id:N",
                        sort=alt.SortField("p90_delay", order="descending"),
                        title=None,
                    ),
                    x=alt.X("p90_delay:Q", title="P90 delay (s)"),
                    tooltip=[
                        "zone_id:N",
                        alt.Tooltip("p90_delay:Q", format=".1f"),
                        alt.Tooltip("mean_delay:Q", format=".1f"),
                        "n_trips:N",
                        "n_reported:N",
                    ],
                )
                .properties(height=200 + 18 * len(shown)),
                width="stretch",
            )
            st.caption(
                "Zone A is central Berlin and carries far more traffic than the "
                "outer zones, so compare zones only within one mode."
            )

    st.subheader("Stop map")
    mapped = all_stops.dropna(subset=["stop_lat", "stop_lon"])
    mapped = mapped[(mapped["stop_lat"] != 0) & (mapped["stop_lon"] != 0)]
    if len(mapped) < 5:
        st.info("Not enough stops with coordinates in the current selection.")
    else:
        st.pydeck_chart(stop_map(mapped))
        st.caption(
            f"{len(mapped):,} stops of the current selection (filter a line, e.g. "
            "S3, to see its stations) on a real basemap. Circle size is reported "
            "updates, colour is P90 delay, grey means no reported delay. Tiles are "
            "fetched from CARTO, so the browser needs internet."
        )

    st.subheader("All stops")
    listing = stops.rename(
        columns={
            "stop_label": "stop",
            "zone_id": "zone",
            "n_trips": "trips",
            "n_updates": "updates",
            "n_reported": "reported",
            "median_delay": "median_s",
            "mean_delay": "mean_s",
            "p90_delay": "p90_s",
            "share_late": "late_%",
            "max_delay": "max_s",
        }
    )
    for column in ["median_s", "mean_s", "p90_s", "max_s"]:
        listing[column] = listing[column].round(1)
    listing["late_%"] = (listing["late_%"] * 100).round(1)
    st.dataframe(listing, width="stretch", hide_index=True, height=420)

# ================================================================ data quality

with tab_quality:
    relationship = view["scheduleRelationship"].fillna("MISSING").value_counts()
    checks = [
        ("Parquet parts in silver", f"{meta['parts']} parts"),
        ("Stop updates read from silver", f"{meta['rows']:,} rows"),
        ("Stop updates after sidebar filters", f"{len(view):,} rows"),
        ("Collection sessions in selection", f"{view['session'].nunique()} sessions"),
        ("Departure sentinels excluded", f"{meta['sentinel_departures']:,} rows"),
        ("Arrival sentinels excluded", f"{meta['sentinel_arrivals']:,} rows"),
        ("Updates with a reported departure delay", f"{len(reported):,} rows"),
        (
            "Updates with a reported arrival delay",
            f"{int(view['arrival_delay'].notna().sum()):,} rows",
        ),
        ("Coverage (departure delay)", f"{coverage:.1%}"),
        (
            "Distinct trips with a reported delay",
            f"{reported['tripId'].nunique():,} trips",
        ),
        ("SCHEDULED stop updates", f"{int(relationship.get('SCHEDULED', 0)):,} rows"),
        ("SKIPPED stop updates", f"{int(relationship.get('SKIPPED', 0)):,} rows"),
        ("NO_DATA stop updates", f"{int(relationship.get('NO_DATA', 0)):,} rows"),
    ]
    st.subheader("Load and coverage")
    st.dataframe(
        pd.DataFrame(checks, columns=["check", "value"]),
        width="stretch",
        hide_index=True,
    )
    st.caption(
        f"{meta['stop_names']:.1%} of the realtime stop ids exist in the static "
        "feed, measured over all of silver. The remainder are bare numeric ids "
        "(e.g. 900008102) that VBB publishes only in GTFS-RT, so they appear "
        "without a name or fare zone. A stale `data/gtfs.zip` lowers this further - "
        "refresh it weekly."
    )

    st.subheader("Realtime vs static schedule")
    validation = load_validation()
    if validation.empty:
        st.info(
            "No delay_validation table yet. Run `spark/enrich.py` (see RUN_ORDER.md)."
        )
    else:
        matched_mask = (
            validation["schedule_matched"].fillna(False).astype(bool)
            if "schedule_matched" in validation.columns
            else pd.Series(False, index=validation.index)
        )
        matched = validation[matched_mask]
        rows = [
            ["Validation rows", f"{len(validation):,} rows"],
            [
                "Matched to the static schedule",
                f"{len(matched):,} ({len(matched) / len(validation):.1%})",
            ],
        ]
        if "stop_match" in validation.columns and len(matched):
            rows.append(
                [
                    "Stop id matches static stop_times",
                    f"{matched['stop_match'].mean():.1%}",
                ]
            )
        if "delta_to_reported" in validation.columns:
            delta = validation["delta_to_reported"].dropna()
            rows.append(
                [
                    "Mean abs gap, reported delay vs schedule",
                    duration(delta.abs().mean()) if len(delta) else "-",
                ]
            )
        for column in ("arrival_dev", "departure_dev"):
            if column in matched.columns:
                deviation = matched[column].dropna()
                rows.append(
                    [
                        f"Mean abs {column.replace('_dev', '')} deviation vs schedule",
                        duration(deviation.abs().mean()) if len(deviation) else "-",
                    ]
                )
                rows.append(
                    [
                        f"Max abs {column.replace('_dev', '')} deviation vs schedule",
                        duration(deviation.abs().max()) if len(deviation) else "-",
                    ]
                )
        st.dataframe(
            pd.DataFrame(rows, columns=["check", "value"]),
            width="stretch",
            hide_index=True,
        )
        st.caption(
            "A low schedule-match rate is expected, not a bug: `data/gtfs.zip` is a "
            "weekly snapshot and only about a third of the trips in a live feed are "
            "still in it. It is a data-freshness signal."
        )

    st.subheader("Arrival vs departure")
    pair = view.dropna(subset=["arrival_delay", "departure_delay"])
    if len(pair) < 10:
        st.info("Not enough updates report both arrival and departure delay.")
    else:
        sample = pair.head(40000)
        st.altair_chart(
            alt.Chart(sample)
            .mark_circle(opacity=0.15)
            .encode(
                x=alt.X(
                    "departure_delay:Q",
                    title="Departure delay (s)",
                    scale=alt.Scale(zero=False, nice=False),
                ),
                y=alt.Y(
                    "arrival_delay:Q",
                    title="Arrival delay (s)",
                    scale=alt.Scale(zero=False, nice=False),
                ),
                tooltip=["route_short_name:N", "arrival_delay:Q", "departure_delay:Q"],
            )
            .properties(height=340),
            width="stretch",
        )
        st.caption(
            f"{len(pair):,} updates report both values, of which "
            f"{(pair['arrival_delay'] == pair['departure_delay']).mean():.1%} are "
            f"identical. Scatter is sampled to the first {len(sample):,} rows."
        )

    st.subheader("Gold table")
    if gold.empty:
        st.info("No gold table yet. Run step 6 in RUN_ORDER.md.")
    else:
        st.caption(
            "One row per line and collection hour, built by `spark/gold.py`. "
            "`n_reported` counts updates carrying a real delay value; "
            "`n_skipped` is separate, and `n_total - n_reported - n_skipped` "
            "is the rest of the feed, including rows that did not report a delay."
        )
        st.dataframe(
            gold.sort_values(gold.columns[2], ascending=False).head(300),
            width="stretch",
            hide_index=True,
        )
        stale = [
            c
            for c in ("n_trips", "n_reported", "p50_delay", "p90_delay")
            if c not in gold.columns
        ]
        if stale:
            st.warning(
                f"The gold table lacks {', '.join(stale)}. Re-run step 6 to pick up "
                "the corrected `spark/gold.py`."
            )

    st.subheader("Sources")
    provenance = []
    for label, path in (
        ("silver stop_updates", SILVER),
        ("gold otp_by_route_hour", GOLD),
        ("gold delay_validation", VALIDATION),
        ("gtfs routes.txt", ROUTES),
        ("gtfs trips.txt", TRIPS),
        ("gtfs stops.txt", STOPS),
    ):
        exists = path.exists()
        provenance.append(
            {
                "source": label,
                "path": str(path),
                "state": "present" if exists else "MISSING",
                "files": len(list(path.glob("*.parquet")))
                if path.is_dir()
                else int(exists),
                "modified": pd.Timestamp(path.stat().st_mtime).strftime(
                    "%Y-%m-%d %H:%M"
                )
                if exists
                else "-",
            }
        )
    st.dataframe(pd.DataFrame(provenance), width="stretch", hide_index=True)

# =============================================================== trip explorer

with tab_trips:
    st.subheader("One trip, stop by stop")
    st.caption(
        "Each row is a single stop update: what the feed reported and how far "
        "behind schedule that left the vehicle."
    )

    options = sorted(view["route_short_name"].dropna().unique())
    if len(options) == 0:
        st.info("No lines in the current selection.")
    else:
        chosen_line = st.selectbox("Line", options)
        trips_here = view[view["route_short_name"] == chosen_line]
        catalogue = (
            trips_here.groupby("tripId")
            .agg(session=("session", "max"), n_updates=("departure_delay", "count"))
            .reset_index()
            .sort_values(["session", "n_updates"], ascending=[False, False])
        )
        st.caption(
            f"{chosen_line} has {len(catalogue):,} trips in this selection"
            + (
                f", showing the {TRIP_CAP} most recent."
                if len(catalogue) > TRIP_CAP
                else "."
            )
        )
        catalogue = catalogue.head(TRIP_CAP)
        labels = {
            str(
                row.tripId
            ): f"{pd.Timestamp(row.session):%Y-%m-%d %H:%M} - {int(row.n_updates)} updates"
            for row in catalogue.itertuples()
        }
        chosen_trip = st.selectbox(
            "Trip",
            list(catalogue["tripId"].astype(str)),
            format_func=lambda t: labels.get(t, t),
        )

        trip = view[view["tripId"].astype(str) == chosen_trip].sort_values(
            ["ingested_at", "stopSequence"]
        )
        if trip.empty:
            st.info("No stop updates for that trip.")
        else:
            reported_trip = trip["departure_delay"].dropna()
            head = st.columns(4)
            head[0].metric("Line", str(trip.iloc[0]["route_short_name"]))
            head[1].metric("Stops reported", trip["stopId"].nunique())
            head[2].metric("Worst delay", duration(reported_trip.max()))
            head[3].metric(
                "Skipped stops", int((trip["scheduleRelationship"] == "SKIPPED").sum())
            )

            st.altair_chart(
                alt.Chart(trip)
                .mark_line(point=True)
                .encode(
                    x=alt.X("stopSequence:Q", title="Stop sequence"),
                    y=alt.Y("departure_delay:Q", title="Reported departure delay (s)"),
                    color=alt.condition(
                        alt.datum.scheduleRelationship == "SKIPPED",
                        alt.value("#d73027"),
                        alt.value("#2b8cbe"),
                    ),
                    tooltip=[
                        alt.Tooltip("stopSequence:Q"),
                        "stop_label:N",
                        alt.Tooltip("arrival_delay:Q", format=".0f"),
                        alt.Tooltip("departure_delay:Q", format=".0f"),
                        "scheduleRelationship:N",
                    ],
                )
                .properties(height=280),
                width="stretch",
            )

            table = trip[
                [
                    "stopSequence",
                    "stop_label",
                    "zone_id",
                    "scheduleRelationship",
                    "arrival_delay",
                    "departure_delay",
                ]
            ].copy()
            table["realtime_departure"] = trip["departure_time"].map(clock)
            st.dataframe(
                table.rename(
                    columns={
                        "stopSequence": "seq",
                        "stop_label": "stop",
                        "zone_id": "zone",
                        "scheduleRelationship": "relationship",
                        "arrival_delay": "arrival_s",
                        "departure_delay": "departure_s",
                        "realtime_departure": "realtime departure",
                    }
                ),
                width="stretch",
                hide_index=True,
            )
            st.caption(
                "Gaps in `seq` mean the feed reported nothing for those stops on "
                "this trip; that is missing data, not a skipped stop. Only rows "
                "whose relationship is SKIPPED were dropped by the publisher. "
                "`realtime departure` is the predicted Berlin local time from the "
                "feed, not the static timetable."
            )
