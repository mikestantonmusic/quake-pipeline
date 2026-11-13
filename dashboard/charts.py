"""Chart and map builders. Colors come from a validated palette with light and dark steps.

Single-series charts use one hue (no legend; the heading names the series). Depth on the
map is an ordered three-step ramp of one hue, validated with the dataviz palette checker:
light 250/450/650, dark 250/450/600 (the darkest step must still clear the dark surface).
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import pydeck as pdk

PALETTES = {
    "light": {
        "series": "#2a78d6",
        "ink": "#0b0b0b",
        "ink_secondary": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "baseline": "#c3c2b7",
        "depth": {"shallow": "#86b6ef", "intermediate": "#2a78d6", "deep": "#104281"},
    },
    "dark": {
        "series": "#3987e5",
        "ink": "#f0efec",
        "ink_secondary": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "baseline": "#383835",
        "depth": {"shallow": "#86b6ef", "intermediate": "#2a78d6", "deep": "#184f95"},
    },
}
UNKNOWN_DEPTH = "#898781"
DEPTH_LABELS = {
    "shallow": "Shallow (< 70 km)",
    "intermediate": "Intermediate (70–300 km)",
    "deep": "Deep (300+ km)",
}
FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'


def palette(mode: str | None) -> dict:
    return PALETTES["dark" if mode == "dark" else "light"]


def _style(chart: alt.Chart, p: dict, height: int) -> alt.Chart:
    """Recessive chrome: hairline solid grid, muted axis labels, transparent background."""
    return (
        chart.properties(height=height, background="transparent")
        .configure_view(strokeWidth=0)
        .configure_axis(
            labelColor=p["muted"],
            titleColor=p["ink_secondary"],
            gridColor=p["grid"],
            domainColor=p["baseline"],
            tickColor=p["baseline"],
            labelFont=FONT,
            titleFont=FONT,
            titleFontWeight="normal",
        )
    )


def daily_line(daily: pd.DataFrame, mode: str | None) -> alt.Chart:
    """Earthquakes per day, with a crosshair and tooltip on the nearest day."""
    p = palette(mode)
    hover = alt.selection_point(nearest=True, on="pointerover", fields=["date"], empty=False)
    base = alt.Chart(daily).encode(x=alt.X("date:T", title=None, axis=alt.Axis(format="%d %b")))
    area = base.mark_area(color=p["series"], opacity=0.12).encode(y="earthquakes:Q")
    line = base.mark_line(color=p["series"], strokeWidth=2).encode(
        y=alt.Y("earthquakes:Q", title="Earthquakes per day")
    )
    rule = (
        base.mark_rule(color=p["muted"])
        .encode(
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[
                alt.Tooltip("date:T", title="Day", format="%a %d %b %Y"),
                alt.Tooltip("earthquakes:Q", title="Earthquakes"),
            ],
        )
        .add_params(hover)
    )
    point = base.mark_point(filled=True, size=80, color=p["series"]).encode(
        y="earthquakes:Q", opacity=alt.condition(hover, alt.value(1), alt.value(0))
    )
    return _style(alt.layer(area, line, rule, point), p, height=260)


def magnitude_bars(hist: pd.DataFrame, mode: str | None) -> alt.Chart:
    p = palette(mode)
    chart = (
        alt.Chart(hist)
        .mark_bar(color=p["series"], cornerRadiusEnd=4)
        .encode(
            x=alt.X(
                "band:N",
                title="Magnitude",
                sort=alt.SortField("bin_start"),
                axis=alt.Axis(labelAngle=0),
                scale=alt.Scale(paddingInner=0.12),
            ),
            y=alt.Y("earthquakes:Q", title="Earthquakes"),
            tooltip=[
                alt.Tooltip("band:N", title="Magnitude"),
                alt.Tooltip("earthquakes:Q", title="Earthquakes"),
            ],
        )
    )
    return _style(chart, p, height=260)


def region_bars(regions: pd.DataFrame, mode: str | None) -> alt.Chart:
    """Ranked horizontal bars, busiest first, one hue."""
    p = palette(mode)
    chart = (
        alt.Chart(regions)
        .mark_bar(color=p["series"], cornerRadiusEnd=4)
        .encode(
            y=alt.Y("region:N", title=None, sort="-x", scale=alt.Scale(paddingInner=0.2)),
            x=alt.X("earthquakes:Q", title="Earthquakes"),
            tooltip=[
                alt.Tooltip("region:N", title="Region"),
                alt.Tooltip("earthquakes:Q", title="Earthquakes"),
            ],
        )
    )
    return _style(chart, p, height=max(160, 28 * len(regions)))


def loads_bars(loads: pd.DataFrame, mode: str | None) -> alt.Chart:
    """New event versions inserted by each load."""
    p = palette(mode)
    chart = (
        alt.Chart(loads)
        .mark_bar(color=p["series"], size=6, cornerRadiusEnd=2)
        .encode(
            x=alt.X("finished_at:T", title=None, axis=alt.Axis(format="%d %b %H:%M")),
            y=alt.Y("n_inserted:Q", title="New versions per load"),
            tooltip=[
                alt.Tooltip("finished_at:T", title="Finished (UTC)", format="%d %b %Y %H:%M"),
                alt.Tooltip("source:N", title="Source"),
                alt.Tooltip("n_features:Q", title="Events in file"),
                alt.Tooltip("n_inserted:Q", title="New versions"),
                alt.Tooltip("n_invalid:Q", title="Invalid"),
            ],
        )
    )
    return _style(chart, p, height=240)


def depth_color(depth_class: str, mode: str | None) -> str:
    return palette(mode)["depth"].get(depth_class, UNKNOWN_DEPTH)


def quake_map(events: pd.DataFrame, mode: str | None) -> pdk.Deck:
    """One dot per earthquake: size = magnitude, color = depth band. Biggest drawn on top."""
    df = events.dropna(subset=["latitude", "longitude"]).copy()
    df["mag_display"] = df["mag"].map(lambda m: "unknown" if pd.isna(m) else f"{m:.1f}")
    df["radius"] = df["mag"].fillna(0).clip(lower=0).map(lambda m: min(2 * 1.4**m, 28))
    df["color"] = df["depth_class"].map(lambda d: _rgb(depth_color(d, mode)) + [200])
    df["when"] = df["occurred_at"].dt.strftime("%d %b %Y %H:%M UTC")
    df = df.sort_values("mag", na_position="first")
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df[
            ["longitude", "latitude", "radius", "color", "place", "mag_display", "depth_km", "when"]
        ],
        get_position="[longitude, latitude]",
        get_radius="radius",
        radius_units="pixels",
        radius_min_pixels=2,
        get_fill_color="color",
        stroked=True,
        get_line_color=[255, 255, 255, 160] if mode != "dark" else [26, 26, 25, 200],
        line_width_min_pixels=1,
        pickable=True,
    )
    return pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=0.8),
        tooltip={
            "html": "<b>M {mag_display}</b> · {place}<br/>{when}<br/>Depth {depth_km} km",
            "style": {"fontFamily": FONT, "fontSize": "13px"},
        },
    )


def _rgb(hex_color: str) -> list[int]:
    h = hex_color.lstrip("#")
    return [int(h[i : i + 2], 16) for i in (0, 2, 4)]
