"""Render benchmark charts (matplotlib PNGs) from the results JSON.

Colours come from the validated data-viz reference palette (categorical slots 1-3, which clear
the colour-blind all-pairs gate), with the palette's chart chrome/ink tokens. Grouped bars are
the right form here: the job is comparing magnitudes across a few labelled configurations.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: write files, never open a window
import matplotlib.pyplot as plt  # noqa: E402

# Validated categorical slots (light mode) + chrome/ink from references/palette.md.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"

RW_ORDER = [(1, 1), (2, 2), (3, 3)]
RW_LABELS = [f"R={r}/W={w}" for r, w in RW_ORDER]


def _style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
            "font.family": "sans-serif",
            "font.sans-serif": ["Segoe UI", "DejaVu Sans", "Arial"],
            "text.color": INK,
            "axes.labelcolor": INK_2,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "axes.edgecolor": BASELINE,
        }
    )


def _rows(results: list[dict], **filters: object) -> list[dict]:
    return [r for r in results if all(r[k] == v for k, v in filters.items())]


def _value_for(results: list[dict], metric: str, **filters: object) -> list[float]:
    out = []
    for r, w in RW_ORDER:
        matches = _rows(results, r=r, w=w, **filters)
        out.append(float(matches[0][metric]) if matches else 0.0)
    return out


def _bars(ax, groups: list[list[float]], labels: list[str], colors: list[str], fmt: str) -> None:
    import numpy as np

    n = len(groups)
    x = np.arange(len(RW_LABELS))
    width = 0.8 / n
    for i, (vals, label, color) in enumerate(zip(groups, labels, colors, strict=True)):
        offset = (i - (n - 1) / 2) * width
        bars = ax.bar(x + offset, vals, width * 0.9, label=label, color=color, zorder=3)
        for b, v in zip(bars, vals, strict=True):
            ax.annotate(
                format(v, fmt),
                (b.get_x() + b.get_width() / 2, b.get_height()),
                textcoords="offset points",
                xytext=(0, 3),
                ha="center",
                va="bottom",
                fontsize=8,
                color=INK_2,
            )
    ax.set_xticks(x)
    ax.set_xticklabels(RW_LABELS)
    ax.yaxis.grid(True, color=GRID, zorder=0)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    ax.legend(frameon=False, fontsize=9)


def _save(fig, path: Path) -> Path:
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return path


def write_charts(data: dict, out_dir: Path) -> list[Path]:
    _style()
    results = data["results"]
    paths: list[Path] = []

    # 1. Throughput by quorum, two concurrency levels (healthy, 1 KB).
    fig, ax = plt.subplots(figsize=(7, 4))
    groups = [
        _value_for(results, "ops_per_sec", condition="healthy", value_size=1024, concurrency=c)
        for c in (8, 32)
    ]
    _bars(ax, groups, ["concurrency 8", "concurrency 32"], SERIES[:2], ",.0f")
    ax.set_ylabel("operations / second")
    ax.set_title("Throughput by quorum setting (healthy, 1 KB values)", color=INK, fontsize=11)
    paths.append(_save(fig, out_dir / "throughput_by_quorum.png"))

    # 2. Tail latency by quorum (healthy, 1 KB, concurrency 32).
    fig, ax = plt.subplots(figsize=(7, 4))
    groups = [
        _value_for(results, m, condition="healthy", value_size=1024, concurrency=32)
        for m in ("p50_ms", "p95_ms", "p99_ms")
    ]
    _bars(ax, groups, ["p50", "p95", "p99"], SERIES, ".1f")
    ax.set_ylabel("latency (ms)")
    ax.set_title(
        "Latency by quorum setting (healthy, 1 KB, concurrency 32)", color=INK, fontsize=11
    )
    paths.append(_save(fig, out_dir / "latency_by_quorum.png"))

    # 3. Throughput: healthy vs one node killed (1 KB, concurrency 32).
    fig, ax = plt.subplots(figsize=(7, 4))
    groups = [
        _value_for(results, "ops_per_sec", condition=cond, value_size=1024, concurrency=32)
        for cond in ("healthy", "one_killed")
    ]
    _bars(ax, groups, ["healthy", "one node killed"], [SERIES[0], SERIES[1]], ",.0f")
    ax.set_ylabel("operations / second")
    ax.set_title(
        "Throughput: healthy vs one node killed (1 KB, concurrency 32)", color=INK, fontsize=11
    )
    paths.append(_save(fig, out_dir / "throughput_healthy_vs_degraded.png"))

    return paths
