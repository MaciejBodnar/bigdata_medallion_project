#!/usr/bin/env python
"""Generate human-readable markdown report and visual summary from quality metrics JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


def _load_metrics(metrics_path: Path) -> dict[str, Any]:
    with open(metrics_path) as f:
        return json.load(f)


def _generate_markdown(metrics: dict[str, Any], output_path: Path) -> None:
    primary = metrics.get("primary_data_product", "unknown")
    supporting = metrics.get("supporting_dataset", [])
    generated_at = metrics.get("generated_at_utc", "unknown")
    metrics_list = metrics.get("metrics", [])

    lines = [
        "# Data Product Quality Report",
        "",
        f"**Generated:** {generated_at}",
        "",
        f"**Primary Dataset:** `{primary}`",
        "",
        "## Quality Metrics Summary",
        "",
    ]

    all_pass = True
    for metric in metrics_list:
        name = metric.get("metric_name", "unknown")
        value = metric.get("current_value", "N/A")
        threshold = metric.get("expected_threshold", "N/A")
        unit = metric.get("unit", "")
        meets = metric.get("meets_threshold", False)
        status = "PASS" if meets else "FAIL"

        all_pass = all_pass and meets

        if unit:
            value_str = f"{value} {unit}"
        else:
            value_str = str(value)

        lines.append(f"### {name.replace('_', ' ').title()}")
        lines.append(f"- **Status:** {status}")
        lines.append(f"- **Current Value:** {value_str}")
        lines.append(f"- **Threshold:** {threshold}")
        lines.append("")

    lines.append("## Overall Status")
    overall_status = "ALL METRICS PASS" if all_pass else "SOME METRICS NEED ATTENTION"
    lines.append(f"**{overall_status}**")
    lines.append("")

    lines.append("---")
    lines.append(
        "_Report generated automatically after pipeline run. "
        "See `artifacts/data_product_quality_metrics.json` for detailed metric definitions._"
    )

    output_path.write_text("\n".join(lines), encoding="utf-8")


def _generate_chart(metrics: dict[str, Any], output_path: Path) -> None:
    if not HAS_MATPLOTLIB:
        print(f"matplotlib not available; skipping chart generation to {output_path}")
        return

    metrics_list = metrics.get("metrics", [])

    names = []
    values = []
    thresholds_normalized = []
    colors = []

    for metric in metrics_list:
        name = metric.get("metric_name", "").replace("_", "\n")
        names.append(name)

        current = metric.get("current_value")
        meets = metric.get("meets_threshold", False)
        unit = metric.get("unit", "")

        if unit == "percent":
            values.append(current if current is not None else 0)
            thresholds_normalized.append(99.5)
            colors.append("green" if meets else "red")
        elif unit == "hours":
            values.append(current if current is not None else 0)
            thresholds_normalized.append(168)
            colors.append("green" if meets else "red")
        elif unit == "rows":
            normalized_value = min(100, (current or 0) / 5) if current else 0
            values.append(normalized_value)
            thresholds_normalized.append(100)
            colors.append("green" if meets else "red")
        else:
            values.append(100 if meets else 50)
            thresholds_normalized.append(100)
            colors.append("green" if meets else "red")

    fig, ax = plt.subplots(figsize=(12, 6))
    x_pos = range(len(names))

    bars = ax.bar(x_pos, values, color=colors, alpha=0.7, edgecolor="black", linewidth=1.5)

    ax.set_ylabel("Value / Normalized Score", fontsize=12, fontweight="bold")
    ax.set_title("Data Product Quality Metrics Summary", fontsize=14, fontweight="bold")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(names, fontsize=10)
    ax.set_ylim(0, max(thresholds_normalized) * 1.1)
    ax.grid(axis="y", alpha=0.3, linestyle="--")

    for i, (bar, metric) in enumerate(zip(bars, metrics_list)):
        value = metric.get("current_value", 0)
        if value is not None:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 2,
                f"{value:.2f}" if isinstance(value, float) else str(value),
                ha="center",
                va="bottom",
                fontweight="bold",
                fontsize=9,
            )

    fig.tight_layout()
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)


def generate_quality_report(metrics_path: Path, output_dir: Path) -> None:
    """Generate markdown report and chart from metrics JSON.

    Args:
        metrics_path: Path to data_product_quality_metrics.json
        output_dir: Directory to write report.md and summary.png
    """
    if not metrics_path.exists():
        print(f"Metrics file not found: {metrics_path}")
        return

    metrics = _load_metrics(metrics_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    report_path = output_dir / "data_product_quality_report.md"
    chart_path = output_dir / "data_product_quality_summary.png"

    _generate_markdown(metrics, report_path)
    print(f"✓ Generated report: {report_path}")

    _generate_chart(metrics, chart_path)
    if HAS_MATPLOTLIB:
        print(f"✓ Generated chart: {chart_path}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        metrics_file = Path(sys.argv[1])
    else:
        metrics_file = Path("artifacts/data_product_quality_metrics.json")

    generate_quality_report(metrics_file, Path("artifacts"))
