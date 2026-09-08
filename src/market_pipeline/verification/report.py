"""Render the immutable accessible evidence report without template dependencies."""

from __future__ import annotations

import html
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any


def _text(value: Any) -> str:
    if isinstance(value, dict | list | tuple):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value)


def render_report(
    report: Mapping[str, Any],
    *,
    timeline: Iterable[Mapping[str, Any]] = (),
    lag_samples: Iterable[Mapping[str, Any]] = (),
) -> str:
    checks = list(report.get("checks") or [])
    transitions = list(timeline)
    samples = list(lag_samples)
    data_status = str(report.get("data_contract_status", "FAILED"))
    release_status = str(report.get("portfolio_release_status", "NOT_READY"))
    passed = data_status == "PASSED"
    state_class = "passed" if passed else "failed"
    status_icon = "✓" if passed else "×"
    check_rows = (
        "".join(
            "<tr>"
            f"<th scope='row'><code>{html.escape(str(check.get('name', 'unknown')))}</code></th>"
            f"<td>{html.escape(_text(check.get('expected')))}</td>"
            f"<td>{html.escape(_text(check.get('actual')))}</td>"
            f"<td class={'pass' if check.get('passed') else 'fail'}>{'PASS' if check.get('passed') else 'FAIL'}</td>"
            "</tr>"
            for check in checks
        )
        or "<tr><td colspan='4'>No invariant results were recorded.</td></tr>"
    )
    timeline_rows = (
        "".join(
            "<li>"
            f"<time datetime='{html.escape(str(item.get('occurred_at', '')))}'>{html.escape(str(item.get('occurred_at', 'unknown')))}</time>"
            f"<div><strong>{html.escape(str(item.get('state', 'UNKNOWN')))}</strong><span>{html.escape(str(item.get('reason', '')))}</span></div>"
            "</li>"
            for item in transitions
        )
        or "<li><div><strong>No timeline available</strong><span>The run did not emit transitions.</span></div></li>"
    )
    lag_rows = (
        "".join(
            "<tr>"
            f"<td>{html.escape(str(item.get('sampled_at', '')))}</td>"
            f"<td>{html.escape(str(item.get('state', '')))}</td>"
            f"<td>{html.escape(str(item.get('produced_frontier', '')))}</td>"
            f"<td>{html.escape(str(item.get('processed_frontier', '')))}</td>"
            f"<td>{html.escape(str(item.get('total_lag', '')))}</td>"
            "</tr>"
            for item in samples
        )
        or "<tr><td colspan='5'>No lag samples were recorded.</td></tr>"
    )
    release_strip = (
        ""
        if release_status == "READY"
        else "<div class='release-warning' role='status'>NOT READY FOR CV — data evidence may pass, but a required portfolio artifact is missing.</div>"
    )
    run_id = html.escape(str(report.get("run_id", "unknown")))
    updated_at = html.escape(str(report.get("updated_at", "unknown")))
    scenario = html.escape(str(report.get("scenario", "unknown")))
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark"><title>Recovery evidence — {run_id}</title>
<style>
:root{{--paper:#f7f5f0;--surface:#fff;--ink:#151a21;--muted:#5b6472;--border:#d7dce2;--primary:#1d4ed8;--success:#137a63;--warning:#8a5a00;--error:#b42318;--success-soft:#e1f2ed;--warning-soft:#f7edcf;--error-soft:#fae7e5;--focus:0 0 0 3px rgba(29,78,216,.25)}}
*{{box-sizing:border-box}}html{{scroll-behavior:smooth}}body{{margin:0;background:var(--paper);color:var(--ink);font:16px/1.5 "Source Sans 3","Segoe UI",sans-serif}}a{{color:var(--primary);text-underline-offset:3px}}a:focus-visible,summary:focus-visible{{outline:0;box-shadow:var(--focus)}}code,time,td{{font-family:"IBM Plex Mono",Consolas,monospace;font-variant-numeric:tabular-nums}}.skip{{position:absolute;left:8px;top:8px;transform:translateY(-160%);background:var(--ink);color:var(--paper);padding:10px 14px}}.skip:focus{{transform:none}}header,main,footer{{width:min(calc(100% - 40px),1280px);margin:auto}}header{{padding:64px 0 32px;border-bottom:2px solid var(--ink)}}.kicker{{color:var(--primary);font:600 12px/1.4 "IBM Plex Mono",Consolas,monospace;letter-spacing:.08em;text-transform:uppercase}}h1,h2{{font-family:"Instrument Serif",Georgia,serif;font-weight:400}}h1{{font-size:clamp(42px,6vw,72px);line-height:1;margin:12px 0}}.meta{{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:var(--border);border:1px solid var(--border);margin-top:28px}}.meta div{{min-width:0;background:var(--surface);padding:14px}}.meta span{{display:block;color:var(--muted);font-size:12px;text-transform:uppercase}}.meta code,.meta time{{overflow-wrap:anywhere}}.verdict{{display:flex;gap:14px;align-items:center;margin:32px 0;padding:20px;border-left:4px solid currentColor}}.verdict.passed{{color:var(--success);background:var(--success-soft)}}.verdict.failed{{color:var(--error);background:var(--error-soft)}}.verdict b{{font-size:20px}}.release-warning{{margin:-20px 0 32px;padding:14px;border-left:4px solid var(--warning);background:var(--warning-soft);color:var(--warning);font-weight:700}}section{{padding:40px 0;border-top:1px solid var(--border)}}h2{{font-size:36px;line-height:1;margin:0 0 20px}}.grid{{display:grid;grid-template-columns:5fr 7fr;gap:32px}}.grid>*{{min-width:0}}table{{width:100%;border-collapse:collapse}}.checks-table{{table-layout:fixed}}.checks-table th:nth-child(1){{width:22%}}.checks-table th:nth-child(2),.checks-table th:nth-child(3){{width:34%}}.checks-table th:nth-child(4){{width:10%}}caption{{text-align:left;color:var(--muted);padding-bottom:10px}}th,td{{padding:10px 8px;border-bottom:1px solid var(--border);text-align:left;vertical-align:top;overflow-wrap:anywhere}}thead th{{font-size:12px;color:var(--muted);text-transform:uppercase}}tbody th{{font-weight:500}}.pass{{color:var(--success);font-weight:700}}.fail{{color:var(--error);font-weight:700}}.table-wrap{{max-width:100%;overflow-x:auto}}ol{{list-style:none;margin:0;padding:0}}li{{display:grid;grid-template-columns:180px 1fr;gap:16px;padding:10px 0;border-bottom:1px solid var(--border)}}li span{{display:block;color:var(--muted)}}details{{margin-top:20px;border:1px solid var(--border);padding:12px}}summary{{min-height:44px;display:flex;align-items:center;cursor:pointer;font-weight:700}}footer{{padding:32px 0 64px;color:var(--muted);border-top:1px solid var(--border)}}
.checks-table th:nth-child(2),.checks-table th:nth-child(3){{width:33%}}.checks-table th:nth-child(4){{width:12%}}li{{grid-template-columns:220px 1fr}}
@media(max-width:767px){{header,main,footer{{width:min(calc(100% - 28px),1280px)}}header{{padding-top:48px}}.meta,.grid{{grid-template-columns:1fr}}section{{padding:32px 0}}h2{{font-size:30px}}li{{grid-template-columns:1fr;gap:3px}}body{{font-size:16px}}}}
@media(prefers-reduced-motion:reduce){{*{{scroll-behavior:auto!important;transition:none!important}}}}
</style></head><body>
<a class="skip" href="#main">Skip to verification evidence</a>
<header><p class="kicker">Immutable verification artifact · schema v1</p><h1>Checkpoint Recovery Evidence</h1><p>Independent reconciliation for the captured Kafka run range.</p>
<div class="meta"><div><span>Run ID</span><code>{run_id}</code></div><div><span>Scenario</span><code>{scenario}</code></div><div><span>Updated UTC</span><time>{updated_at}</time></div></div></header>
<main id="main"><div class="verdict {state_class}" aria-live="polite"><span aria-hidden="true">{status_icon}</span><div><b>Data Contract {data_status.title()}</b><div>{sum(1 for check in checks if check.get('passed'))} of {len(checks)} named invariants passed.</div></div></div>{release_strip}
<div class="grid"><section aria-labelledby="checks"><h2 id="checks">Required invariants</h2><div class="table-wrap"><table class="checks-table"><caption>Expected and actual values remain adjacent.</caption><thead><tr><th>Invariant</th><th>Expected</th><th>Actual</th><th>Result</th></tr></thead><tbody>{check_rows}</tbody></table></div></section>
<section aria-labelledby="timeline"><h2 id="timeline">Recovery timeline</h2><ol>{timeline_rows}</ol></section></div>
<section aria-labelledby="lag"><h2 id="lag">Lag evidence</h2><p>Text and tabular evidence are canonical; a chart may be added without replacing them.</p><div class="table-wrap"><table><caption>Produced and processed frontiers sampled during the run.</caption><thead><tr><th>UTC timestamp</th><th>State</th><th>Produced</th><th>Processed</th><th>Lag</th></tr></thead><tbody>{lag_rows}</tbody></table></div></section>
<details><summary>How to read this report</summary><p>Source coverage proves every captured Kafka coordinate reached Bronze. Silver identity proves replay and injected duplicates converged to one logical event. DLQ reconciliation proves invalid records were quarantined with deterministic reasons. Recovery checks prove the same checkpoint resumed and caught up.</p></details></main>
<footer>This report says <strong>at-least-once processing with replay-safe projections</strong>. It does not claim end-to-end exactly-once delivery.</footer></body></html>"""


def write_report(
    path: Path,
    report: Mapping[str, Any],
    *,
    timeline: Iterable[Mapping[str, Any]] = (),
    lag_samples: Iterable[Mapping[str, Any]] = (),
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        render_report(report, timeline=timeline, lag_samples=lag_samples),
        encoding="utf-8",
        newline="\n",
    )
    temporary.replace(path)
