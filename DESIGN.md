# Design System - Market Data Reliability Lab

This file is the visual source of truth for the live Grafana dashboard and the immutable HTML verification report. Read it with `docs/designs/market-data-reliability-lab.md` before changing any user-facing output.

## Product Context

- **What this is:** A local, deterministic financial market data pipeline that demonstrates Kafka-to-Spark-to-Cassandra processing and proves correctness across a real Spark driver failure and checkpoint recovery.
- **Who it is for:** Hiring managers and interviewers evaluating a fresher data engineering candidate. A secondary audience is engineers who want to reproduce the run locally.
- **Space/industry:** Financial market data, streaming data engineering, pipeline reliability, and technical portfolio evidence.
- **Project type:** A desktop-first live observability dashboard plus a responsive, immutable HTML evidence report.
- **Memorable claim:** This project proves data correctness and real recovery, not just dashboard polish.
- **Primary experience:** The terminal controls the scenario, Grafana observes the live run, and the final HTML report records evidence that cannot change underneath the reviewer.

## Aesthetic Direction

- **Direction:** Forensic Editorial Utility, combining editorial hierarchy with industrial, data-first restraint.
- **Decoration level:** Minimal. Typography, rules, spacing, and state color carry the interface.
- **Mood:** Precise, calm, inspectable, and credible. It should feel like a well-published engineering test report, not a trading terminal or a generic SaaS dashboard.
- **Light and dark posture:** The HTML report is light-first so screenshots and printed evidence remain legible. Dark mode is supported as a complete surface redesign, not a simple color inversion. Grafana may use the matching dark tokens when that improves native panel legibility.
- **Approved preview:** `C:\Users\hoang\.gstack\projects\hoang\designs\design-system-20260907\preview.html`
- **Reference sites:**
  - Grafana dashboard best practices: https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/best-practices/
  - Datadog dashboards: https://docs.datadoghq.com/getting_started/dashboards/
  - Honeycomb Boards: https://docs.honeycomb.io/investigate/observe/boards
  - Redpanda Console: https://docs.redpanda.com/streaming/current/console/
  - Confluent Control Center: https://docs.confluent.io/control-center/current/overview.html
  - SigNoz dashboards: https://signoz.io/docs/dashboards/overview/

### Safe category choices

- Use a clear verdict, a small number of focused visualizations, and a general-to-specific reading order.
- Pair every semantic color with a word and a symbol. Color alone never communicates state.
- Use UTC timestamps, tabular numbers, visible units, and direct links to deeper evidence.
- Keep the dashboard provisioned and version-controlled. It is not edited during the demo.

### Deliberate risks

- Use a warm, light evidence sheet instead of the default dark operations-room look. This improves CV screenshots and makes the report feel publishable.
- Use an editorial serif for large titles. This separates the claim from the machine-generated evidence without making body text ornamental.
- Prefer ruled sections over a mosaic of floating cards. The page reads as one argument, not a pile of unrelated widgets.

### Avoid

- No gradients, glass effects, glow, terminal-green decoration, or fake trading visuals.
- No purple default accent, rounded card wall, colored-circle icon grid, or gradient buttons.
- No auto-animated counters or charts that make a static result look live.
- No generic claims such as "production ready" without a linked invariant that supports them.
- No raw payloads in the public report. Show safe coordinates, reason codes, hashes, and a bounded escaped hex preview.

## Typography

- **Display/Hero:** Instrument Serif, weight 400. Use for product, section, and report titles only. It gives the evidence the authority of a published report without reducing data readability.
- **Body:** Source Sans 3, weights 400, 600, and 700. Use for explanations, controls, table headings, and UI labels.
- **UI/Labels:** Source Sans 3. Use 600 or 700 for compact controls and 600 for uppercase section labels.
- **Data/Tables:** IBM Plex Mono, weights 400, 500, and 600, with tabular numbers enabled. Use for run IDs, offsets, counts, hashes, durations, reason codes, and timestamps.
- **Code:** IBM Plex Mono.
- **Loading:** The design preview may load Google Fonts. Production artifacts must vendor pinned WOFF2 files and their licenses so the local demo and downloaded CI report render without network access. Use `font-display: swap` and retain metric-compatible fallbacks.
- **Grafana:** Preserve the same hierarchy, scale, weight, and data treatment within supported panel options. Do not patch Grafana's application bundle only to force custom fonts.

### Type scale

| Token | Size / line height | Use |
|---|---:|---|
| `display-xl` | 92px / 0.95 | Wide preview hero only; clamp to 48px on mobile |
| `display-lg` | 64px / 1.00 | Report title on wide screens |
| `title-1` | 48px / 1.00 | Major report section |
| `title-2` | 36px / 1.05 | Evidence section heading |
| `title-3` | 24px / 1.20 | Screen or group heading |
| `body-lg` | 20px / 1.45 | Introductory explanation |
| `body` | 16px / 1.50 | Default body and control text |
| `small` | 14px / 1.45 | Secondary explanation |
| `label` | 12px / 1.40 | Uppercase labels and table headings |
| `data-lg` | 32px / 1.00 | Primary numeric evidence |
| `data` | 12px / 1.45 | Coordinates, timestamps, hashes, and code |

Use a maximum of three hierarchy levels in one viewport. Large numbers use `font-variant-numeric: tabular-nums` and always include a visible unit or explanatory label.

## Color

- **Approach:** Restrained. Most surfaces are neutral; color appears only for interaction, current context, and semantic state.
- **Primary:** Cobalt `#1D4ED8`. Use for links, focus context, the selected run, and the lag line.
- **Secondary:** Teal `#0F766E`. Use sparingly for supporting context that is neither a verdict nor an error.
- **Success:** `#137A63` with soft background `#E1F2ED`.
- **Warning:** `#8A5A00` with soft background `#F7EDCF`.
- **Error:** `#B42318` with soft background `#FAE7E5`.
- **Info:** `#2457D6` with soft background `#E7EEFC`.

### Light neutrals

| Token | Value | Use |
|---|---|---|
| `paper` | `#F7F5F0` | Page canvas and printable background |
| `surface` | `#FFFFFF` | Tables, preview frames, and bounded controls |
| `surface-subtle` | `#EEECE7` | Code chips and quiet grouped rows |
| `ink` | `#151A21` | Primary text and strong rules |
| `muted` | `#5B6472` | Secondary text |
| `border` | `#D7DCE2` | Dividers and table rules |
| `border-strong` | `#9CA5B2` | Frame and section boundaries |

### Dark neutrals

| Token | Value | Use |
|---|---|---|
| `paper` | `#0D1117` | Dark canvas |
| `surface` | `#151B23` | Dark report surface |
| `surface-subtle` | `#1D2530` | Dark grouped rows |
| `ink` | `#E6EDF3` | Primary dark-mode text |
| `muted` | `#9DA7B4` | Secondary dark-mode text |
| `border` | `#30363D` | Dark divider |
| `border-strong` | `#596474` | Strong dark boundary |

Dark semantic colors are reduced in saturation and raised in lightness: primary `#82A6F4`, secondary `#66BCB3`, success `#63B59E`, warning `#D5A84A`, error `#E77970`, and info `#86A9EF`. Recheck contrast against each actual surface instead of assuming light-mode pairs remain valid.

## Spacing

- **Base unit:** 4px.
- **Density:** Compact-comfortable. Data rows stay efficient while headings and proof groups receive enough separation to tell a clear story.
- **Scale:** `2xs` 2px, `xs` 4px, `sm` 8px, `md` 16px, `lg` 24px, `xl` 32px, `2xl` 48px, `3xl` 64px, `4xl` 80px.
- **Control target:** Interactive controls are at least 44 by 44px even when their visible treatment is compact.
- **Section rhythm:** Major report sections use 48 to 72px vertical separation on desktop and 40 to 52px on mobile.

## Layout

- **Approach:** Grid-disciplined. Architecture diagrams and evidence tables follow the same alignment as the verdict and proof groups.
- **Desktop grid:** 12 columns from 1200px upward, 32px outer gutters, 24px column gaps.
- **Tablet grid:** 8 columns from 768px to 1199px, 24px outer gutters, 20px column gaps.
- **Mobile grid:** 4 columns below 768px, 14 to 16px outer gutters, 16px column gaps.
- **Max content width:** 1280px.
- **Border radius:** `sm` 2px, `md` 4px, `lg` 8px, `full` 9999px. Full radius is reserved for scenario or state pills.
- **Borders:** Use 1px neutral rules for grouping and 2px ink rules for section starts. Do not use shadows for routine grouping.

### Evidence order

The first live-dashboard viewport contains only:

1. Run identity, copy action, scenario, and UTC update time.
2. `data_contract_status` and `portfolio_release_status` as separate verdicts.
3. Source coverage, logical correctness, and recovery proof groups.
4. One consumer lag chart with failure and restart annotations.

The next viewport contains the invariant table and recovery timeline. The data-flow diagram comes after the evidence. Do not add a run-history picker; use the deep link `?var-run_id=<id>`.

## Motion

- **Approach:** Minimal-functional. Motion confirms state changes but never performs for the viewer.
- **Easing:** Enter `ease-out`, exit `ease-in`, position change `ease-in-out`.
- **Duration:** `micro` 80ms, `short` 160ms, `medium` 240ms. No routine interaction exceeds 240ms.
- **Allowed:** Focus treatment, button state, disclosure open/close, theme transition, and a short state-label transition.
- **Not allowed:** Count-up numbers, decorative page entrances, looping status pulses, animated chart drawing, parallax, and scroll choreography.
- **Reduced motion:** Under `prefers-reduced-motion: reduce`, remove nonessential transitions and use instant state replacement.

## Components and States

### Verdicts

- Always show the status word, a symbol, a one-line explanation, and the last-updated timestamp.
- Keep `data_contract_status` independent from `portfolio_release_status`.
- If data passes and portfolio release fails, keep the green data verdict and add an amber `NOT READY FOR CV` strip. Never collapse the two into one red or green banner.

### State language

| State | Label | Treatment |
|---|---|---|
| No run | `NO RUN SELECTED` | Neutral instructions and expected command |
| Initializing | `INITIALIZING` | Info label; show services being checked |
| Running | `RUNNING` | Info label; show latest sample timestamp |
| Failure observed | `FAILURE OBSERVED` | Error label; do not call the run failed yet |
| Recovering | `RECOVERING` | Warning label; show checkpoint reuse |
| Verifying | `VERIFYING` | Info label; show invariant progress |
| Passed | `PASSED` | Success label with report link |
| Failed | `FAILED` | Error label with the first broken invariant |
| Timed out | `TIMED OUT` | Error label with the elapsed limit |
| Unavailable | `LIVE DATA UNAVAILABLE` | Warning/error treatment with last known timestamp |

For active runs, no new sample for more than 10 seconds becomes `STATUS DELAYED`. More than 30 seconds becomes `LIVE DATA UNAVAILABLE`. Retain the last values with a stale label, show chart gaps, and display their timestamps. Completed terminal states do not become stale.

### Controls

- Primary action: open the final report.
- Secondary action: copy the run ID.
- Ghost action: view method or data contract documentation.
- The dashboard does not start, kill, restart, or clean up a run.
- `--open-dashboard` is opt-in for the CLI and may be used for recording. Do not auto-open during the default demo.

## Charts and Data Display

- Consumer lag is the single visual anchor. Annotate the hard kill and restart directly on the time axis.
- Show gaps for missing samples. Do not connect a line through unknown data.
- Pair every chart with a sentence summarizing peak, recovery time, and final value.
- Use fixed, meaningful units and comparable axes. Avoid stacked series unless addition is the intended question.
- Tables keep labels left-aligned and numeric evidence right-aligned where practical.
- Public rejected-record samples may show Kafka coordinate, reason code, SHA256 digest, and an escaped preview of no more than 64 bytes.
- Label value verification exactly as `sampled_value_integrity`. Never present it as exhaustive payload equality.

## Responsive Behavior

- **HTML report:** Fully responsive and verified at 375px, 768px, and 1440px.
- **Grafana:** Desktop-first at 1440px. Below 768px, show an orientation notice and a link to the accessible HTML report instead of claiming mobile dashboard parity.
- Two-column report areas stack in evidence order. Tables may scroll horizontally inside their own region without moving the whole page.
- Large titles clamp down on small screens; body text stays at least 16px.
- Important actions remain at least 44px tall and are not hidden behind hover.

## Accessibility

- Target WCAG 2.2 AA for the HTML report.
- Use semantic landmarks, heading order, tables with header cells, a skip link, visible focus, and descriptive link text.
- Maintain at least 4.5:1 contrast for normal text and 3:1 for large text and interface boundaries.
- Do not rely on red/green alone. Every state includes a label and symbol.
- Provide a text summary and an accessible sample table for the lag chart.
- Support keyboard operation and `prefers-reduced-motion`.
- Treat Grafana accessibility as a keyboard and contrast smoke-test target; keep the accessible HTML report linked from the first viewport.

## Content Rules

- Use plain, falsifiable language: `12,763 / 12,763 Kafka coordinates accounted for` is better than `all data processed successfully`.
- Primary timestamps are UTC. A local time may appear only as an optional tooltip or secondary annotation.
- Show elapsed duration beside important transitions.
- Use `at-least-once processing with replay-safe projections`, never `end-to-end exactly once`.
- The standard run retains 12,763 source records, 12,480 canonical trades, 246 duplicates, and 37 invalid records.
- The separately labeled `recovery-showcase` fixture may be used for the short video. Never attribute its smaller numbers to the standard run.

## Decisions Log

| Date | Decision | Rationale |
|---|---|---|
| 2026-09-07 | Use Forensic Editorial Utility | The interface must read as technical evidence for hiring review, not a generic operations product. |
| 2026-09-07 | Use a light-first, cardless evidence sheet | It produces clear CV screenshots and preserves a single argument from verdict to proof. |
| 2026-09-07 | Use Instrument Serif, Source Sans 3, and IBM Plex Mono | The roles separate the claim, explanation, and machine-verifiable evidence. |
| 2026-09-07 | Keep semantic color rare and redundant | Status must remain understandable in grayscale and for color-vision differences. |
| 2026-09-07 | Make the HTML report responsive and Grafana desktop-first | The report is the accessible shareable artifact; mobile Grafana parity is not needed for this portfolio scope. |
| 2026-09-07 | Initial system approved through `/design-consultation` | The user approved the full proposal and working preview before implementation. |
