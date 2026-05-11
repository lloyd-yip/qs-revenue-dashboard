"""
Quantum Scaling — Weekly Revenue Dashboard Report
Runs every Monday at 9am ET via Cowork scheduled task.

Hits the QS Revenue Dashboard Railway API (unauthenticated, browser-facing endpoints),
builds an HTML report, and sends it via Gmail SMTP.

Period: previous Mon–Sun by appointment date.
Metrics: calls booked, show rates, qual rates, 2nd-call show rate,
         close rates, rep breakdowns, lead quality by channel,
         upcoming pipeline snapshot, compliance flags.
"""

import smtplib
import sys
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_BASE    = "https://qs-revenue-dashboard-production.up.railway.app"
SMTP_USER   = "lloyd@quantum-scaling.com"
SMTP_PASS   = "rxxeukliingtgroq"

# Main report recipients
RECIPIENTS  = [
    "lloyd@attractandscale.com",
    # Uncomment when report is confirmed accurate:
    # "scott@quantum-scaling.com",
    # "alex@quantum-scaling.com",
    # "geri@quantum-scaling.com",
]

# Rep email map — used for individual compliance reminder emails
REP_EMAILS = {
    "Ryan Matsumori":     "ryan@quantum-scaling.com",
    "Melissa Fredericks": "melissa@quantum-scaling.com",
    "Armando Valencia":   "armando@quantum-scaling.com",
    "Alex Amor Gesell":   "alex@quantum-scaling.com",
    "Lloyd Yip":          "lloyd@attractandscale.com",
}

GHL_LOCATION_ID = "G7ZOWCq78JrzUjlLMCxt"
REQUEST_TIMEOUT = 30  # seconds

# ── STYLES ────────────────────────────────────────────────────────────────────
C = {
    "bg": "#0f0f0f", "card": "#1a1a1a", "border": "#2a2a2a",
    "text": "#e8e8e8", "muted": "#888", "accent": "#6ee7b7",
    "red": "#f87171", "yellow": "#fbbf24", "green": "#4ade80",
    "blue": "#60a5fa", "purple": "#c084fc",
}

def card(content, title=None, accent_color=None):
    color = accent_color or C["muted"]
    header = (
        f'<div style="font-size:10px;font-weight:700;letter-spacing:2px;'
        f'text-transform:uppercase;color:{color};margin-bottom:16px;">{title}</div>'
        if title else ""
    )
    return (
        f'<div style="background:{C["card"]};border:1px solid {C["border"]};'
        f'border-radius:10px;padding:22px 26px;margin-bottom:14px;">'
        f'{header}{content}</div>'
    )

def row_html(label, value, color=None, sub=None):
    vc = color or C["text"]
    sub_html = (
        f'<span style="color:{C["muted"]};font-size:11px;margin-left:6px;">{sub}</span>'
        if sub else ""
    )
    return (
        f'<div style="display:flex;justify-content:space-between;align-items:center;'
        f'padding:7px 0;border-bottom:1px solid {C["border"]};">'
        f'<span style="color:{C["muted"]};font-size:13px;">{label}</span>'
        f'<span style="font-weight:700;color:{vc};font-size:14px;">{value}{sub_html}</span></div>'
    )

def th(cells):
    return "<tr>" + "".join(
        f'<td style="padding:7px 10px;color:{C["muted"]};font-size:11px;font-weight:700;'
        f'text-transform:uppercase;letter-spacing:.8px;border-bottom:1px solid {C["border"]};">{c}</td>'
        for c in cells
    ) + "</tr>"

def tr_html(cells, colors=None):
    colors = colors or []
    return "<tr>" + "".join(
        f'<td style="padding:8px 10px;color:{colors[i] if i < len(colors) else C["text"]};'
        f'font-size:12px;border-bottom:1px solid {C["border"]};">{c}</td>'
        for i, c in enumerate(cells)
    ) + "</tr>"

def section_header(text, color):
    return (
        f'<div style="font-size:11px;font-weight:700;letter-spacing:2px;'
        f'text-transform:uppercase;color:{color};margin:26px 0 12px;padding-left:2px;">{text}</div>'
    )

def fmt_rate(rate_decimal) -> str:
    """Format a pre-computed rate (0.0–1.0) as a percentage string."""
    if rate_decimal is None:
        return "—"
    return f"{round(rate_decimal * 100, 1)}%"

def rate_color_from_decimal(rate_decimal, green=0.70, yellow=0.55) -> str:
    if rate_decimal is None:
        return C["muted"]
    return C["green"] if rate_decimal >= green else (C["yellow"] if rate_decimal >= yellow else C["red"])

def fmt_money(val):
    if not val:
        return "—"
    return f"${val:,.0f}"


# ── DATE RANGE ────────────────────────────────────────────────────────────────
def last_week_range() -> tuple[date, date]:
    """Return (Monday, Sunday) of the previous full week."""
    today = date.today()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday  = last_monday + timedelta(days=6)
    return last_monday, last_sunday


# ── API CALLS ────────────────────────────────────────────────────────────────
def api_get(path: str, params: dict) -> dict | list:
    url = f"{API_BASE}{path}"
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"  WARNING: {path} failed — {e}", file=sys.stderr)
        return {}


def fetch_all(start: date, end: date) -> dict:
    base = {"start": start.isoformat(), "end": end.isoformat(), "date_by": "appointment"}

    # Closes: wide 90-day window — deal cycles are weeks long, so filtering closes
    # by a 7-day call date misses deals whose 1st call was weeks ago but closed this week.
    today = date.today()
    wide_90 = {
        "start": (today - timedelta(days=90)).isoformat(),
        "end": today.isoformat(),
        "date_by": "appointment",
    }

    # Upcoming pipeline: next 35 days from today (scheduled calls per rep)
    upcoming = {
        "start": today.isoformat(),
        "end": (today + timedelta(days=35)).isoformat(),
        "date_by": "appointment",
    }

    print("  summary...")
    summary    = api_get("/api/dashboard/summary", base)

    print("  by-rep...")
    by_rep     = api_get("/api/dashboard/by-rep", base)

    # /channels returns show rates + qual/DQ rates + LQ breakdown per lead source
    print("  channels...")
    channels   = api_get("/api/dashboard/channels", base)

    # Closes: 90-day wide window
    print("  closes (90-day window)...")
    closes_raw = api_get("/api/dashboard/closes", wide_90)

    print("  compliance...")
    compliance = api_get("/api/dashboard/compliance", base)

    # Upcoming pipeline: next 35 days per rep (how loaded is each rep's calendar?)
    print("  upcoming pipeline (next 35 days)...")
    upcoming_pipeline = api_get("/api/dashboard/pipeline-intelligence",
                                {**upcoming, "group_by": "rep"})

    def unwrap(d):
        return d.get("data", d) if isinstance(d, dict) else d

    by_rep_list = unwrap(by_rep)
    if isinstance(by_rep_list, dict):
        by_rep_list = by_rep_list.get("reps", [])

    # Per-rep LQ breakdown: call pipeline-intelligence?group_by=lead_quality per rep
    # so Section 2 can show Great/Ok/Barely/Bad mix per rep.
    lq_by_rep_id: dict[str, dict] = {}
    if isinstance(by_rep_list, list):
        print("  per-rep LQ breakdown...")
        for r in by_rep_list:
            rep_id   = r.get("rep_id")
            rep_name = r.get("rep_name", "?")
            if not rep_id:
                continue
            try:
                lq_raw  = api_get("/api/dashboard/pipeline-intelligence",
                                  {**base, "group_by": "lead_quality", "rep_id": rep_id})
                lq_data = lq_raw.get("data", lq_raw) if isinstance(lq_raw, dict) else {}
                lq_rows = lq_data.get("rows", []) if isinstance(lq_data, dict) else []
                lq_by_rep_id[rep_id] = {
                    (row.get("segment") or "(Not Set)"): row.get("shows_1st", 0)
                    for row in lq_rows
                    if isinstance(row, dict)
                }
            except Exception as e:
                print(f"  WARNING: LQ for {rep_name} failed — {e}", file=sys.stderr)
                lq_by_rep_id[rep_id] = {}

    return {
        "summary":          unwrap(summary),
        "by_rep":           by_rep_list,
        "channels":         unwrap(channels),
        "closes":           unwrap(closes_raw),
        "compliance":       unwrap(compliance),
        "upcoming_pipeline": unwrap(upcoming_pipeline),
        "lq_by_rep_id":    lq_by_rep_id,
    }


# ── ANOMALY DETECTION ────────────────────────────────────────────────────────
def detect_anomalies(s: dict, reps: list) -> list[str]:
    flags = []

    # Team show rate — flag only if clearly unhealthy (below 50%)
    sr1 = s.get("show_rate_1st")
    if sr1 is not None and s.get("calls_booked_1st", 0) >= 5:
        if sr1 < 0.50:
            flags.append(
                f"⚠️ Team 1st-call show rate {fmt_rate(sr1)} — below 50% floor "
                f"({s.get('shows_1st', 0)} shows / {s.get('calls_booked_1st', 0)} booked)"
            )

    # Team qual rate
    qual_r = s.get("qualification_rate")
    if qual_r is not None and (s.get("total_shows") or 0) >= 5 and qual_r < 0.50:
        flags.append(f"⚠️ Qual rate {fmt_rate(qual_r)} — below 50% floor")

    # Compliance failures (any is noteworthy)
    comp = s.get("compliance_failures") or 0
    if comp > 0:
        flags.append(f"⚠️ {comp} compliance failure(s) — unfilled outcomes or missing notes")

    # Per-rep flags
    for r in (reps if isinstance(reps, list) else []):
        rsr  = r.get("show_rate_1st")
        name = (r.get("rep_name") or "Unknown").split()[0]
        booked = r.get("calls_booked_1st") or 0
        if rsr is not None and booked >= 3 and rsr < 0.50:
            flags.append(
                f"⚠️ {name} show rate {fmt_rate(rsr)} ({r.get('shows_1st', 0)}/{booked}) — below 50%"
            )
        notlogged = r.get("outcome_not_logged_count") or 0
        if notlogged >= 3:
            flags.append(f"⚠️ {name} has {notlogged} calls with outcome not logged")

    return flags or ["✅ All metrics within normal range"]


# ── BUILD HTML ────────────────────────────────────────────────────────────────
def build_html(start: date, end: date, data: dict, anomalies: list, generated_at: str) -> str:
    period_label       = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"
    s                  = data.get("summary") or {}
    reps               = data.get("by_rep") or []
    channels           = data.get("channels") or []
    closes             = data.get("closes") or []
    upcoming_pipeline  = data.get("upcoming_pipeline") or {}
    lq_by_rep_id       = data.get("lq_by_rep_id") or {}
    up_rows            = upcoming_pipeline.get("rows", []) if isinstance(upcoming_pipeline, dict) else []

    # ── Anomaly banner ───────────────────────────────────────────────────────
    if anomalies[0].startswith("✅"):
        anomaly_html = f'<div style="color:{C["green"]};font-size:14px;">{anomalies[0]}</div>'
    else:
        anomaly_html = "".join(
            f'<div style="color:{C["yellow"]};font-size:13px;margin-bottom:6px;">{a}</div>'
            for a in anomalies
        )

    # ── SECTION 1: Team overview ─────────────────────────────────────────────
    sh1     = s.get("shows_1st") or 0
    b1      = s.get("calls_booked_1st") or 0
    sh2     = s.get("shows_2nd") or 0
    b2      = s.get("calls_booked_2nd") or 0
    ts      = s.get("total_shows") or 0
    cl_won  = s.get("units_closed") or 0
    comp_f  = s.get("compliance_failures") or 0
    sr1     = s.get("show_rate_1st")       # pre-computed 0.0–1.0
    sr2     = s.get("show_rate_2nd")
    qual_r  = s.get("qualification_rate")
    dq_r    = s.get("dq_rate")

    # Note: close_rate is excluded — not meaningful on a weekly basis.
    # A close this week likely came from a call made 2–4 weeks ago; the weekly window misleads.
    overview = (
        row_html("1st Calls Booked (this week)", str(b1)) +
        row_html("1st Call Show Rate",
                 fmt_rate(sr1),
                 color=rate_color_from_decimal(sr1, 0.70, 0.55),
                 sub=f"{sh1} showed") +
        row_html("2nd Calls Added to Calendar (this week)", str(b2)) +
        row_html("2nd Call Show Rate (of previously scheduled)",
                 fmt_rate(sr2),
                 color=rate_color_from_decimal(sr2, 0.75, 0.60),
                 sub=f"{sh2}/{b2} showed") +
        row_html("Qual Rate (of 1st-call shows)",
                 fmt_rate(qual_r),
                 color=rate_color_from_decimal(qual_r, 0.60, 0.45)) +
        row_html("DQ Rate",
                 fmt_rate(dq_r),
                 color=C["muted"]) +
        row_html("Total Shows (1st + 2nd)", str(ts)) +
        row_html("Compliance Failures",
                 str(comp_f),
                 color=C["red"] if comp_f > 0 else C["green"])
    )

    # ── SECTION 2: Rep breakdown ─────────────────────────────────────────────
    # Columns: Rep, Booked, Show%, Qual%, C2 Show%, Closed, LQ Mix, Not Logged, Comp
    rep_rows_html = ""
    for r in (reps if isinstance(reps, list) else []):
        name    = (r.get("rep_name") or "?").split()[0]
        rep_id  = r.get("rep_id") or ""
        rsr1    = r.get("show_rate_1st")
        rqr     = r.get("qualification_rate")
        rsr2    = r.get("show_rate_2nd")
        rcomp   = r.get("compliance_failures") or 0
        rnotl   = r.get("outcome_not_logged_count") or 0

        # LQ mini-breakdown for this rep
        lq_data = lq_by_rep_id.get(rep_id, {})
        lq_great = lq_data.get("Great", 0)
        lq_ok    = lq_data.get("Ok", 0)
        lq_bare  = lq_data.get("Barely Passable", 0)
        lq_bad   = lq_data.get("Bad", 0)
        lq_ns    = lq_data.get("(Not Set)", 0)
        if lq_data:
            lq_cell = (
                f'<span style="color:{C["green"]};font-size:11px;">{lq_great}G</span> '
                f'<span style="color:{C["accent"]};font-size:11px;">{lq_ok}Ok</span> '
                f'<span style="color:{C["yellow"]};font-size:11px;">{lq_bare}B</span>'
                + (f' <span style="color:{C["red"]};font-size:10px;">{lq_bad}✗</span>' if lq_bad else '')
                + (f' <span style="color:{C["muted"]};font-size:10px;">{lq_ns}∅</span>' if lq_ns else '')
            )
        else:
            lq_cell = f'<span style="color:{C["muted"]};">—</span>'

        comp_cell = (
            f'<span style="color:{C["red"]};">{rcomp}⚠</span>'
            if rcomp else f'<span style="color:{C["green"]};">✓</span>'
        )
        notlogged_cell = (
            f'<span style="color:{C["yellow"]};">{rnotl}</span>'
            if rnotl else f'<span style="color:{C["muted"]};">0</span>'
        )
        rep_rows_html += tr_html([
            name,
            str(r.get("calls_booked_1st") or 0),
            f'<span style="color:{rate_color_from_decimal(rsr1, 0.70, 0.55)};font-weight:700;">{fmt_rate(rsr1)}</span>',
            f'<span style="color:{rate_color_from_decimal(rqr, 0.60, 0.45)};font-weight:700;">{fmt_rate(rqr)}</span>',
            f'<span style="color:{rate_color_from_decimal(rsr2, 0.75, 0.60)};font-weight:700;">{fmt_rate(rsr2)}</span>',
            str(r.get("units_closed") or 0),
            lq_cell,
            notlogged_cell,
            comp_cell,
        ])

    rep_table = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "Booked", "Show%", "Qual%", "C2 Show%", "Closed", "LQ Mix", "Not Logged", "Comp"])
        + rep_rows_html
        + "</table>"
    ) if rep_rows_html else f'<div style="color:{C["muted"]};">No rep data for this period.</div>'

    # ── SECTION 3: Channel breakdown ─────────────────────────────────────────
    # /channels fields: channel, total_ops, shows, qual_rate, dq_rate,
    #                   great_count, ok_count, barely_passable_count, bad_count, missing_data_count
    ch_rows_html = ""
    for ch in (channels if isinstance(channels, list) else []):
        cname  = ch.get("channel") or "Unknown"
        ctotal = ch.get("total_ops") or 0
        csh    = ch.get("shows") or 0
        cqr    = ch.get("qual_rate")
        cdqr   = ch.get("dq_rate")
        ccl    = ch.get("units_closed") or 0
        cgreat = ch.get("great_count") or 0
        cok    = ch.get("ok_count") or 0
        cbare  = ch.get("barely_passable_count") or 0
        cbad   = ch.get("bad_count") or 0
        cmiss  = ch.get("missing_data_count") or 0
        if not ctotal:
            continue
        lq_mini = (
            f'<span style="color:{C["green"]};font-size:11px;">{cgreat}G</span> '
            f'<span style="color:{C["accent"]};font-size:11px;">{cok}Ok</span> '
            f'<span style="color:{C["yellow"]};font-size:11px;">{cbare}B</span>'
            + (f' <span style="color:{C["muted"]};font-size:10px;">{cmiss}∅</span>' if cmiss else '')
        )
        ch_rows_html += tr_html([
            cname,
            str(ctotal),
            str(csh),
            f'<span style="color:{rate_color_from_decimal(cqr, 0.60, 0.45)};font-weight:700;">{fmt_rate(cqr)}</span>',
            fmt_rate(cdqr),
            lq_mini,
            str(ccl),
        ])

    channel_table = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Channel", "Opps", "Shows", "Qual%", "DQ%", "Lead Quality", "Closed"])
        + ch_rows_html
        + "</table>"
    ) if ch_rows_html else f'<div style="color:{C["muted"]};">No channel data for this period.</div>'

    # ── SECTION 4: Upcoming pipeline (next 35 days, per rep) ─────────────────
    # Shows how loaded each rep's near-term calendar is.
    # C2 heavy = rep is deep in the pipeline → 🔥 indicator.
    today        = date.today()
    look_end     = today + timedelta(days=35)
    up_period    = f"{today.strftime('%b %d')} – {look_end.strftime('%b %d')}"
    up_html_rows = ""
    total_c1_up  = 0
    total_c2_up  = 0

    for pr in up_rows:
        seg  = pr.get("segment") or "?"
        pc1b = pr.get("calls_booked_1st") or 0
        pc2b = pr.get("calls_booked_2nd") or 0
        if not (pc1b or pc2b):
            continue
        total  = pc1b + pc2b
        hot    = " 🔥" if pc2b >= 10 else ""
        name   = seg.split()[0]
        total_c1_up += pc1b
        total_c2_up += pc2b
        up_html_rows += tr_html([
            name,
            str(pc1b),
            f'<span style="color:{C["accent"]};font-weight:700;">{pc2b}{hot}</span>',
            str(total),
        ])

    # Totals row
    if up_html_rows:
        up_html_rows += tr_html([
            f'<strong style="color:{C["text"]};">TOTAL</strong>',
            f'<strong style="color:{C["text"]};">{total_c1_up}</strong>',
            f'<strong style="color:{C["accent"]};">{total_c2_up}</strong>',
            f'<strong style="color:{C["text"]};">{total_c1_up + total_c2_up}</strong>',
        ])

    upcoming_html = (
        f'<div style="font-size:11px;color:{C["muted"]};font-style:italic;margin-bottom:12px;">'
        f'Scheduled appointments from {up_period} · 🔥 = 10+ 2nd calls (hot pipeline)</div>'
        + f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "1st Calls Scheduled", "2nd Calls Scheduled", "Total"])
        + up_html_rows
        + "</table>"
    ) if up_html_rows else f'<div style="color:{C["muted"]};">No upcoming pipeline data.</div>'

    # ── SECTION 5: Recent closes (90-day window) ──────────────────────────────
    close_rows_html = ""
    for cl in (closes if isinstance(closes, list) else []):
        cname  = str(cl.get("name") or "—")
        crep   = str(cl.get("rep") or "—")
        cval   = cl.get("value") or 0
        cdate  = cl.get("close_date") or "—"
        close_rows_html += tr_html(
            [cname[:42] + ("…" if len(cname) > 42 else ""),
             crep.split()[0],
             fmt_money(cval),
             str(cdate)],
            colors=[C["text"], C["muted"], C["green"], C["muted"]]
        )

    closes_html = (
        f'<div style="font-size:11px;color:{C["muted"]};font-style:italic;margin-bottom:12px;">'
        f'90-day lookback — deals won where 1st call was in last 90 days</div>'
        + f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Deal", "Rep", "Value", "Close Date"])
        + close_rows_html
        + "</table>"
    ) if close_rows_html else f'<div style="color:{C["muted"]};">No closes in last 90 days.</div>'

    # ── Full HTML ─────────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:{C['bg']};font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<div style="max-width:680px;margin:0 auto;padding:32px 16px;">

  <div style="margin-bottom:28px;">
    <div style="font-size:10px;font-weight:700;letter-spacing:2.5px;text-transform:uppercase;color:{C['muted']};margin-bottom:6px;">QUANTUM SCALING</div>
    <h1 style="margin:0;font-size:24px;font-weight:800;color:{C['text']};">Revenue Dashboard — Weekly Report</h1>
    <div style="font-size:13px;color:{C['muted']};margin-top:5px;">
      Period: <strong style="color:{C['text']};">{period_label}</strong> &nbsp;·&nbsp;
      Generated {generated_at} &nbsp;·&nbsp; Sent every Monday 9am ET
    </div>
  </div>

  {card(anomaly_html, "ALERTS", C["green"] if anomalies[0].startswith("✅") else C["yellow"])}

  {section_header("Section 1 — Team Overview", C["accent"])}
  {card(overview, "WEEK AT A GLANCE · Appointment Date Mode", C["accent"])}

  {section_header("Section 2 — Rep Breakdown", C["blue"])}
  {card(rep_table, "PERFORMANCE BY REP · LQ Mix = shows per lead quality tier", C["blue"])}

  {section_header("Section 3 — Lead Quality by Channel", C["purple"])}
  {card(channel_table, "CHANNEL ATTRIBUTION + QUALITY SIGNAL", C["purple"])}

  {section_header("Section 4 — Upcoming Pipeline (Next 35 Days)", C["yellow"])}
  {card(upcoming_html, "SCHEDULED CALLS PER REP · FORWARD-LOOKING", C["yellow"])}

  {section_header("Section 5 — Recent Closes", C["green"])}
  {card(closes_html, "DEALS WON (90-DAY LOOKBACK)", C["green"])}

  <div style="text-align:center;padding:24px 0 8px;font-size:11px;color:{C['muted']};">
    Quantum Scaling · Revenue Dashboard · Weekly Report<br>
    Auto-generated · Sent from lloyd@quantum-scaling.com
  </div>

</div></body></html>"""

    return html


# ── COMPLIANCE EMAILS ─────────────────────────────────────────────────────────
def build_compliance_email_html(rep_first_name: str, failures: list, period_label: str) -> str:
    """Build per-rep HTML email listing outcomes that need to be logged."""
    rows = ""
    for f in failures:
        prospect  = f.get("opportunity_name") or "Unknown"
        appt_raw  = f.get("call1_appointment_date") or ""
        violation = f.get("violations") or "Outcome not logged"

        # Parse ISO date if present
        try:
            appt_dt      = datetime.fromisoformat(appt_raw.replace("Z", "+00:00"))
            appt_display = appt_dt.strftime("%b %d, %Y")
        except Exception:
            appt_display = appt_raw or "—"

        # GHL contact search URL using prospect name (no contact_id in compliance response)
        q       = prospect.replace(" ", "+")
        ghl_url = f"https://app.gohighlevel.com/v2/location/{GHL_LOCATION_ID}/contacts/?q={q}"

        rows += f"""
        <tr>
          <td style="padding:10px 12px;border-bottom:1px solid #2a2a2a;color:#e8e8e8;font-size:13px;">{prospect}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #2a2a2a;color:#888;font-size:13px;">{appt_display}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #2a2a2a;color:#fbbf24;font-size:12px;">{violation}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #2a2a2a;">
            <a href="{ghl_url}" style="color:#60a5fa;font-size:12px;text-decoration:none;">Open in GHL →</a>
          </td>
        </tr>"""

    count = len(failures)
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#0f0f0f;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<div style="max-width:640px;margin:0 auto;padding:32px 16px;">

  <div style="margin-bottom:24px;">
    <div style="font-size:10px;font-weight:700;letter-spacing:2.5px;text-transform:uppercase;color:#888;margin-bottom:6px;">QUANTUM SCALING</div>
    <h1 style="margin:0;font-size:20px;font-weight:800;color:#e8e8e8;">⚠️ Action Required: Outcomes Need Logging</h1>
    <div style="font-size:13px;color:#888;margin-top:5px;">Week of {period_label}</div>
  </div>

  <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:10px;padding:20px 24px;margin-bottom:16px;">
    <p style="margin:0 0 12px;color:#e8e8e8;font-size:14px;">
      Hey {rep_first_name}, you have <strong style="color:#fbbf24;">{count} call outcome{'' if count == 1 else 's'}</strong> that still need to be logged in GHL.
    </p>
    <p style="margin:0;color:#888;font-size:13px;">
      Please log the outcome, lead quality, and post-call notes for each call below. Click "Open in GHL" to go directly to the contact.
    </p>
  </div>

  <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:10px;overflow:hidden;margin-bottom:16px;">
    <table style="width:100%;border-collapse:collapse;">
      <tr>
        <td style="padding:8px 12px;color:#888;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;border-bottom:1px solid #2a2a2a;">Prospect</td>
        <td style="padding:8px 12px;color:#888;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;border-bottom:1px solid #2a2a2a;">Appt Date</td>
        <td style="padding:8px 12px;color:#888;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;border-bottom:1px solid #2a2a2a;">Issue</td>
        <td style="padding:8px 12px;color:#888;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;border-bottom:1px solid #2a2a2a;">Link</td>
      </tr>
      {rows}
    </table>
  </div>

  <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:10px;padding:16px 20px;">
    <p style="margin:0;color:#888;font-size:12px;">
      <strong style="color:#e8e8e8;">Reminder:</strong> Every call needs: (1) outcome logged, (2) lead quality set, (3) post-call note with 50+ words. This data feeds the team dashboard and weekly report.
    </p>
  </div>

  <div style="text-align:center;padding:20px 0 8px;font-size:11px;color:#555;">
    Quantum Scaling · Auto-generated compliance reminder
  </div>

</div></body></html>"""


def send_compliance_emails(compliance_data: dict, start: date, end: date) -> None:
    """Send individual compliance reminder emails to each rep with outstanding outcomes."""
    failures = compliance_data.get("failures", [])
    if not failures:
        print("  No compliance failures — skipping rep compliance emails.")
        return

    # Group failures by rep name
    by_rep: dict[str, list] = {}
    for f in failures:
        rep = f.get("rep_name") or "Unknown"
        by_rep.setdefault(rep, []).append(f)

    period_label = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"

    # Send all compliance emails in a single SMTP session
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SMTP_USER, SMTP_PASS)
            for rep_name, rep_failures in by_rep.items():
                # Normalize whitespace — DB sometimes stores "Melissa  Fredericks" (double space)
                canonical = " ".join(rep_name.split())
                email = REP_EMAILS.get(canonical)
                if not email:
                    print(f"  WARNING: No email for '{canonical}' — skipping compliance email",
                          file=sys.stderr)
                    continue

                first_name = canonical.split()[0]
                count      = len(rep_failures)
                html       = build_compliance_email_html(first_name, rep_failures, period_label)
                subject    = (
                    f"⚠️ Action Required: {count} outcome{'s' if count != 1 else ''} "
                    f"to log — {period_label}"
                )

                msg             = MIMEMultipart("alternative")
                msg["Subject"]  = subject
                msg["From"]     = SMTP_USER
                msg["To"]       = email
                msg.attach(MIMEText(html, "html"))

                server.sendmail(SMTP_USER, [email], msg.as_string())
                print(f"  ✅ Compliance email → {canonical} ({email}): {count} failures")
    except smtplib.SMTPException as e:
        print(f"  ERROR sending compliance emails: {e}", file=sys.stderr)


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    start, end = last_week_range()
    print(f"QS Revenue Dashboard — Weekly Report")
    print(f"Period: {start} → {end}")

    print("Fetching data from Railway API...")
    data = fetch_all(start, end)

    s    = data.get("summary") or {}
    reps = data.get("by_rep") or []
    if isinstance(reps, dict):
        reps = reps.get("reps", [])

    anomalies    = detect_anomalies(s, reps)
    generated_at = datetime.now(timezone.utc).strftime("%B %d, %Y %H:%M UTC")
    html         = build_html(start, end, data, anomalies, generated_at)

    period_label = f"{start.strftime('%b %d')} – {end.strftime('%b %d')}"
    subject      = f"📊 Revenue Report — Week of {period_label}"
    print(f"Sending main report: {subject}")

    msg             = MIMEMultipart("alternative")
    msg["Subject"]  = subject
    msg["From"]     = SMTP_USER
    msg["To"]       = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, RECIPIENTS, msg.as_string())

    print(f"✅ Main report sent to {len(RECIPIENTS)} recipient(s)")

    # Send individual compliance emails to reps with outstanding outcomes
    print("Sending compliance emails...")
    compliance = data.get("compliance") or {}
    send_compliance_emails(compliance, start, end)

    print("Done.")


if __name__ == "__main__":
    main()
