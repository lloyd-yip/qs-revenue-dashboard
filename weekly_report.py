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
import uuid
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

from compliance_email import send_compliance_emails

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
    # appointment window — "calls date passed": shows, show rate, qual/DQ, compliance
    base        = {"start": start.isoformat(), "end": end.isoformat(), "date_by": "appointment"}
    # booked window — "generation": calls booked, lead quality sourced
    booked_base = {"start": start.isoformat(), "end": end.isoformat(), "date_by": "booked"}

    # Closes: wide 90-day window — deal cycles are weeks long, so filtering closes
    # by a 7-day call date misses deals whose 1st call was weeks ago but closed this week.
    today = date.today()
    wide_90 = {
        "start": (today - timedelta(days=90)).isoformat(),
        "end": today.isoformat(),
        "date_by": "appointment",
    }

    # Upcoming week: Mon–Sun of the current week (report fires Monday morning)
    week_monday = today - timedelta(days=today.weekday())
    week_sunday  = week_monday + timedelta(days=6)
    this_week = {
        "start": week_monday.isoformat(),
        "end":   week_sunday.isoformat(),
        "date_by": "appointment",
    }

    # Upcoming 35 days: broader forward pipeline per rep
    upcoming_35 = {
        "start": today.isoformat(),
        "end": (today + timedelta(days=35)).isoformat(),
        "date_by": "appointment",
    }

    # ── Appointment window (performance: shows, rates, compliance) ────────────
    print("  summary (appointment)...")
    summary    = api_get("/api/dashboard/summary", base)

    print("  by-rep (appointment)...")
    by_rep     = api_get("/api/dashboard/by-rep", base)

    print("  channels (appointment)...")
    channels   = api_get("/api/dashboard/channels", base)

    # ── Booked window (generation: calls booked, LQ sourced) ─────────────────
    print("  summary (booked)...")
    summary_booked  = api_get("/api/dashboard/summary",  booked_base)

    print("  by-rep (booked)...")
    by_rep_booked   = api_get("/api/dashboard/by-rep",   booked_base)

    print("  channels (booked)...")
    channels_booked = api_get("/api/dashboard/channels", booked_base)

    # ── Other fetches (unchanged) ─────────────────────────────────────────────
    print("  closes (90-day window)...")
    closes_raw = api_get("/api/dashboard/closes", wide_90)

    print("  compliance...")
    compliance = api_get("/api/dashboard/compliance", base)

    # This week's scheduled calls per rep (Mon–Sun)
    print("  this week's pipeline (Mon–Sun)...")
    this_week_pipeline = api_get("/api/dashboard/pipeline-intelligence",
                                 {**this_week, "group_by": "rep"})

    # 35-day forward calendar per rep (broader view)
    print("  upcoming pipeline (35-day)...")
    upcoming_pipeline = api_get("/api/dashboard/pipeline-intelligence",
                                {**upcoming_35, "group_by": "rep"})

    # Hot/Warm list snapshot — current-state, no date filter
    print("  stage snapshot (hot/warm)...")
    stage_snap_raw = api_get("/api/dashboard/stage-snapshot", {})

    def unwrap(d):
        return d.get("data", d) if isinstance(d, dict) else d

    by_rep_list = unwrap(by_rep)
    if isinstance(by_rep_list, dict):
        by_rep_list = by_rep_list.get("reps", [])

    by_rep_booked_list = unwrap(by_rep_booked)
    if isinstance(by_rep_booked_list, dict):
        by_rep_booked_list = by_rep_booked_list.get("reps", [])

    stage_snap_data = stage_snap_raw.get("data", {}) if isinstance(stage_snap_raw, dict) else {}

    # Per-rep LQ breakdown using BOOKED window — quality of what was generated this week
    # Collect unique rep_ids across both windows
    all_rep_ids: dict[str, str] = {}  # rep_id → rep_name
    for r in (by_rep_booked_list if isinstance(by_rep_booked_list, list) else []):
        if r.get("rep_id"):
            all_rep_ids[r["rep_id"]] = r.get("rep_name", "?")
    for r in (by_rep_list if isinstance(by_rep_list, list) else []):
        if r.get("rep_id") and r["rep_id"] not in all_rep_ids:
            all_rep_ids[r["rep_id"]] = r.get("rep_name", "?")

    lq_by_rep_id: dict[str, dict] = {}
    if all_rep_ids:
        print("  per-rep LQ breakdown (booked)...")
        for rep_id, rep_name in all_rep_ids.items():
            try:
                lq_raw  = api_get("/api/dashboard/pipeline-intelligence",
                                  {**booked_base, "group_by": "lead_quality", "rep_id": rep_id})
                lq_data = lq_raw.get("data", lq_raw) if isinstance(lq_raw, dict) else {}
                lq_rows = lq_data.get("rows", []) if isinstance(lq_data, dict) else []
                lq_by_rep_id[rep_id] = {
                    (row.get("segment") or "(Not Set)"): row.get("calls_booked_1st", 0)
                    for row in lq_rows
                    if isinstance(row, dict)
                }
            except Exception as e:
                print(f"  WARNING: LQ for {rep_name} failed — {e}", file=sys.stderr)
                lq_by_rep_id[rep_id] = {}

    return {
        # Appointment window (performance)
        "summary":            unwrap(summary),
        "by_rep":             by_rep_list,
        "channels":           unwrap(channels),
        # Booked window (generation)
        "summary_booked":     unwrap(summary_booked),
        "by_rep_booked":      by_rep_booked_list,
        "channels_booked":    unwrap(channels_booked),
        # Other
        "closes":             unwrap(closes_raw),
        "compliance":         unwrap(compliance),
        "this_week_pipeline": unwrap(this_week_pipeline),
        "upcoming_pipeline":  unwrap(upcoming_pipeline),
        "stage_snapshot":     stage_snap_data,
        "lq_by_rep_id":       lq_by_rep_id,
        "week_monday":        week_monday,
        "week_sunday":        week_sunday,
    }


# ── ANOMALY DETECTION ────────────────────────────────────────────────────────
def detect_anomalies(s: dict, reps: list, stage_snapshot: dict | None = None) -> list[str]:
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

    # Missing deal values on Hot/Warm list
    if stage_snapshot:
        team = stage_snapshot.get("team") or {}
        total_missing = (team.get("hot_missing_value") or 0) + (team.get("warm_missing_value") or 0)
        if total_missing > 0:
            flags.append(
                f"⚠️ {total_missing} Hot/Warm list opp(s) have no deal value — "
                f"reps need to fill in monetary value in GHL"
            )

    return flags or ["✅ All metrics within normal range"]


# ── BUILD HTML ────────────────────────────────────────────────────────────────
def build_html(start: date, end: date, data: dict, anomalies: list, generated_at: str) -> str:
    period_label        = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"
    # Appointment window — performance (shows, rates, compliance)
    s                   = data.get("summary") or {}
    reps                = data.get("by_rep") or []
    channels            = data.get("channels") or []
    # Booked window — generation (calls booked, LQ sourced)
    s_booked            = data.get("summary_booked") or {}
    reps_booked         = data.get("by_rep_booked") or []
    channels_booked     = data.get("channels_booked") or []
    # Other
    closes              = data.get("closes") or []
    this_week_pipeline  = data.get("this_week_pipeline") or {}
    upcoming_pipeline   = data.get("upcoming_pipeline") or {}
    stage_snapshot      = data.get("stage_snapshot") or {}
    lq_by_rep_id        = data.get("lq_by_rep_id") or {}
    week_monday         = data.get("week_monday") or date.today()
    week_sunday         = data.get("week_sunday") or (date.today() + timedelta(days=6))
    compliance_data     = data.get("compliance") or {}

    # Rep lookup maps for both windows (normalize whitespace for key)
    rep_appt_map   = {" ".join((r.get("rep_name") or "").split()): r
                      for r in (reps if isinstance(reps, list) else [])}
    rep_booked_map = {" ".join((r.get("rep_name") or "").split()): r
                      for r in (reps_booked if isinstance(reps_booked, list) else [])}
    all_rep_names  = sorted(set(list(rep_appt_map.keys()) + list(rep_booked_map.keys()))
                            - {""})

    tw_rows   = this_week_pipeline.get("rows", []) if isinstance(this_week_pipeline, dict) else []
    up_rows   = upcoming_pipeline.get("rows",  []) if isinstance(upcoming_pipeline, dict) else []
    snap_reps = stage_snapshot.get("by_rep", []) if isinstance(stage_snapshot, dict) else []
    snap_team = stage_snapshot.get("team", {})   if isinstance(stage_snapshot, dict) else {}

    # Build a quick lookup: rep_name → 2nd-call outcome_unfilled count
    # from the compliance by_rep data so we can annotate the 2nd call row in Section 1
    comp_by_rep_map: dict[str, dict] = {}
    for cr in (compliance_data.get("by_rep") or []):
        n = " ".join((cr.get("rep_name") or "").split())
        comp_by_rep_map[n] = cr

    # ── Anomaly banner ───────────────────────────────────────────────────────
    if anomalies[0].startswith("✅"):
        anomaly_html = f'<div style="color:{C["green"]};font-size:14px;">{anomalies[0]}</div>'
    else:
        anomaly_html = "".join(
            f'<div style="color:{C["yellow"]};font-size:13px;margin-bottom:6px;">{a}</div>'
            for a in anomalies
        )

    # ── SECTION 1: Team overview ─────────────────────────────────────────────
    # Booked window — generation
    b1_booked = s_booked.get("calls_booked_1st") or 0
    b2_booked = s_booked.get("calls_booked_2nd") or 0
    # Appointment window — performance
    b1_passed = s.get("calls_booked_1st") or 0   # appointments whose date passed
    b2_passed = s.get("calls_booked_2nd") or 0
    sh1       = s.get("shows_1st") or 0
    sh2       = s.get("shows_2nd") or 0
    ts        = s.get("total_shows") or 0
    comp_f    = s.get("compliance_failures") or 0
    sr1       = s.get("show_rate_1st")
    sr2       = s.get("show_rate_2nd")
    qual_r    = s.get("qualification_rate")
    dq_r      = s.get("dq_rate")

    team_outcome_unfilled = (compliance_data.get("summary") or {}).get("outcome_unfilled_count") or 0
    c2_sub = f"{sh2}/{b2_passed} showed"
    if b2_passed > 0 and sh2 == 0 and team_outcome_unfilled > 0:
        c2_sub += f" · {team_outcome_unfilled} outcome unfilled"

    overview = (
        row_html("1st Calls Booked Last Week",   str(b1_booked)) +
        row_html("1st Calls Date Passed",         str(b1_passed),  sub=f"{sh1} showed") +
        row_html("1st Call Show Rate",
                 fmt_rate(sr1),
                 color=rate_color_from_decimal(sr1, 0.70, 0.55)) +
        row_html("2nd Calls Booked Last Week",   str(b2_booked)) +
        row_html("2nd Calls Date Passed",         str(b2_passed),  sub=c2_sub) +
        row_html("2nd Call Show Rate",
                 fmt_rate(sr2),
                 color=rate_color_from_decimal(sr2, 0.75, 0.60)) +
        row_html("Qual Rate (of 1st-call shows)",
                 fmt_rate(qual_r),
                 color=rate_color_from_decimal(qual_r, 0.60, 0.45)) +
        row_html("DQ Rate",    fmt_rate(dq_r), color=C["muted"]) +
        row_html("Total Shows (1st + 2nd)", str(ts)) +
        row_html("Compliance Failures",
                 str(comp_f),
                 color=C["red"] if comp_f > 0 else C["green"])
    )

    # ── SECTION 2: Rep breakdown — two sub-tables (generation / performance) ────
    def _lq_cell(rep_id: str) -> str:
        lq_data  = lq_by_rep_id.get(rep_id, {})
        lq_great = lq_data.get("Great", 0)
        lq_ok    = lq_data.get("Ok", 0)
        lq_bare  = lq_data.get("Barely Passable", 0)
        lq_bad   = lq_data.get("Bad", 0)
        lq_ns    = lq_data.get("(Not Set)", 0)
        if not lq_data:
            return f'<span style="color:{C["muted"]};">—</span>'
        return (
            f'<span style="color:{C["green"]};font-size:11px;">{lq_great}G</span> '
            f'<span style="color:{C["accent"]};font-size:11px;">{lq_ok}Ok</span> '
            f'<span style="color:{C["yellow"]};font-size:11px;">{lq_bare}B</span>'
            + (f' <span style="color:{C["red"]};font-size:10px;">{lq_bad}✗</span>' if lq_bad else '')
            + (f' <span style="color:{C["muted"]};font-size:10px;">{lq_ns}∅</span>' if lq_ns else '')
        )

    gen_rows  = ""
    perf_rows = ""
    for rname in all_rep_names:
        rb    = rep_booked_map.get(rname, {})
        ra    = rep_appt_map.get(rname, {})
        first = rname.split()[0]
        rep_id = rb.get("rep_id") or ra.get("rep_id") or ""

        # Generation sub-table
        gen_rows += tr_html([
            first,
            str(rb.get("calls_booked_1st") or 0),
            str(rb.get("calls_booked_2nd") or 0),
            _lq_cell(rep_id),
        ])

        # Performance sub-table
        rsr1  = ra.get("show_rate_1st")
        rsr2  = ra.get("show_rate_2nd")
        rqr   = ra.get("qualification_rate")
        rcomp = ra.get("compliance_failures") or 0
        rnotl = ra.get("outcome_not_logged_count") or 0
        comp_cell = (
            f'<span style="color:{C["red"]};">{rcomp}⚠</span>'
            if rcomp else f'<span style="color:{C["green"]};">✓</span>'
        )
        perf_rows += tr_html([
            first,
            str(ra.get("calls_booked_1st") or 0),
            f'<span style="color:{rate_color_from_decimal(rsr1, 0.70, 0.55)};font-weight:700;">{fmt_rate(rsr1)}</span>',
            str(ra.get("calls_booked_2nd") or 0),
            f'<span style="color:{rate_color_from_decimal(rsr2, 0.75, 0.60)};font-weight:700;">{fmt_rate(rsr2)}</span>',
            f'<span style="color:{rate_color_from_decimal(rqr, 0.60, 0.45)};font-weight:700;">{fmt_rate(rqr)}</span>',
            comp_cell,
        ])

    sub_label = (
        f'<div style="font-size:10px;font-weight:700;letter-spacing:1.5px;'
        f'text-transform:uppercase;color:{C["muted"]};margin:0 0 8px;">'
    )
    if gen_rows:
        gen_table = (
            sub_label + "GENERATION (Booked Last Week)</div>"
            + f'<table style="width:100%;border-collapse:collapse;">'
            + th(["Rep", "C1 Booked", "C2 Booked", "Lead Quality"])
            + gen_rows + "</table>"
        )
        perf_table = (
            sub_label + "PERFORMANCE (Appointment Date Passed)</div>"
            + f'<table style="width:100%;border-collapse:collapse;margin-top:18px;">'
            + th(["Rep", "C1 Date Passed", "C1 Show%", "C2 Date Passed", "C2 Show%", "Qual%", "Comp"])
            + perf_rows + "</table>"
        )
        rep_table = gen_table + perf_table
    else:
        rep_table = f'<div style="color:{C["muted"]};">No rep data for this period.</div>'

    # ── SECTION 3: Channel breakdown — generation (booked) + performance (appt) ─
    def _ch_lq_mini(ch: dict) -> str:
        cgreat = ch.get("great_count") or 0
        cok    = ch.get("ok_count") or 0
        cbare  = ch.get("barely_passable_count") or 0
        cbad   = ch.get("bad_count") or 0
        cmiss  = ch.get("missing_data_count") or 0
        return (
            f'<span style="color:{C["green"]};font-size:11px;">{cgreat}G</span> '
            f'<span style="color:{C["accent"]};font-size:11px;">{cok}Ok</span> '
            f'<span style="color:{C["yellow"]};font-size:11px;">{cbare}B</span>'
            + (f' <span style="color:{C["red"]};font-size:10px;">{cbad}✗</span>' if cbad else '')
            + (f' <span style="color:{C["muted"]};font-size:10px;">{cmiss}∅</span>' if cmiss else '')
        )

    # Generation sub-table — booked window
    ch_gen_rows = ""
    for ch in sorted(
        (r for r in (channels_booked if isinstance(channels_booked, list) else [])
         if (r.get("total_ops") or 0) > 0),
        key=lambda r: r.get("total_ops") or 0, reverse=True,
    ):
        ch_gen_rows += tr_html([
            ch.get("channel") or "Unknown",
            str(ch.get("total_ops") or 0),
            _ch_lq_mini(ch),
        ])

    # Performance sub-table — appointment window
    ch_perf_rows = ""
    for ch in sorted(
        (r for r in (channels if isinstance(channels, list) else [])
         if (r.get("total_ops") or 0) > 0),
        key=lambda r: r.get("total_ops") or 0, reverse=True,
    ):
        cqr  = ch.get("qual_rate")
        cdqr = ch.get("dq_rate")
        ch_perf_rows += tr_html([
            ch.get("channel") or "Unknown",
            str(ch.get("total_ops") or 0),
            str(ch.get("shows") or 0),
            f'<span style="color:{rate_color_from_decimal(ch.get("show_rate_1st"), 0.70, 0.55)};font-weight:700;">{fmt_rate(ch.get("show_rate_1st"))}</span>',
            f'<span style="color:{rate_color_from_decimal(cqr, 0.60, 0.45)};font-weight:700;">{fmt_rate(cqr)}</span>',
            fmt_rate(cdqr),
        ])

    sub_label_ch = (
        f'<div style="font-size:10px;font-weight:700;letter-spacing:1.5px;'
        f'text-transform:uppercase;color:{C["muted"]};margin:0 0 8px;">'
    )
    if ch_gen_rows or ch_perf_rows:
        ch_gen_tbl = (
            sub_label_ch + "GENERATION (Booked Last Week)</div>"
            + f'<table style="width:100%;border-collapse:collapse;">'
            + th(["Channel", "Leads Booked", "Lead Quality"])
            + ch_gen_rows + "</table>"
        ) if ch_gen_rows else ""
        ch_perf_tbl = (
            sub_label_ch + "PERFORMANCE (Appointment Date Passed)</div>"
            + f'<table style="width:100%;border-collapse:collapse;margin-top:18px;">'
            + th(["Channel", "Calls Date Passed", "Showed", "Show%", "Qual%", "DQ%"])
            + ch_perf_rows + "</table>"
        ) if ch_perf_rows else ""
        channel_table = ch_gen_tbl + ch_perf_tbl
    else:
        channel_table = f'<div style="color:{C["muted"]};">No channel data for this period.</div>'

    # ── SECTION 4: Pipeline — this week + forward calendar + hot/warm value ──────
    today = date.today()

    # Part A: This week's scheduled calls (Mon–Sun)
    week_label   = f"{week_monday.strftime('%b %d')} – {week_sunday.strftime('%b %d')}"
    tw_html_rows = ""
    for pr in tw_rows:
        seg  = pr.get("segment") or "?"
        pc1b = pr.get("calls_booked_1st") or 0
        pc2b = pr.get("calls_booked_2nd") or 0
        if not (pc1b or pc2b):
            continue
        tw_html_rows += tr_html([
            seg.split()[0],
            str(pc1b),
            f'<span style="color:{C["accent"]};font-weight:700;">{pc2b}</span>',
            str(pc1b + pc2b),
        ])

    tw_table = (
        f'<div style="font-size:11px;color:{C["muted"]};font-style:italic;margin-bottom:10px;">'
        f'Appointments scheduled {week_label}</div>'
        + f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "1st Calls", "2nd Calls", "Total"])
        + tw_html_rows
        + "</table>"
    ) if tw_html_rows else f'<div style="color:{C["muted"]};">No calls scheduled this week.</div>'

    # Part B: 35-day forward calendar (broader load view)
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
        hot   = " 🔥" if pc2b >= 10 else ""
        total_c1_up += pc1b
        total_c2_up += pc2b
        up_html_rows += tr_html([
            seg.split()[0],
            str(pc1b),
            f'<span style="color:{C["accent"]};font-weight:700;">{pc2b}{hot}</span>',
            str(pc1b + pc2b),
        ])
    if up_html_rows:
        up_html_rows += tr_html([
            f'<strong style="color:{C["text"]};">TOTAL</strong>',
            f'<strong style="color:{C["text"]};">{total_c1_up}</strong>',
            f'<strong style="color:{C["accent"]};">{total_c2_up}</strong>',
            f'<strong style="color:{C["text"]};">{total_c1_up + total_c2_up}</strong>',
        ])
    up_table = (
        f'<div style="font-size:11px;color:{C["muted"]};font-style:italic;margin-bottom:10px;">'
        f'{up_period} · 🔥 = 10+ 2nd calls lined up</div>'
        + f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "1st Calls", "2nd Calls", "Total"])
        + up_html_rows
        + "</table>"
    ) if up_html_rows else f'<div style="color:{C["muted"]};">No upcoming pipeline data.</div>'

    # Part C: Hot / Warm list projected value
    HOT_DISCOUNT  = 0.50   # 50% close probability
    WARM_DISCOUNT = 0.10   # 10% close probability

    snap_rows_html  = ""
    team_hot_proj   = snap_team.get("hot_value", 0)  * HOT_DISCOUNT
    team_warm_proj  = snap_team.get("warm_value", 0) * WARM_DISCOUNT
    team_total_proj = team_hot_proj + team_warm_proj
    team_hot_miss   = snap_team.get("hot_missing_value", 0)
    team_warm_miss  = snap_team.get("warm_missing_value", 0)

    for sr in snap_reps:
        rname     = (sr.get("rep_name") or "?").split()[0]
        hot_cnt   = sr.get("hot_count", 0)
        hot_val   = sr.get("hot_value", 0.0)
        hot_miss  = sr.get("hot_missing_value", 0)
        warm_cnt  = sr.get("warm_count", 0)
        warm_val  = sr.get("warm_value", 0.0)
        warm_miss = sr.get("warm_missing_value", 0)
        if not (hot_cnt or warm_cnt):
            continue

        hot_proj  = hot_val  * HOT_DISCOUNT
        warm_proj = warm_val * WARM_DISCOUNT
        proj_total = hot_proj + warm_proj

        miss_note = ""
        if hot_miss or warm_miss:
            miss_note = (
                f'<span style="color:{C["yellow"]};font-size:10px;margin-left:4px;">'
                f'⚠ {hot_miss + warm_miss} no deal value</span>'
            )

        snap_rows_html += tr_html([
            rname,
            f'{hot_cnt}' + (f' <span style="color:{C["yellow"]};font-size:10px;">({hot_miss} no $)</span>' if hot_miss else ''),
            fmt_money(hot_proj),
            f'{warm_cnt}' + (f' <span style="color:{C["yellow"]};font-size:10px;">({warm_miss} no $)</span>' if warm_miss else ''),
            fmt_money(warm_proj),
            f'<span style="color:{C["green"]};font-weight:700;">{fmt_money(proj_total)}</span>',
        ])

    # Team totals row
    if snap_rows_html:
        miss_total = team_hot_miss + team_warm_miss
        snap_rows_html += tr_html([
            f'<strong style="color:{C["text"]};">TEAM</strong>',
            f'<strong>{snap_team.get("hot_count", 0)}</strong>',
            f'<strong style="color:{C["accent"]};">{fmt_money(team_hot_proj)}</strong>',
            f'<strong>{snap_team.get("warm_count", 0)}</strong>',
            f'<strong style="color:{C["accent"]};">{fmt_money(team_warm_proj)}</strong>',
            f'<strong style="color:{C["green"]};">{fmt_money(team_total_proj)}</strong>',
        ])

    pipeline_value_note = (
        f'<div style="font-size:11px;color:{C["muted"]};font-style:italic;margin-bottom:10px;">'
        f'Current GHL pipeline · Hot×50% + Warm×10% = projected value'
        + (f' · <span style="color:{C["yellow"]};">⚠ {team_hot_miss + team_warm_miss} opps missing deal value</span>' if (team_hot_miss + team_warm_miss) > 0 else '')
        + '</div>'
    )
    snap_table = (
        pipeline_value_note
        + f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "Hot 🔥", "Hot Proj", "Warm", "Warm Proj", "Total Proj"])
        + snap_rows_html
        + "</table>"
    ) if snap_rows_html else f'<div style="color:{C["muted"]};">No Hot/Warm list data found.</div>'

    upcoming_html = f"""
    <div style="margin-bottom:20px;">
      <div style="font-size:11px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;
                  color:{C["yellow"]};margin-bottom:8px;">THIS WEEK — {week_label}</div>
      {tw_table}
    </div>
    <div style="margin-bottom:20px;">
      <div style="font-size:11px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;
                  color:{C["yellow"]};margin-bottom:8px;">35-DAY FORWARD LOAD</div>
      {up_table}
    </div>
    <div>
      <div style="font-size:11px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;
                  color:{C["yellow"]};margin-bottom:8px;">PROJECTED PIPELINE VALUE</div>
      {snap_table}
    </div>
    """

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
  {card(overview, "WEEK AT A GLANCE", C["accent"])}

  {section_header("Section 2 — Rep Breakdown", C["blue"])}
  {card(rep_table, "BY REP · Generation (Booked) + Performance (Date Passed)", C["blue"])}

  {section_header("Section 3 — Channel Breakdown", C["purple"])}
  {card(channel_table, "BY CHANNEL · Generation (Booked) + Performance (Date Passed)", C["purple"])}

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

    stage_snap   = data.get("stage_snapshot") or {}
    anomalies    = detect_anomalies(s, reps, stage_snap)
    generated_at = datetime.now(timezone.utc).strftime("%B %d, %Y %H:%M UTC")
    html         = build_html(start, end, data, anomalies, generated_at)

    period_label = f"{start.strftime('%b %d')} – {end.strftime('%b %d')}"
    subject      = f"📊 Revenue Report — Week of {period_label}"
    print(f"Sending main report: {subject}")

    msg                = MIMEMultipart("alternative")
    msg["Message-ID"]  = f"<{uuid.uuid4()}@quantum-scaling.com>"
    msg["Subject"]     = subject
    msg["From"]        = SMTP_USER
    msg["To"]          = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, RECIPIENTS, msg.as_string())

    print(f"✅ Main report sent to {len(RECIPIENTS)} recipient(s)")

    # Send individual compliance emails to reps with outstanding outcomes
    print("Sending compliance emails...")
    compliance  = data.get("compliance") or {}
    stage_snap2 = data.get("stage_snapshot") or {}
    send_compliance_emails(compliance, start, end,
                           smtp_user=SMTP_USER, smtp_pass=SMTP_PASS,
                           stage_snapshot=stage_snap2)

    print("Done.")


if __name__ == "__main__":
    main()
