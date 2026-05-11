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

    stage_snap_data = stage_snap_raw.get("data", {}) if isinstance(stage_snap_raw, dict) else {}

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
        "summary":            unwrap(summary),
        "by_rep":             by_rep_list,
        "channels":           unwrap(channels),
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
    s                   = data.get("summary") or {}
    reps                = data.get("by_rep") or []
    channels            = data.get("channels") or []
    closes              = data.get("closes") or []
    this_week_pipeline  = data.get("this_week_pipeline") or {}
    upcoming_pipeline   = data.get("upcoming_pipeline") or {}
    stage_snapshot      = data.get("stage_snapshot") or {}
    lq_by_rep_id        = data.get("lq_by_rep_id") or {}
    week_monday         = data.get("week_monday") or date.today()
    week_sunday         = data.get("week_sunday") or (date.today() + timedelta(days=6))
    compliance_data     = data.get("compliance") or {}

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

    # 2nd call compliance: count team outcome_unfilled on 2nd calls this period.
    # The compliance endpoint flags outcome_unfilled for any appointment (incl. call2)
    # so if b2 > 0 and shows_2nd = 0 we surface how many are outcome_unfilled.
    team_outcome_unfilled = (compliance_data.get("summary") or {}).get("outcome_unfilled_count") or 0
    # We show a note on the 2nd call row when there are b2 calls and zero shows,
    # to make clear it may be a logging gap rather than all genuine no-shows.
    c2_sub = f"{sh2}/{b2} showed"
    if b2 > 0 and sh2 == 0 and team_outcome_unfilled > 0:
        c2_sub += f" · {team_outcome_unfilled} outcome unfilled"

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
                 sub=c2_sub) +
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
def build_compliance_email_html(rep_first_name: str, failures: list, period_label: str,
                                missing_value_opps: list | None = None) -> str:
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

    # Missing deal value section
    if missing_value_opps:
        mv_rows = ""
        for opp in missing_value_opps:
            opp_name = opp.get("opp_name") or "Unknown"
            bucket   = opp.get("bucket") or "hot"
            q        = opp_name.replace(" ", "+")
            ghl_url  = f"https://app.gohighlevel.com/v2/location/{GHL_LOCATION_ID}/contacts/?q={q}"
            badge_color = "#f87171" if bucket == "hot" else "#fbbf24"
            mv_rows += f"""
            <tr>
              <td style="padding:9px 12px;border-bottom:1px solid #2a2a2a;color:#e8e8e8;font-size:13px;">{opp_name}</td>
              <td style="padding:9px 12px;border-bottom:1px solid #2a2a2a;">
                <span style="color:{badge_color};font-size:11px;font-weight:700;text-transform:uppercase;">{bucket} list</span>
              </td>
              <td style="padding:9px 12px;border-bottom:1px solid #2a2a2a;">
                <a href="{ghl_url}" style="color:#60a5fa;font-size:12px;text-decoration:none;">Open in GHL →</a>
              </td>
            </tr>"""
        missing_value_section = f"""
  <div style="background:#1a1a1a;border:1px solid #fbbf24;border-radius:10px;overflow:hidden;margin-bottom:12px;">
    <div style="padding:12px 16px;background:#1f1a0a;border-bottom:1px solid #fbbf24;">
      <strong style="color:#fbbf24;font-size:13px;">⚠️ Deal value missing on {len(missing_value_opps)} Hot/Warm opp(s)</strong>
      <p style="margin:4px 0 0;color:#888;font-size:12px;">Please add monetary value in GHL so pipeline projections are accurate.</p>
    </div>
    <table style="width:100%;border-collapse:collapse;">
      <tr>
        <td style="padding:7px 12px;color:#888;font-size:11px;font-weight:700;text-transform:uppercase;border-bottom:1px solid #2a2a2a;">Opportunity</td>
        <td style="padding:7px 12px;color:#888;font-size:11px;font-weight:700;text-transform:uppercase;border-bottom:1px solid #2a2a2a;">List</td>
        <td style="padding:7px 12px;color:#888;font-size:11px;font-weight:700;text-transform:uppercase;border-bottom:1px solid #2a2a2a;">Link</td>
      </tr>
      {mv_rows}
    </table>
  </div>"""
    else:
        missing_value_section = ""

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

  <div style="background:#1a1a1a;border:1px solid #2a2a2a;border-radius:10px;padding:16px 20px;margin-bottom:12px;">
    <p style="margin:0;color:#888;font-size:12px;">
      <strong style="color:#e8e8e8;">Reminder:</strong> Every call needs: (1) outcome logged, (2) lead quality set, (3) post-call note with 50+ words. This data feeds the team dashboard and weekly report.
    </p>
  </div>

  {missing_value_section}

  <div style="text-align:center;padding:20px 0 8px;font-size:11px;color:#555;">
    Quantum Scaling · Auto-generated compliance reminder
  </div>

</div></body></html>"""


def send_compliance_emails(compliance_data: dict, start: date, end: date,
                           stage_snapshot: dict | None = None) -> None:
    """Send individual compliance reminder emails to each rep with outstanding outcomes.

    Also attaches a missing-deal-value section for any rep who has Hot/Warm opps
    without a monetary value filled in.
    """
    failures = compliance_data.get("failures", [])

    # Build per-rep missing-value lookup from stage snapshot
    # snap_reps is a list of {rep_name, hot_count, hot_missing_value, warm_count, ...}
    # We don't have opp names here — just counts. We flag the count and tell them to check GHL.
    # For the compliance email, we'll construct a synthetic list of "unknown opp name" placeholders
    # based on counts so reps know exactly how many to fix.
    mv_by_rep: dict[str, list] = {}
    if stage_snapshot:
        for sr in (stage_snapshot.get("by_rep") or []):
            canonical = " ".join((sr.get("rep_name") or "").split())
            items = []
            for bucket, miss_key in [("hot", "hot_missing_value"), ("warm", "warm_missing_value")]:
                miss_count = sr.get(miss_key) or 0
                for _ in range(miss_count):
                    items.append({"opp_name": f"(no deal value — check GHL {bucket} list)", "bucket": bucket})
            if items:
                mv_by_rep[canonical] = items

    if not failures and not any(mv_by_rep.values()):
        print("  No compliance failures or missing deal values — skipping rep emails.")
        return

    # Group failures by canonical rep name (normalize whitespace)
    by_rep: dict[str, list] = {}
    for f in failures:
        raw = f.get("rep_name") or "Unknown"
        canonical_rep = " ".join(raw.split())
        by_rep.setdefault(canonical_rep, []).append(f)

    # Also ensure reps with ONLY missing values (no call failures) get emailed
    for rep_name in mv_by_rep:
        if rep_name not in by_rep:
            by_rep[rep_name] = []

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

                first_name   = canonical.split()[0]
                count        = len(rep_failures)
                missing_opps = mv_by_rep.get(canonical, [])
                html         = build_compliance_email_html(
                    first_name, rep_failures, period_label,
                    missing_value_opps=missing_opps if missing_opps else None,
                )
                mv_count = len(missing_opps)
                parts = []
                if count:
                    parts.append(f"{count} outcome{'s' if count != 1 else ''} to log")
                if mv_count:
                    parts.append(f"{mv_count} deal value{'s' if mv_count != 1 else ''} missing")
                subject = f"⚠️ Action Required: {' · '.join(parts)} — {period_label}"

                msg                = MIMEMultipart("alternative")
                msg["Message-ID"]  = f"<{uuid.uuid4()}@quantum-scaling.com>"
                msg["Subject"]     = subject
                msg["From"]        = SMTP_USER
                msg["To"]          = email
                msg.attach(MIMEText(html, "html"))

                server.sendmail(SMTP_USER, [email], msg.as_string())
                summary_parts = []
                if count:     summary_parts.append(f"{count} failures")
                if mv_count:  summary_parts.append(f"{mv_count} missing values")
                print(f"  ✅ Compliance email → {canonical} ({email}): {', '.join(summary_parts) or 'sent'}")
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
    send_compliance_emails(compliance, start, end, stage_snapshot=stage_snap2)

    print("Done.")


if __name__ == "__main__":
    main()
