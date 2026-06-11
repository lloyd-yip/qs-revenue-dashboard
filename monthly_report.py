#!/usr/bin/env python3
"""Monthly Revenue Report — sent on the first weekday of each month.

Covers the prior complete calendar month with month-over-month comparisons
and a 3-month trend line on key metrics.
"""

import smtplib
import sys
import uuid
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

# ── Config ────────────────────────────────────────────────────────────────────
API_BASE  = "https://qs-revenue-dashboard-production.up.railway.app"
API_TOKEN = "RAILWAY_BEARER_REMOVED"
from email_config import SMTP_USER, SMTP_PASS  # loaded from .env (gitignored) — never hardcode

RECIPIENTS = ["lloyd@attractandscale.com", "scott@quantum-scaling.com", "alex@quantum-scaling.com"]

MONTH_NAMES = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

ACTIVE_REP_NAMES = {
    "Ryan Matsumori", "Melissa Fredericks", "Armando Valencia",
    "Alex Amor Gesell", "Lloyd Yip",
}

# ── Colors (matches weekly report palette) ────────────────────────────────────
C = {
    "bg":     "#0f0f0f",
    "card":   "#1a1a1a",
    "border": "#2a2a2a",
    "text":   "#f0f0f0",
    "muted":  "#888888",
    "accent": "#ff6b35",
    "green":  "#4ade80",
    "red":    "#f87171",
    "yellow": "#fbbf24",
    "blue":   "#60a5fa",
    "purple": "#c084fc",
}


# ── API helpers ───────────────────────────────────────────────────────────────
def api_get(path, params=None):
    r = requests.get(
        f"{API_BASE}{path}",
        headers={"Authorization": f"Bearer {API_TOKEN}"},
        params=params or {},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def unwrap(resp):
    if isinstance(resp, dict):
        d = resp.get("data", resp)
        if isinstance(d, dict) and "rows" in d:
            return d["rows"]
        return d
    return resp or {}


# ── HTML helpers (match weekly report style) ──────────────────────────────────
def card(content, title=None, accent_color=None):
    color  = accent_color or C["muted"]
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
    vc  = color or C["text"]
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


def section_header(text, color=None):
    color = color or C["muted"]
    return (
        f'<div style="font-size:11px;font-weight:700;letter-spacing:2px;'
        f'text-transform:uppercase;color:{color};margin:26px 0 12px;padding-left:2px;">{text}</div>'
    )


# ── Formatting helpers ────────────────────────────────────────────────────────
def pct(v):
    if v is None:
        return "—"
    return f"{v * 100:.1f}%"


def dollars(v):
    if not v:
        return "—"
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v/1_000:.0f}K"
    return f"${v:,.0f}"


def delta_html(cur, prior, fmt="pct", invert=False):
    """Return colored +/- delta badge. invert=True means lower is better."""
    if cur is None or prior is None or prior == 0:
        return ""
    diff = cur - prior
    if fmt == "pct":
        label = f"{'+' if diff >= 0 else ''}{diff * 100:.1f}pp"
    elif fmt == "dollars":
        if abs(diff) >= 1000:
            label = f"{'+' if diff >= 0 else ''}{diff/1000:.0f}K"
        else:
            label = f"{'+' if diff >= 0 else ''}{diff:.0f}"
    else:
        label = f"{'+' if diff >= 0 else ''}{diff:.1f}"

    good   = (diff >= 0) if not invert else (diff <= 0)
    color  = C["green"] if good else C["red"]
    return (
        f' <span style="font-size:11px;color:{color};'
        f'background:rgba({"74,222,128" if good else "248,113,113"},.12);'
        f'padding:2px 6px;border-radius:4px;">{label}</span>'
    )


def trend_sparkline(vals, fmt="pct"):
    """3-value text sparkline: Apr → May → Jun  9.9% → 11.2% → 8.8%"""
    parts = []
    for v in vals:
        if v is None:
            parts.append("—")
        elif fmt == "pct":
            parts.append(f"{v * 100:.1f}%")
        elif fmt == "dollars":
            parts.append(dollars(v))
        else:
            parts.append(str(v))
    return " → ".join(parts)


# ── Date helpers ──────────────────────────────────────────────────────────────
def month_range(year, month):
    last_day = monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def prev_month(year, month):
    if month == 1:
        return year - 1, 12
    return year, month - 1


def month_label(year, month):
    return f"{MONTH_NAMES[month]} {year}"


def get_report_months():
    """Return (current, prior, two_ago) as (year, month) tuples."""
    today = date.today()
    cy, cm = prev_month(today.year, today.month)
    py, pm = prev_month(cy, cm)
    ty, tm = prev_month(py, pm)
    return (cy, cm), (py, pm), (ty, tm)


def mature_close_window(year, month):
    """4-week appointment cohort ending 3 weeks before month-end.

    Every appointment in this window had ≥3 weeks to close within the month,
    making the close rate a fair reflection of actual conversion — not skewed
    by recent calls still in progress.

    Example for April 2026 (month_end = Apr 30):
      window_end   = Apr 9  (30 - 21 days)
      window_start = Mar 13 (Apr 9 - 27 days)
    """
    _, last = monthrange(year, month)
    window_end   = date(year, month, last) - timedelta(days=21)
    window_start = window_end - timedelta(days=27)
    return window_start, window_end


# ── Data fetching ─────────────────────────────────────────────────────────────
def fetch_month(year, month, print_label=None):
    start, end = month_range(year, month)
    label = month_label(year, month)
    print(f"  {print_label or label}...")

    base = {"start": start.isoformat(), "end": end.isoformat(), "date_by": "appointment"}

    summary    = unwrap(api_get("/api/dashboard/summary", base))
    by_rep_raw = unwrap(api_get("/api/dashboard/by-rep", base))
    channels   = unwrap(api_get("/api/dashboard/channels", base))
    compliance = unwrap(api_get("/api/dashboard/compliance", base))
    lq_raw     = api_get("/api/dashboard/pipeline-intelligence", {**base, "group_by": "lead_quality"})

    # by_rep: list at data level or nested
    by_rep = by_rep_raw if isinstance(by_rep_raw, list) else []

    # lq: rows nested under data.rows
    lq_data = lq_raw.get("data", {}).get("rows", []) if isinstance(lq_raw, dict) else []

    return {
        "year": year, "month": month, "label": label,
        "start": start, "end": end,
        "summary": summary if isinstance(summary, dict) else {},
        "by_rep":  [r for r in by_rep if r.get("rep_name") in ACTIVE_REP_NAMES],
        "channels": channels if isinstance(channels, list) else [],
        "compliance": compliance if isinstance(compliance, dict) else {},
        "lq_data":  lq_data,
    }


def fetch_mature_close(year, month, prior_year, prior_month):
    """Fetch close rate for the lagged cohort window (current + prior month)."""
    results = {}
    for label, y, m in [("current", year, month), ("prior", prior_year, prior_month)]:
        ws, we = mature_close_window(y, m)
        base   = {"start": ws.isoformat(), "end": we.isoformat(), "date_by": "appointment"}
        s      = unwrap(api_get("/api/dashboard/summary", base))
        results[label] = {
            "close_rate":   s.get("close_rate") if isinstance(s, dict) else None,
            "units_closed": s.get("units_closed") if isinstance(s, dict) else None,
            "window_start": ws,
            "window_end":   we,
        }
    return results


def fetch_all():
    (cy, cm), (py, pm), (ty, tm) = get_report_months()
    print("Fetching monthly data from Railway API...")
    current  = fetch_month(cy, cm, print_label="current month")
    prior    = fetch_month(py, pm, print_label="prior month")
    two_ago  = fetch_month(ty, tm, print_label="2 months ago")
    print("  mature close rate cohort...")
    mature   = fetch_mature_close(cy, cm, py, pm)
    print("  2nd call by channel...")
    ch_base  = {"start": current["start"].isoformat(), "end": current["end"].isoformat(),
                "date_by": "appointment"}
    try:
        ch_raw   = api_get("/api/dashboard/pipeline-intelligence",
                           {**ch_base, "group_by": "channel"})
        ch_2nd   = ch_raw.get("data", {}).get("rows", []) if isinstance(ch_raw, dict) else []
    except Exception:
        ch_2nd   = []
    print("  stage snapshot...")
    snapshot = unwrap(api_get("/api/dashboard/stage-snapshot"))
    print("  per-rep LQ breakdown...")
    lq_by_rep: dict[str, list] = {}
    for rep in current["by_rep"]:
        rep_id = rep.get("rep_id")
        if not rep_id:
            continue
        try:
            lq_raw = api_get(
                "/api/dashboard/pipeline-intelligence",
                {**ch_base, "group_by": "lead_quality", "rep_id": rep_id},
            )
            lq_by_rep[rep_id] = (
                lq_raw.get("data", {}).get("rows", []) if isinstance(lq_raw, dict) else []
            )
        except Exception:
            lq_by_rep[rep_id] = []

    return current, prior, two_ago, mature, ch_2nd, snapshot, lq_by_rep


# ── Section builders ──────────────────────────────────────────────────────────

def build_executive_pulse(cur, pri, mature):
    """4 headline KPIs with MoM arrows + one-line narrative."""
    cs  = cur["summary"]
    ps  = pri["summary"]
    mcr = mature.get("current", {})
    mpr = mature.get("prior", {})

    kpis = [
        ("Calls Booked",       cs.get("calls_booked_1st"),  ps.get("calls_booked_1st"),  "int", False),
        ("1st Call Show",      cs.get("show_rate_1st"),      ps.get("show_rate_1st"),     "pct", False),
        ("Mature Close Rate*", mcr.get("close_rate"),        mpr.get("close_rate"),       "pct", False),
        ("Units Closed",       mcr.get("units_closed"),      mpr.get("units_closed"),     "int", False),
    ]

    cells = ""
    for label, cur_v, pri_v, fmt, invert in kpis:
        if fmt == "pct":
            display = pct(cur_v)
        elif fmt == "dollars":
            display = dollars(cur_v)
        else:
            display = str(cur_v) if cur_v is not None else "—"

        d_html = delta_html(cur_v, pri_v, fmt=fmt, invert=invert)

        cells += (
            f'<td style="padding:0 18px 0 0;vertical-align:top;width:25%;">'
            f'<div style="font-size:26px;font-weight:800;color:{C["text"]};">'
            f'{display}</div>'
            f'<div style="font-size:11px;color:{C["muted"]};margin-top:4px;">{label}</div>'
            f'<div style="margin-top:6px;">{d_html or "<span style=\"color:" + C["muted"] + ";font-size:11px;\">vs " + pri["label"] + "</span>"}</div>'
            f'</td>'
        )

    pulse = f'<table style="width:100%;border-collapse:collapse;"><tr>{cells}</tr></table>'

    # Narrative: flag most notable metric
    close_diff = (mcr.get("close_rate") or 0) - (mpr.get("close_rate") or 0)
    show_diff  = (cs.get("show_rate_1st") or 0) - (ps.get("show_rate_1st") or 0)
    if abs(close_diff) >= 0.03:
        direction = "improved" if close_diff > 0 else "dropped"
        note = f"Mature close rate {direction} {abs(close_diff)*100:.1f}pp vs {pri['label']}."
    elif abs(show_diff) >= 0.05:
        direction = "up" if show_diff > 0 else "down"
        note = f"1st call show rate {direction} {abs(show_diff)*100:.1f}pp vs {pri['label']}."
    else:
        note = f"Metrics largely stable vs {pri['label']}."

    ws = mcr.get("window_start")
    we = mcr.get("window_end")
    footnote_dates = (
        f'{ws.strftime("%b %-d")}–{we.strftime("%b %-d")} appointments'
        if ws and we else "lagged cohort"
    )
    footnote = (
        f'<div style="margin-top:10px;">'
        f'<span style="font-size:10px;color:{C["muted"]};font-style:italic;">'
        f'* Mature Close Rate uses {footnote_dates} — ensures every call had ≥3 weeks to close.</span></div>'
    )
    narrative = (
        f'<div style="margin-top:18px;padding-top:14px;border-top:1px solid {C["border"]};">'
        f'<span style="color:{C["muted"]};font-size:12px;">📋 {note}</span></div>'
    )

    return card(pulse + narrative + footnote, f"EXECUTIVE PULSE · {cur['label'].upper()}", C["accent"])


def build_revenue_section(cur, pri, two_ago, mature):
    """Section 1: Revenue & Closes with MoM and 3-month trend."""
    cs = cur["summary"];  ps = pri["summary"];  ts = two_ago["summary"]

    rows_html = ""

    # Units closed
    rows_html += row_html(
        "Units Closed",
        str(cs.get("units_closed", 0)),
        sub=f"vs {pri['label']}: {ps.get('units_closed', 0)}"
             + (delta_html(cs.get("units_closed"), ps.get("units_closed"), fmt="int") or ""),
    )

    # Mature close rate + 3-month trend (uses lagged cohort, not raw month)
    mcr_trend = trend_sparkline(
        [mature.get("two_ago_rate"), mature["prior"]["close_rate"], mature["current"]["close_rate"]],
        fmt="pct",
    )
    rows_html += row_html(
        "Mature Close Rate*",
        pct(mature["current"]["close_rate"]),
        color=C["green"] if (mature["current"]["close_rate"] or 0) >= 0.12 else C["text"],
        sub=f"3-mo: {mcr_trend}",
    )

    # 1st call show rate + trend
    trend_show = trend_sparkline(
        [ts.get("show_rate_1st"), ps.get("show_rate_1st"), cs.get("show_rate_1st")],
        fmt="pct",
    )
    rows_html += row_html(
        "1st Call Show Rate",
        pct(cs.get("show_rate_1st")),
        color=C["green"] if (cs.get("show_rate_1st") or 0) >= 0.55 else C["yellow"],
        sub=f"3-mo: {trend_show}",
    )

    # 2nd call show rate + trend
    trend_show2 = trend_sparkline(
        [ts.get("show_rate_2nd"), ps.get("show_rate_2nd"), cs.get("show_rate_2nd")],
        fmt="pct",
    )
    rows_html += row_html(
        "2nd Call Show Rate",
        pct(cs.get("show_rate_2nd")),
        color=C["green"] if (cs.get("show_rate_2nd") or 0) >= 0.55 else C["yellow"],
        sub=f"3-mo: {trend_show2}",
    )

    # Qualification rate
    rows_html += row_html(
        "Qual Rate (of shows)",
        pct(cs.get("qualification_rate")),
        sub=f"vs {pri['label']}: {pct(ps.get('qualification_rate'))}",
    )

    # Total shows
    rows_html += row_html(
        "Total Shows",
        str(cs.get("total_shows", 0)),
        sub=f"1st: {cs.get('shows_1st',0)} · 2nd: {cs.get('shows_2nd',0)}",
    )

    return card(rows_html, "SECTION 1 — REVENUE & FUNNEL METRICS", C["green"])


def _lq_mix_html(lq_rows: list) -> str:
    """Compact colored LQ distribution: G:3 · O:8 · BP:2 · B:1"""
    lq_map = {row.get("segment", ""): row.get("calls_booked_1st", 0) for row in lq_rows}
    parts = []
    g  = lq_map.get("Great", 0)
    o  = lq_map.get("Ok", 0)
    bp = lq_map.get("Barely Passable", 0)
    b  = lq_map.get("Bad", 0)
    if g:  parts.append(f'<span style="color:{C["green"]};">G:{g}</span>')
    if o:  parts.append(f'<span style="color:{C["blue"]};">O:{o}</span>')
    if bp: parts.append(f'<span style="color:{C["yellow"]};">BP:{bp}</span>')
    if b:  parts.append(f'<span style="color:{C["red"]};">B:{b}</span>')
    return " · ".join(parts) if parts else "—"


def build_rep_scorecard(cur, pri, lq_by_rep: dict):
    """Section 2: Per-rep monthly scorecard with MoM on close rate + LQ mix."""
    cur_reps = {r["rep_name"]: r for r in cur["by_rep"]}
    pri_reps = {r["rep_name"]: r for r in pri["by_rep"]}

    # Sort by units_closed desc, then close_rate
    sorted_reps = sorted(
        cur_reps.values(),
        key=lambda r: (r.get("units_closed", 0), r.get("close_rate", 0)),
        reverse=True,
    )

    if not sorted_reps:
        return ""

    tbl = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "Calls", "Show%", "Close%", "Units", "Lead Quality", "MoM Close"])
    )

    for r in sorted_reps:
        name     = r.get("rep_name", "")
        first    = name.split()[0] if name else name
        pr       = pri_reps.get(name, {})
        close_d  = delta_html(r.get("close_rate"), pr.get("close_rate"), fmt="pct")
        rep_id   = r.get("rep_id", "")
        lq_mix   = _lq_mix_html(lq_by_rep.get(rep_id, []))

        show_c   = C["green"] if (r.get("show_rate_1st") or 0) >= 0.55 else C["yellow"]
        close_c  = C["green"] if (r.get("close_rate") or 0) >= 0.12 else C["text"]

        tbl += tr_html([
            first,
            str(r.get("calls_booked_1st", 0)),
            pct(r.get("show_rate_1st")),
            pct(r.get("close_rate")),
            str(r.get("units_closed", 0)),
            lq_mix,
            close_d or "—",
        ], colors=[C["text"], C["muted"], show_c, close_c, C["text"], C["text"], C["text"]])

    tbl += "</table>"
    return card(tbl, "SECTION 2 — REP SCORECARD · Monthly Close Rates", C["blue"])


def build_2nd_call_section(cur, pri, ch_2nd):
    """Section 3: 2nd call funnel — booking conversion + show rate, by rep and channel."""
    cur_reps = {r["rep_name"]: r for r in cur["by_rep"]}
    pri_reps = {r["rep_name"]: r for r in pri["by_rep"]}

    sorted_reps = sorted(
        cur_reps.values(),
        key=lambda r: r.get("show_rate_2nd") or 0,
        reverse=True,
    )

    # ── By Rep ────────────────────────────────────────────────────────────────
    rep_tbl = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "1st Shows", "2nd Booked", "Book%", "2nd Shows", "2nd Show%", "MoM"])
    )
    for r in sorted_reps:
        name       = r.get("rep_name", "")
        first      = name.split()[0]
        pr         = pri_reps.get(name, {})
        shows1     = r.get("shows_1st") or 0
        booked2    = r.get("calls_booked_2nd") or 0
        shows2     = r.get("shows_2nd") or 0
        show_r2    = r.get("show_rate_2nd")
        book_pct   = booked2 / shows1 if shows1 > 0 else None
        d_html     = delta_html(show_r2, pr.get("show_rate_2nd"), fmt="pct")
        show_c     = C["green"] if (show_r2 or 0) >= 0.55 else C["yellow"]
        book_c     = C["green"] if (book_pct or 0) >= 0.70 else C["yellow"]

        rep_tbl += tr_html([
            first,
            str(shows1),
            str(booked2),
            pct(book_pct),
            str(shows2),
            pct(show_r2),
            d_html or "—",
        ], colors=[C["text"], C["muted"], C["muted"], book_c, C["muted"], show_c, C["text"]])

    rep_tbl += "</table>"

    # ── By Channel ────────────────────────────────────────────────────────────
    ch_rows = [r for r in ch_2nd if r.get("segment") and r.get("segment") != "Unknown"
               and (r.get("calls_booked_2nd") or 0) > 0]
    ch_tbl_html = ""
    if ch_rows:
        ch_rows_sorted = sorted(ch_rows, key=lambda r: r.get("calls_booked_2nd") or 0, reverse=True)
        ch_tbl = (
            f'<table style="width:100%;border-collapse:collapse;margin-top:18px;">'
            + th(["Channel", "2nd Booked", "2nd Shows", "2nd Show%"])
        )
        for r in ch_rows_sorted:
            sr2   = r.get("show_rate_2nd")
            show_c = C["green"] if (sr2 or 0) >= 0.55 else C["yellow"]
            ch_tbl += tr_html([
                r["segment"][:28],
                str(r.get("calls_booked_2nd") or 0),
                str(r.get("shows_2nd") or 0),
                pct(sr2),
            ], colors=[C["text"], C["muted"], C["muted"], show_c])
        ch_tbl += "</table>"
        ch_header = (
            f'<div style="font-size:10px;font-weight:700;letter-spacing:1.5px;'
            f'text-transform:uppercase;color:{C["muted"]};margin:18px 0 8px;">'
            f'BY CHANNEL</div>'
        )
        ch_tbl_html = ch_header + ch_tbl

    note = (
        f'<div style="font-size:11px;color:{C["muted"]};margin-top:10px;">'
        f'Book% = 2nd calls booked ÷ 1st call shows. '
        f'Low book% = not getting commitment on the call. '
        f'Low show% = prospects ghosting after committing.</div>'
    )

    rep_header = (
        f'<div style="font-size:10px;font-weight:700;letter-spacing:1.5px;'
        f'text-transform:uppercase;color:{C["muted"]};margin-bottom:8px;">'
        f'BY REP</div>'
    )

    return card(
        rep_header + rep_tbl + ch_tbl_html + note,
        "SECTION 3 — 2ND CALL FUNNEL · Booking Conversion + Show Rate",
        C["blue"],
    )


def build_lq_analysis(cur):
    """Section 3: Lead quality breakdown — show rate + close rate by LQ tier."""
    lq_rows = cur.get("lq_data", [])

    ORDER = ["Great", "Ok", "Barely Passable", "Bad", "(Not Set)"]
    lq_map = {r["segment"]: r for r in lq_rows}

    tbl = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Lead Quality", "1st Calls", "Shows", "Show%", "Closes", "Close%"])
    )

    any_data = False
    for seg in ORDER:
        r = lq_map.get(seg)
        if not r:
            continue
        any_data = True
        show_c  = C["green"] if (r.get("show_rate_1st") or 0) >= 0.55 else C["yellow"]
        close_c = C["green"] if (r.get("close_rate") or 0) >= 0.12 else C["text"]
        seg_color = {
            "Great": C["green"], "Ok": C["blue"],
            "Barely Passable": C["yellow"], "Bad": C["red"],
        }.get(seg, C["muted"])

        tbl += tr_html([
            f'<span style="color:{seg_color};">{seg}</span>',
            str(r.get("calls_booked_1st", 0)),
            str(r.get("shows_1st", 0)),
            pct(r.get("show_rate_1st")),
            str(r.get("units_closed", 0)),
            pct(r.get("close_rate")),
        ], colors=[C["text"], C["muted"], C["muted"], show_c, C["text"], close_c])

    tbl += "</table>"

    if not any_data:
        return ""

    note = (
        f'<div style="font-size:11px;color:{C["muted"]};margin-top:12px;">'
        f'Tells you which lead quality tiers actually close — not just show.</div>'
    )
    return card(tbl + note, "SECTION 3 — LEAD QUALITY ANALYSIS · Show & Close Rate by Tier", C["purple"])


def build_channels_section(cur, pri):
    """Section 4: Lead sources — which channels produced shows and closes."""
    cur_ch = {r["channel"]: r for r in cur.get("channels", []) if r.get("channel") != "Unknown"}
    pri_ch = {r["channel"]: r for r in pri.get("channels", []) if r.get("channel") != "Unknown"}

    if not cur_ch:
        return ""

    sorted_ch = sorted(cur_ch.values(), key=lambda r: r.get("shows", 0), reverse=True)

    tbl = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Channel", "Leads", "Shows", "Closes", "Close%", "Lead Quality", "MoM Shows"])
    )

    for r in sorted_ch:
        ch  = r["channel"]
        pr  = pri_ch.get(ch, {})
        d   = delta_html(r.get("shows"), pr.get("shows"), fmt="int")
        close_c = C["green"] if r.get("units_closed", 0) > 0 else C["muted"]

        # LQ mix from counts already in channels API response
        g  = r.get("great_count") or 0
        o  = r.get("ok_count") or 0
        bp = r.get("barely_passable_count") or 0
        b  = r.get("bad_count") or 0
        lq_parts = []
        if g:  lq_parts.append(f'<span style="color:{C["green"]};">G:{g}</span>')
        if o:  lq_parts.append(f'<span style="color:{C["blue"]};">O:{o}</span>')
        if bp: lq_parts.append(f'<span style="color:{C["yellow"]};">BP:{bp}</span>')
        if b:  lq_parts.append(f'<span style="color:{C["red"]};">B:{b}</span>')
        lq_mix = " · ".join(lq_parts) if lq_parts else "—"

        tbl += tr_html([
            ch[:30],
            str(r.get("total_ops", 0)),
            str(r.get("shows", 0)),
            str(r.get("units_closed", 0)),
            pct(r.get("units_closed", 0) / r["shows"] if r.get("shows") else None),
            lq_mix,
            d or "—",
        ], colors=[C["text"], C["muted"], C["text"], close_c, close_c, C["text"], C["text"]])

    tbl += "</table>"
    return card(tbl, "SECTION 4 — LEAD SOURCES · Shows & Closes by Channel", C["yellow"])


def build_pipeline_snapshot(snapshot):
    """Section 5: Hot/Warm pipeline — live current state with projected value."""
    snap_reps = (snapshot or {}).get("by_rep", [])
    snap_team = (snapshot or {}).get("team", {})

    if not snap_reps:
        return ""

    tbl = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "Hot", "Hot $", "Warm", "Warm $", "Projected"])
    )

    for r in snap_reps:
        hot_proj  = (r.get("hot_value") or 0) * 0.50
        warm_proj = (r.get("warm_value") or 0) * 0.10
        proj      = hot_proj + warm_proj
        mv_warn   = ""
        if (r.get("hot_missing_value") or 0) + (r.get("warm_missing_value") or 0) > 0:
            mv_warn = f' <span style="color:{C["yellow"]};font-size:10px;">⚠</span>'

        tbl += tr_html([
            r.get("rep_name", "").split()[0],
            str(r.get("hot_count", 0)),
            dollars(r.get("hot_value")),
            str(r.get("warm_count", 0)),
            dollars(r.get("warm_value")),
            dollars(proj) + mv_warn,
        ])

    # Team totals
    t_hot_proj  = (snap_team.get("hot_value") or 0) * 0.50
    t_warm_proj = (snap_team.get("warm_value") or 0) * 0.10
    t_proj      = t_hot_proj + t_warm_proj
    tbl += tr_html([
        "<strong>Team</strong>",
        f'<strong>{snap_team.get("hot_count", 0)}</strong>',
        f'<strong>{dollars(snap_team.get("hot_value"))}</strong>',
        f'<strong>{snap_team.get("warm_count", 0)}</strong>',
        f'<strong>{dollars(snap_team.get("warm_value"))}</strong>',
        f'<strong>{dollars(t_proj)}</strong>',
    ], colors=[C["accent"]] * 6)

    tbl += "</table>"

    note = (
        f'<div style="font-size:11px;color:{C["muted"]};margin-top:10px;">'
        f'Live snapshot · Hot×50% + Warm×10% = projected value</div>'
    )
    return card(tbl + note, "SECTION 5 — PIPELINE SNAPSHOT · Live Hot/Warm List", C["accent"])


def build_compliance_summary(cur, pri):
    """Section 6: Monthly compliance rate by rep — no individual failure list."""
    cur_by_rep = {r["rep_name"]: r for r in cur.get("by_rep", [])}
    pri_by_rep = {r["rep_name"]: r for r in pri.get("by_rep", [])}

    tbl = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "Calls", "Failures", "Compliance%", "MoM"])
    )

    for name, r in sorted(cur_by_rep.items(), key=lambda x: x[0]):
        total    = r.get("calls_booked_1st", 0) + r.get("calls_booked_2nd", 0)
        failures = r.get("compliance_failures", 0)
        rate     = 1 - (failures / total) if total > 0 else 1.0

        pr       = pri_by_rep.get(name, {})
        pr_total = pr.get("calls_booked_1st", 0) + pr.get("calls_booked_2nd", 0)
        pr_fail  = pr.get("compliance_failures", 0)
        pr_rate  = 1 - (pr_fail / pr_total) if pr_total > 0 else None

        d_html   = delta_html(rate, pr_rate, fmt="pct")
        comp_c   = C["green"] if rate >= 0.85 else C["yellow"] if rate >= 0.70 else C["red"]

        tbl += tr_html([
            name.split()[0],
            str(total),
            str(failures),
            pct(rate),
            d_html or "—",
        ], colors=[C["text"], C["muted"], C["red"] if failures > 0 else C["green"], comp_c, C["text"]])

    tbl += "</table>"

    note = (
        f'<div style="font-size:11px;color:{C["muted"]};margin-top:10px;">'
        f'Individual failures surface in weekly compliance emails.</div>'
    )
    return card(tbl + note, "SECTION 6 — COMPLIANCE SUMMARY · Monthly Rate by Rep", C["muted"])


# ── Email assembly ────────────────────────────────────────────────────────────
def build_html(cur, pri, two_ago, mature, ch_2nd, snapshot, lq_by_rep):
    generated_at = datetime.now(timezone.utc).strftime("%b %d %Y %H:%M UTC")
    label        = cur["label"]

    executive   = build_executive_pulse(cur, pri, mature)
    revenue     = build_revenue_section(cur, pri, two_ago, mature)
    scorecard   = build_rep_scorecard(cur, pri, lq_by_rep)
    second_call = build_2nd_call_section(cur, pri, ch_2nd)
    lq          = build_lq_analysis(cur)
    channels    = build_channels_section(cur, pri)
    pipeline    = build_pipeline_snapshot(snapshot)
    compliance  = build_compliance_summary(cur, pri)

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:{C['bg']};font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<div style="max-width:680px;margin:0 auto;padding:32px 20px;">

  <!-- Header -->
  <div style="margin-bottom:28px;">
    <div style="font-size:11px;font-weight:700;letter-spacing:3px;text-transform:uppercase;
                color:{C['accent']};margin-bottom:8px;">Quantum Scaling</div>
    <div style="font-size:22px;font-weight:800;color:{C['text']};">Monthly Revenue Report</div>
    <div style="font-size:13px;color:{C['muted']};margin-top:4px;">
      {label} &nbsp;·&nbsp; Generated {generated_at}
    </div>
  </div>

  {executive}
  {revenue}
  {scorecard}
  {second_call}
  {lq}
  {channels}
  {pipeline}
  {compliance}

  <!-- Footer -->
  <div style="text-align:center;margin-top:32px;padding-top:20px;border-top:1px solid {C['border']};">
    <div style="font-size:11px;color:{C['muted']};">
      Quantum Scaling · Monthly Revenue Report · {label}<br>
      Sent automatically on the first weekday of each month.
    </div>
  </div>

</div>
</body>
</html>"""


# ── Send ──────────────────────────────────────────────────────────────────────
def send_report(html, cur_label):
    subject = f"📈 Monthly Report — {cur_label}"
    print(f"Sending: {subject}")

    msg                = MIMEMultipart("alternative")
    msg["Message-ID"]  = f"<{uuid.uuid4()}@quantum-scaling.com>"
    msg["Subject"]     = subject
    msg["From"]        = SMTP_USER
    msg["To"]          = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, RECIPIENTS, msg.as_string())

    print(f"✅ Monthly report sent to {len(RECIPIENTS)} recipient(s)")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    current, prior, two_ago, mature, ch_2nd, snapshot, lq_by_rep = fetch_all()

    # Attach two_ago mature close rate to mature dict for 3-month trend
    (ty, tm) = prev_month(*prev_month(
        *prev_month(date.today().year, date.today().month)
    ))
    ta_ws, ta_we = mature_close_window(ty, tm)
    ta_base = {"start": ta_ws.isoformat(), "end": ta_we.isoformat(), "date_by": "appointment"}
    ta_s    = unwrap(api_get("/api/dashboard/summary", ta_base))
    mature["two_ago_rate"] = ta_s.get("close_rate") if isinstance(ta_s, dict) else None

    html = build_html(current, prior, two_ago, mature, ch_2nd, snapshot, lq_by_rep)
    send_report(html, current["label"])


if __name__ == "__main__":
    main()
