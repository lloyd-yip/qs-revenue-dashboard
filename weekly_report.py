"""
Quantum Scaling — Weekly Revenue Dashboard Report
Runs every Monday at 9am ET via Cowork scheduled task.

Hits the QS Revenue Dashboard Railway API (unauthenticated, browser-facing endpoints),
builds an HTML report, and sends it via Gmail SMTP.

Period: previous Mon–Sun by appointment date.
Metrics: calls booked, show rates, qual rates, 2nd-call show rate,
         close rates, rep breakdowns, lead quality by channel,
         pipeline snapshot, compliance flags.
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
RECIPIENTS  = [
    "lloyd@attractandscale.com",
    "scott@quantum-scaling.com",
    "alex@quantum-scaling.com",
    "geri@quantum-scaling.com",
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

def pct(num, den, decimals=1):
    if not den:
        return "—"
    return f"{round(num / den * 100, decimals)}%"

def rate_color(num, den, green=0.70, yellow=0.55):
    if not den:
        return C["muted"]
    r = num / den
    return C["green"] if r >= green else (C["yellow"] if r >= yellow else C["red"])

def fmt_money(val):
    if not val:
        return "—"
    return f"${val:,.0f}"


# ── DATE RANGE ────────────────────────────────────────────────────────────────
def last_week_range() -> tuple[date, date]:
    """Return (Monday, Sunday) of the previous full week."""
    today = date.today()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
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

    print("  summary...")
    summary    = api_get("/api/dashboard/summary", base)

    print("  by-rep...")
    by_rep     = api_get("/api/dashboard/by-rep", base)

    print("  channel quality...")
    channels   = api_get("/api/dashboard/channel-quality", base)

    print("  closes...")
    closes_raw = api_get("/api/dashboard/closes", base)

    print("  compliance...")
    compliance = api_get("/api/dashboard/compliance", base)

    print("  pipeline intelligence...")
    pipeline   = api_get("/api/dashboard/pipeline-intelligence", {
        "start": start.isoformat(), "end": end.isoformat(),
        "date_by": "appointment", "group_by": "rep"
    })

    return {
        "summary":    summary.get("data", summary) if isinstance(summary, dict) else summary,
        "by_rep":     by_rep.get("data", by_rep) if isinstance(by_rep, dict) else by_rep,
        "channels":   channels.get("data", channels) if isinstance(channels, dict) else channels,
        "closes":     closes_raw.get("data", closes_raw) if isinstance(closes_raw, dict) else closes_raw,
        "compliance": compliance.get("data", compliance) if isinstance(compliance, dict) else compliance,
        "pipeline":   pipeline.get("data", pipeline) if isinstance(pipeline, dict) else pipeline,
    }


# ── ANOMALY DETECTION ────────────────────────────────────────────────────────
def detect_anomalies(s: dict, reps: list) -> list[str]:
    flags = []

    b1 = s.get("bookable_1st") or 0
    sh1 = s.get("shows_1st") or 0
    if b1 >= 5:
        sr = sh1 / b1
        if sr < 0.60:
            flags.append(
                f"⚠️ Team 1st-call show rate {pct(sh1, b1)} — below 60% floor ({sh1}/{b1})"
            )

    total_sh = s.get("total_shows") or sh1 or 0
    q = s.get("qualified_shows") or 0
    if total_sh >= 5 and (q / total_sh) < 0.50:
        flags.append(
            f"⚠️ Qual rate {pct(q, total_sh)} — below 50% floor ({q}/{total_sh} qualified)"
        )

    comp = s.get("compliance_failures") or 0
    if comp > 0:
        flags.append(f"⚠️ {comp} compliance failure(s) — unfilled outcomes or missing notes")

    for r in (reps if isinstance(reps, list) else []):
        rb = r.get("bookable_1st") or 0
        rs = r.get("shows_1st") or 0
        name = (r.get("rep_name") or "Unknown").split()[0]
        if rb >= 3 and rs / rb < 0.50:
            flags.append(
                f"⚠️ {name} show rate {pct(rs, rb)} ({rs}/{rb}) — below 50%"
            )

    return flags or ["✅ All metrics within normal range"]


# ── BUILD HTML ────────────────────────────────────────────────────────────────
def build_html(start: date, end: date, data: dict, anomalies: list, generated_at: str) -> str:
    period_label = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"
    s = data.get("summary") or {}
    reps = data.get("by_rep") or []
    if isinstance(reps, dict):
        reps = reps.get("reps", [])
    channels = data.get("channels") or []
    if isinstance(channels, dict):
        channels = channels.get("rows", [])
    closes = data.get("closes") or []
    if isinstance(closes, dict):
        closes = closes.get("closes", [])
    compliance = data.get("compliance") or {}
    pipeline = data.get("pipeline") or {}
    pipe_rows_data = pipeline.get("rows", []) if isinstance(pipeline, dict) else []

    # ── Anomaly banner ───────────────────────────────────────────────────────
    if anomalies[0].startswith("✅"):
        anomaly_html = f'<div style="color:{C["green"]};font-size:14px;">{anomalies[0]}</div>'
    else:
        anomaly_html = "".join(
            f'<div style="color:{C["yellow"]};font-size:13px;margin-bottom:6px;">{a}</div>'
            for a in anomalies
        )

    # ── Team overview ────────────────────────────────────────────────────────
    b1  = s.get("bookable_1st") or 0
    sh1 = s.get("shows_1st") or 0
    b2  = s.get("bookable_2nd") or 0
    sh2 = s.get("shows_2nd") or 0
    q   = s.get("qualified_shows") or 0
    ts  = s.get("total_shows") or sh1 or 0
    dq  = s.get("dq_count") or 0
    cl  = s.get("units_closed") or 0
    rev = s.get("projected_contract_value") or 0
    comp_f = s.get("compliance_failures") or 0

    overview = (
        row_html("1st Calls Booked",    str(s.get("calls_booked_1st") or 0)) +
        row_html("1st Call Show Rate",
                 pct(sh1, b1), color=rate_color(sh1, b1, 0.70, 0.55),
                 sub=f"{sh1}/{b1}") +
        row_html("2nd Calls Booked",    str(s.get("calls_booked_2nd") or 0)) +
        row_html("2nd Call Show Rate",
                 pct(sh2, b2), color=rate_color(sh2, b2, 0.75, 0.60),
                 sub=f"{sh2}/{b2}") +
        row_html("Qual Rate (of shows)",
                 pct(q, ts), color=rate_color(q, ts, 0.60, 0.45),
                 sub=f"{q} qualified") +
        row_html("DQ Rate",
                 pct(dq, ts), color=C["muted"]) +
        row_html("Units Closed",
                 str(cl),
                 color=C["green"] if cl > 0 else C["muted"]) +
        row_html("Revenue (Projected)",
                 fmt_money(rev),
                 color=C["green"] if rev > 0 else C["muted"]) +
        row_html("Compliance Failures",
                 str(comp_f),
                 color=C["red"] if comp_f > 0 else C["green"])
    )

    # ── Rep breakdown table ──────────────────────────────────────────────────
    rep_rows_html = ""
    for r in reps:
        rb  = r.get("bookable_1st") or 0
        rs  = r.get("shows_1st") or 0
        rb2 = r.get("bookable_2nd") or 0
        rs2 = r.get("shows_2nd") or 0
        rq  = r.get("qualified_shows") or 0
        rts = r.get("total_shows") or rs or 0
        rc  = r.get("units_closed") or 0
        rcompf = r.get("compliance_failures") or 0
        name = (r.get("rep_name") or "?").split()[0]

        lq_great  = r.get("lq_great") or 0
        lq_ok     = r.get("lq_ok") or 0
        lq_barely = r.get("lq_barely") or 0
        lq_bad    = r.get("lq_bad") or 0
        lq_miss   = r.get("lq_missing") or 0

        lq_html = (
            f'<span style="color:{C["green"]};">{lq_great}</span>'
            f'<span style="color:{C["muted"]};font-size:10px;"> G</span>&nbsp;'
            f'<span style="color:{C["accent"]};">{lq_ok}</span>'
            f'<span style="color:{C["muted"]};font-size:10px;"> Ok</span>&nbsp;'
            f'<span style="color:{C["yellow"]};">{lq_barely}</span>'
            f'<span style="color:{C["muted"]};font-size:10px;"> Bare</span>&nbsp;'
            f'<span style="color:{C["red"]};">{lq_bad}</span>'
            f'<span style="color:{C["muted"]};font-size:10px;"> Bad</span>'
        )
        if lq_miss:
            lq_html += f'&nbsp;<span style="color:{C["muted"]};font-size:10px;">({lq_miss} ∅)</span>'

        comp_cell = (
            f'<span style="color:{C["red"]};">{rcompf}</span>'
            if rcompf else f'<span style="color:{C["green"]};">✓</span>'
        )
        rep_rows_html += tr_html([
            name,
            str(r.get("calls_booked_1st") or 0),
            f'<span style="color:{rate_color(rs, rb, 0.70, 0.55)};font-weight:700;">{pct(rs, rb)}</span>',
            f'<span style="color:{rate_color(rq, rts, 0.60, 0.45)};font-weight:700;">{pct(rq, rts)}</span>',
            pct(rs2, rb2),
            str(rc),
            lq_html,
            comp_cell,
        ])

    rep_table = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "Booked", "Show%", "Qual%", "C2 Show%", "Closed", "Lead Quality", "Comp"])
        + rep_rows_html
        + "</table>"
    ) if rep_rows_html else f'<div style="color:{C["muted"]};">No rep data for this period.</div>'

    # ── Channel quality table ────────────────────────────────────────────────
    ch_rows_html = ""
    for ch in (channels if isinstance(channels, list) else []):
        cname  = ch.get("channel") or ch.get("segment") or "Unknown"
        csh    = ch.get("shows") or 0
        cbk    = ch.get("bookable") or ch.get("total_opps") or 0
        cq     = ch.get("qualified") or ch.get("qualified_shows") or 0
        cdq    = ch.get("dq") or ch.get("dq_count") or 0
        ccl    = ch.get("units_closed") or 0
        ctotal = ch.get("total_opps") or cbk or 0
        if not ctotal:
            continue
        ch_rows_html += tr_html([
            cname,
            str(ctotal),
            f'<span style="color:{rate_color(csh, cbk, 0.70, 0.55)};font-weight:700;">{pct(csh, cbk)}</span>',
            f'<span style="color:{rate_color(cq, csh, 0.60, 0.45)};font-weight:700;">{pct(cq, csh)}</span>',
            pct(cdq, csh),
            str(ccl),
        ])

    channel_table = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Channel", "Opps", "Show%", "Qual%", "DQ%", "Closed"])
        + ch_rows_html
        + "</table>"
    ) if ch_rows_html else f'<div style="color:{C["muted"]};">No channel data for this period.</div>'

    # ── Pipeline snapshot (rep breakdown by stage) ───────────────────────────
    # pipeline-intelligence grouped by rep shows current pipeline state
    UPCOMING_STAGE   = "e82907fd-4d76-4c1a-a867-b82c1093a88d"
    SECOND_CALL_DONE = "10e6b1ef-0685-4f73-b3c7-b5006b7bc311"

    pipe_html_rows = ""
    for pr in pipe_rows_data:
        seg    = pr.get("segment") or "?"
        booked = pr.get("calls_booked_1st") or 0
        shows  = pr.get("shows_1st") or 0
        c2b    = pr.get("calls_booked_2nd") or 0
        c2s    = pr.get("shows_2nd") or 0
        won    = pr.get("units_closed") or 0
        rev_p  = pr.get("projected_contract_value") or 0
        if not (booked or shows or won):
            continue
        pipe_html_rows += tr_html([
            seg.split()[0],
            str(booked),
            pct(shows, pr.get("bookable_1st") or 0),
            str(c2b),
            str(won),
            f'<span style="color:{C["accent"]};font-weight:700;">{fmt_money(rev_p)}</span>',
        ])

    pipeline_html = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Rep", "C1 Booked", "C1 Show%", "C2 Booked", "Closed", "Proj. Value"])
        + pipe_html_rows
        + "</table>"
        + f'<div style="font-size:11px;color:{C["muted"]};font-style:italic;margin-top:10px;">'
        f'Projected close month: requires GHL custom field sync — '
        f'add field ID to CUSTOM_FIELD_IDS in ghl_client.py to enable.</div>'
    ) if pipe_html_rows else f'<div style="color:{C["muted"]};">No pipeline data.</div>'

    # ── Recent closes ────────────────────────────────────────────────────────
    close_rows_html = ""
    for cl in (closes if isinstance(closes, list) else []):
        cname  = cl.get("name") or cl.get("opportunity_name") or "—"
        crep   = cl.get("rep") or cl.get("rep_name") or "—"
        cval   = cl.get("value") or cl.get("monetary_value") or 0
        cdate  = cl.get("close_date") or cl.get("date") or "—"
        close_rows_html += tr_html(
            [cname[:40] + ("…" if len(str(cname)) > 40 else ""),
             str(crep).split()[0],
             fmt_money(cval),
             str(cdate)],
            colors=[C["text"], C["muted"], C["green"], C["muted"]]
        )

    closes_html = (
        f'<table style="width:100%;border-collapse:collapse;">'
        + th(["Deal", "Rep", "Value", "Close Date"])
        + close_rows_html
        + "</table>"
    ) if close_rows_html else f'<div style="color:{C["muted"]};">No closes in this period.</div>'

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
  {card(rep_table, "PERFORMANCE BY REP", C["blue"])}

  {section_header("Section 3 — Lead Quality by Channel", C["purple"])}
  {card(channel_table, "CHANNEL ATTRIBUTION + QUALITY SIGNAL", C["purple"])}

  {section_header("Section 4 — Pipeline Performance (Period)", C["yellow"])}
  {card(pipeline_html, "CALLS → PIPELINE BY REP", C["yellow"])}

  {section_header("Section 5 — Recent Closes", C["green"])}
  {card(closes_html, "DEALS WON (PERIOD)", C["green"])}

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

    s = data.get("summary") or {}
    reps = data.get("by_rep") or []
    if isinstance(reps, dict):
        reps = reps.get("reps", [])

    anomalies = detect_anomalies(s, reps)

    generated_at = datetime.now(timezone.utc).strftime("%B %d, %Y %H:%M UTC")
    html = build_html(start, end, data, anomalies, generated_at)

    period_label = f"{start.strftime('%b %d')} – {end.strftime('%b %d')}"
    subject = f"📊 Revenue Report — Week of {period_label}"
    print(f"Sending: {subject}")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, RECIPIENTS, msg.as_string())

    print(f"✅ Report sent to {len(RECIPIENTS)} recipients")


if __name__ == "__main__":
    main()
