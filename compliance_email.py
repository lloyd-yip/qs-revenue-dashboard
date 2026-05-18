"""Per-rep compliance reminder emails — extracted from weekly_report.py.

Sends individual HTML emails to reps with outstanding call outcomes or
missing deal values on their Hot/Warm list.
"""

import smtplib
import uuid
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

GHL_LOCATION_ID = "G7ZOWCq78JrzUjlLMCxt"

# Rep email map — used for individual compliance reminder emails
REP_EMAILS = {
    "Ryan Matsumori":     "ryan@quantum-scaling.com",
    "Melissa Fredericks": "melissa@quantum-scaling.com",
    "Armando Valencia":   "armando@quantum-scaling.com",
    "Alex Amor Gesell":   "alex@quantum-scaling.com",
    "Lloyd Yip":          "lloyd@attractandscale.com",
}


def build_compliance_email_html(rep_first_name: str, failures: list, period_label: str,
                                missing_value_opps: list | None = None) -> str:
    """Build per-rep HTML email listing outcomes that need to be logged."""
    rows = ""
    for f in failures:
        prospect  = f.get("opportunity_name") or "Unknown"
        appt_raw  = f.get("call1_appointment_date") or ""
        violation = f.get("violations") or "Outcome not logged"

        try:
            appt_dt      = datetime.fromisoformat(appt_raw.replace("Z", "+00:00"))
            appt_display = appt_dt.strftime("%b %d, %Y")
        except Exception:
            appt_display = appt_raw or "—"

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

    if missing_value_opps:
        mv_rows = ""
        for opp in missing_value_opps:
            opp_name    = opp.get("opp_name") or "Unknown"
            bucket      = opp.get("bucket") or "hot"
            q           = opp_name.replace(" ", "+")
            ghl_url     = f"https://app.gohighlevel.com/v2/location/{GHL_LOCATION_ID}/contacts/?q={q}"
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


def send_compliance_emails(
    compliance_data: dict,
    start: date,
    end: date,
    smtp_user: str,
    smtp_pass: str,
    stage_snapshot: dict | None = None,
) -> None:
    """Send individual compliance reminder emails to each rep with outstanding outcomes."""
    failures = compliance_data.get("failures", [])

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

    by_rep: dict[str, list] = {}
    for f in failures:
        raw           = f.get("rep_name") or "Unknown"
        canonical_rep = " ".join(raw.split())
        by_rep.setdefault(canonical_rep, []).append(f)

    for rep_name in mv_by_rep:
        if rep_name not in by_rep:
            by_rep[rep_name] = []

    period_label = f"{start.strftime('%b %d')} – {end.strftime('%b %d, %Y')}"

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(smtp_user, smtp_pass)
            for rep_name, rep_failures in by_rep.items():
                canonical    = " ".join(rep_name.split())
                email        = REP_EMAILS.get(canonical)
                if not email:
                    print(f"  WARNING: No email for '{canonical}' — skipping", flush=True)
                    continue

                first_name   = canonical.split()[0]
                count        = len(rep_failures)
                missing_opps = mv_by_rep.get(canonical, [])
                html         = build_compliance_email_html(
                    first_name, rep_failures, period_label,
                    missing_value_opps=missing_opps if missing_opps else None,
                )
                mv_count = len(missing_opps)
                parts    = []
                if count:    parts.append(f"{count} outcome{'s' if count != 1 else ''} to log")
                if mv_count: parts.append(f"{mv_count} deal value{'s' if mv_count != 1 else ''} missing")
                subject = f"⚠️ Action Required: {' · '.join(parts)} — {period_label}"

                msg               = MIMEMultipart("alternative")
                msg["Message-ID"] = f"<{uuid.uuid4()}@quantum-scaling.com>"
                msg["Subject"]    = subject
                msg["From"]       = smtp_user
                msg["To"]         = email
                msg.attach(MIMEText(html, "html"))

                server.sendmail(smtp_user, [email], msg.as_string())
                summary_parts = []
                if count:    summary_parts.append(f"{count} failures")
                if mv_count: summary_parts.append(f"{mv_count} missing values")
                print(f"  ✅ Compliance email → {canonical} ({email}): {', '.join(summary_parts) or 'sent'}")
    except smtplib.SMTPException as e:
        print(f"  ERROR sending compliance emails: {e}")
