# v0 Mockup — Approved Visual Record

**Project:** qs-dashboard — unified two-tier navigation
**Approved by Lloyd:** 2026-06-15
**Mockup (localhost-first, committed):** `prototypes/nav-dashboard-mockup.html` (commit ae67a98)
**Design brief:** `design_brief.md` (locked 2026-06-15)

## What was approved

A fully-clickable HTML mockup of the unified nav, verified live across all three interaction states:

- **Tier 1 (global top bar):** brand "QS Revenue Dashboard" left · main tabs **Sales · P&L · Deals** (filled-active, `--accent`) · utility **Data Quality · Sync** right (outline-active, quieter than tabs). Identical on every page.
- **Tier 2 (contextual sub-nav, underline-active):**
  - Sales → 7 sub-tabs (Performance · Lead Quality · Pipeline · Funnel · Compliance · Dead Deals · Upsells) — all fit on one row.
  - Deals → Live·This-Month / Historical·Reconciled lens.
  - P&L → **none** (single page, expenses inline) → strip collapses.
  - Utility pages → none → strip collapses.
- **Hierarchy treatment confirmed:** filled = primary section (tier 1); underline = view within (tier 2). Two distinct active styles on purpose.
- **Text-only nav** (no icons/emoji in either tier). Emoji retained in page *titles* only (existing convention).

## Deviations from brief
None. Mockup tokens were copied verbatim from the app's real `:root`, so brief ↔ mockup are token-identical.

## States verified (playwright screenshots)
- Sales (7 underline sub-tabs, all fit) ✓
- Deals (segmented Live/Historical lens, green live-dot) ✓
- P&L (strip collapses, single page) ✓
- Data Quality (utility active, strip collapses) ✓

---

## EXTRACTED TOKENS (canonical — `getComputedStyle` on the rendered mockup, 2026-06-15)

Extracted by rendering `prototypes/nav-dashboard-mockup.html` and reading computed styles off the live DOM (not hand-typed). The VISUAL_CONTRACT copies these VERBATIM. These match the design_brief `:root` exactly → **zero drift** between brief and rendered mockup.

```
theme:                 dark
page_bg:               #0f1117      (body background — MANDATORY parity)
surface_bar:           #1a1d27      (Tier-1 .topbar background — MANDATORY parity)
border:                #2e3347      (bar + sub-nav hairline)
font_family:           Inter, system-ui, -apple-system, sans-serif
accent:                #6366f1      (main-tab active fill, sub-tab underline, seg-active fill)
accent2:               #818cf8      (Revenue wordmark, sub-tab active text, utility active text)
active_text:           #ffffff      (text on filled-active)
live_dot:              #22c55e      (green status dot on Deals "Live" — status indicator, not an icon)
nav_height (--qs-nav-h): 52px       (Tier-1 height; Tier-2 sticky top references this)
```

**coarse_ref screenshot:** `project-control/specs/nav-coarse-ref.png` (Sales default state — design-drive grades screenshot-parity against this: theme/palette/layout gestalt).

**Theme-parity note:** the redesign reuses the app's existing dark/indigo system verbatim (same tokens) → it is NOT a re-skin. `/design-drive`'s theme-parity gate should PASS; no MIGRATION-REQUIRED expected.
