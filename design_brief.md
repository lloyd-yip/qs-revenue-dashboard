# Design Brief — QS Revenue Dashboard
Generated: 2026-06-15
Status: **Retrofit** — documents the EXISTING design system so it becomes enforceable. NOT a new aesthetic.
Aesthetic reference: the app's own established system — dark data-dashboard, indigo accent (revenue/sales-analytics vertical, Gong/Apollo-class internal tools). NOT generic SaaS (Linear/Stripe/Vercel).
Approved mockup: `prototypes/nav-dashboard-mockup.html` (nav redesign, approved 2026-06-15)

> **Stack note:** qs-dashboard is **hand-written CSS with custom properties — NOT Tailwind.** Tokens below are the real `:root` variables from the deployed app (`dashboard.html`). This brief enforces CONSISTENCY WITH THE APP; `/design-audit` and `/design-drive` must not flag the app's own established conventions. The design-brief skill's generic Tailwind/zinc defaults do **not** apply to this project.

---

## Color Tokens (real `:root` — copy verbatim, reference by var)

```
--bg        #0f1117   page background
--surface   #1a1d27   cards, top-nav bar, panels
--surface2  #22263a   inset controls (toggle bg, hover fills)
--border    #2e3347   card edges, dividers, sub-nav underline track
--accent    #6366f1   indigo — active/selected, primary action, focus ring
--accent2   #818cf8   lighter indigo — active TEXT on underline tabs, links
--text      #e2e8f0   primary text, titles
--text2     #94a3b8   secondary text, labels, inactive tabs, metadata
--green     #22c55e   positive / high-confidence
--red       #ef4444   negative / lost / regressing
--yellow    #f59e0b   caution / medium-confidence
--radius    10px
font        'Inter', system-ui, -apple-system, sans-serif
```

**Accent use:** active main tab (filled `--accent`), active sub-tab (underline `--accent` + text `--accent2`), focus rings, primary buttons. Never decorative. No third accent.

---

## Typography Scale (observed app scale)

```
Page title:     22–26px / 700 / tracking -0.4px / --text        (e.g. "💰 P&L")
Section label:  11–13px / 600 / UPPERCASE / letter-spacing 0.4–0.5px / --text2
Main tab:       13.5px / 600 / --text2 inactive · #fff active
Sub tab:        13px   / 500 / --text2 inactive · --accent2 active
Body / table:   13–14px / --text
KPI value:      26px / 700 / tracking -0.5px
Metadata:       11–12px / --text2
```

---

## Density — Power-user dense

Data-dashboard density: tight rows, multiple tables per screen, base unit 8px.
Top-nav height **52px** · sub-nav height **42px** · card padding 16–22px · radius 10px.

---

## NAVIGATION CONTRACT (this redesign — LOCKED, mockup: `prototypes/nav-dashboard-mockup.html`)

**Two tiers, identical on every page, rendered from ONE shared component** (see blueprint — not duplicated per page).

### Tier 1 — global top bar
- 52px tall, `bg --surface`, `border-bottom 1px --border`, `position:sticky; top:0`.
- Brand (left): "QS Revenue Dashboard" ("Revenue" in `--accent2`).
- **Main tabs:** Sales · P&L · Deals
  - inactive: `--text2`, no background
  - hover: `bg --surface2`, `--text`
  - **ACTIVE: `bg --accent` (#6366f1), `#fff` text, radius 8px** ← filled = primary nav
- **Utility (right):** Data Quality · Sync
  - inactive: `--text2`, transparent border
  - hover: `bg --surface2`, `border --border`
  - **ACTIVE: `--accent2` text, `--accent` border, `bg rgba(99,102,241,0.08)`** ← outline = utility, visually quieter than tabs

### Tier 2 — contextual sub-nav strip (two idioms, one sticky container)
- 42px tall, `bg --bg`, `border-bottom 1px --border`, `position:sticky; top:var(--qs-nav-h)`, `overflow-x:auto`.
- **Sales →** 7 UNDERLINE sub-tabs (genuine sub-navigation): Performance · Lead Quality · Pipeline · Funnel · Compliance · Dead Deals · Upsells
  - inactive `--text2` · hover `--text` · **ACTIVE: `--accent2` text + 2px `--accent` bottom-border**
- **Deals →** SEGMENTED lens (mode-switch, NOT navigation): Live·This-Month / Historical·Reconciled, green live-dot on Live
  - bordered box, `bg --surface2` · **ACTIVE: filled `--accent` + #fff** (matches existing `.wl-lens` / `.toggle-btn`)
- **P&L →** NONE (single page; expenses inline) → **strip collapses**
- **Utility pages →** NONE → **strip collapses**

### Active-state mapping (nav.js, from `window.location.pathname`)
- `/`, `/dashboard` → **Sales** · `/pnl` → **P&L** · `/deals` → **Deals**
- `/data-quality` → **Data Quality** (utility) · `/sync-history` → **Sync** (utility)
- **Detail views keep their PARENT tab active** (reachable from within a section, dropped from the top nav but not dead): `/expenses` → P&L · `/channels/slwa` → Sales · `/debug` → Sales. The user is never stranded with nothing active.

**Hierarchy rule (non-negotiable):** filled = which app *section* (Tier 1). Tier 2 = which *view/mode* within it — **underline sub-tabs for navigation (Sales), segmented toggle for a lens (Deals)**. The two active treatments differ on purpose. **Nav is TEXT-ONLY** — no icons/emoji in either tier (the green live-dot on the Deals lens is a status indicator, not an icon).

---

## Component conventions (existing — DO NOT flag as defects)

- **Segmented toggle** (`.toggle-group`/`.toggle-btn`): bordered box, filled-accent active. For in-page FILTERS/LENSES (Day/Week/Month, Table/Chart). Distinct from nav.
- **Confidence pills** (`.pill .pill-high/medium/low/unmatched`): **ALLOWED** — semantic match-confidence tiers on the Deals page. Established convention, not decoration.
- **Emoji in PAGE TITLES only** (📊 Sales, 💰 P&L, 🤝 Deals): **ALLOWED** — existing convention, Lloyd-confirmed. FORBIDDEN in nav, buttons, labels, table cells.

---

## Interactive rules

Every clickable element: explicit hover (bg/color shift), `cursor:pointer`, visible focus, `transition ~0.15s`. Matches existing `.tab-btn` / `.toggle-btn`.

---

## Anti-Pattern Checklist (enforced by `/design-audit` on nav files)

**FORBIDDEN:**
1. Hardcoded hex/rgb not in Color Tokens above — use the `--vars`.
2. Emoji or icon in the nav (either tier) — nav is text-only.
3. Emoji anywhere except page titles.
4. A main tab styled like a sub-tab, or vice-versa — keep the filled-vs-underline hierarchy.
5. Per-page divergent nav markup — nav must come from the single shared component.
6. Interactive nav element with no hover/focus state.
7. A second accent color (only `--accent` / `--accent2`).
8. Underscore in any display string rendered to DOM.

**Explicitly NOT forbidden** (existing app conventions): emoji in page titles · confidence pills on Deals · segmented toggles for in-page filters.
