# Blueprint — Unified Two-Tier Navigation (v1)

**Project:** qs-dashboard
**Type:** A — pure frontend (no LLM, no external API, no DB writes)
**Author:** Claude (Opus) · 2026-06-15
**Design brief:** `design_brief.md` (locked) · **Approved mockup:** `prototypes/nav-dashboard-mockup.html` (commit ae67a98)
**Backend:** unaffected — no new endpoints/schema/pipeline. All data endpoints already exist and are already consumed.

---

## INTENT

```
outcome_trigger: >
  The dashboard's top navigation is inconsistent (4 different inline implementations,
  4 pages with no nav at all), has a broken SLWA link, and exposes dead/unused
  destinations (SLWA, standalone Expenses). Users can't reliably navigate.

success_state: >
  Every page renders ONE identical top nav from a single shared component; sub-navs
  are styled consistently; SLWA + Expenses are gone from the nav; no broken links.

observable_done_state:
  - Every page (dashboard, pnl, deals, expenses, data-quality, sync-history, slwa-dashboard, debug)
    renders the Tier-1 bar from static/nav.js — byte-identical markup across pages.
  - Tier-1 shows Sales · P&L · Deals (filled-active) + Data Quality · Sync (outline-active utility),
    with the active item determined by the current URL.
  - No page contains its own hand-written top-nav markup anymore (no .nav-links, no .nav-brand,
    no bespoke inline-styled nav links).
  - There is no "SLWA" and no "Expenses" entry anywhere in the Tier-1 bar; no link points to
    the dead /slwa-dashboard path (route is /channels/slwa).
  - Tier-2 sub-navs (Sales 7-tab, Deals lens) use the shared .subbar/.stab underline treatment and
    sit as a sticky strip directly under Tier-1; P&L and utility pages render no Tier-2 strip.
  - Page bodies/content below the nav are unchanged (no data, table, chart, or lens BEHAVIOR altered).

depends_on: []
```

---

## SCOPE_FENCE

```
explicitly_excluded:
  - item: Retiring the /expenses route and /channels/slwa page entirely
    reason: Out of scope — we de-link them from nav now; route retirement is a separate backlog item (bookmarks may exist).
  - item: Any change to page CONTENT (tables, charts, KPIs, the Deals matching/lens DATA logic)
    reason: This is a nav-only refactor. Content stays byte-for-byte except where the old nav markup is removed.
  - item: New backend endpoints, schema, or migrations
    reason: Confirmed unnecessary — P&L already fetches+renders expenses; Sales/Deals sub-navs already work.
  - item: Mobile/responsive redesign of the nav
    reason: Desktop-first internal tool; Tier-2 uses overflow-x:auto as the only concession. Full responsive deferred.

conformance_rules:
  allowed_without_flagging:
    - Removing dead nav CSS (.nav-links/.nav-link/.nav-brand rules) left orphaned after markup removal
    - Adding the single <script src="/static/nav.js"> include to each page
    - Wrapping an existing sub-nav in <div class="subbar"> and renaming its button class to .stab
  flag_for_review:
    - Any change to a page's data-fetching JS or content-rendering functions
    - Any new color/hex not in design_brief.md Color Tokens
    - Any change to the Deals matching engine or lens DATA behavior (styling-only is allowed)
  disallowed:
    - Per-page divergent nav markup (nav MUST come from static/nav.js)
    - Emoji or icons inside the nav (either tier)
    - A second accent color
    - Altering any /api/* call or response handling
```

---

## VERIFICATION_CONTRACTS

```
acceptance_criteria:
  - function: activeKeyForPath
    given: pathname "/" or "/dashboard"
    then: returns "sales"
  - function: activeKeyForPath
    given: pathname "/pnl"
    then: returns "pnl"
  - function: activeKeyForPath
    given: pathname "/deals"
    then: returns "deals"
  - function: activeKeyForPath
    given: pathname "/data-quality"
    then: returns "dq" (utility-active, no main tab filled)
  - function: activeKeyForPath
    given: pathname "/sync-history"
    then: returns "sync" (utility-active)
  - function: activeKeyForPath
    given: pathname "/expenses", "/channels/slwa", or "/debug"
    then: returns null (nav renders, nothing active — page is still navigable away from)
  - function: renderTopbar
    given: any page load
    then: inserts exactly ONE .topbar as first child of <body>; never a second if called twice (idempotent guard)
  - function: injectStyles
    given: called more than once (e.g., script included twice)
    then: injects the <style id="qs-nav-styles"> only once (no duplicate styles)

verifier_contracts:
  signatures:
    - name: activeKeyForPath
      params: "pathname: string"
      returns: "'sales' | 'pnl' | 'deals' | 'dq' | 'sync' | null"
    - name: renderTopbar
      params: "activeKey: string | null"
      returns: "void (side effect: DOM insertion)"
    - name: injectStyles
      params: "(none)"
      returns: "void"
  url_params: []
  states:
    empty: { trigger: "n/a — nav has no data dependency", required_selectors: [] }
  token_allowlist:
    - "var(--bg)"
    - "var(--surface)"
    - "var(--surface2)"
    - "var(--border)"
    - "var(--accent)"
    - "var(--accent2)"
    - "var(--text)"
    - "var(--text2)"
    - "rgba(99,102,241,0.08)"   # the one approved derived value — accent at 0.08 for utility-active bg

dependency_state:
  - name: "FastAPI static routes (/, /dashboard, /pnl, /deals, /expenses, /data-quality, /sync-history, /channels/slwa, /debug)"
    status: stable
    contract: "Each serves its existing static HTML file. nav.js is served from /static/nav.js (same _STATIC_DIR). No route changes."
```

---

## SYSTEM_CONTRACTS

```
preconditions:
  - static/nav.js is reachable at /static/nav.js (it lives in the existing _STATIC_DIR that already serves the .html files).

postconditions:
  success: One shared nav on every page; no per-page nav markup; no SLWA/Expenses nav entries; no broken links.
  failure: N/A — purely additive JS + markup deletion; no transactional state. A bad deploy is reverted by Railway rollback.

ownership_boundaries:
  reads_from: [window.location.pathname]
  writes_to: [document DOM — prepends .topbar; injects one <style>]
  must_never_touch: [any /api/* call, any page's data-rendering JS, the Deals matching/lens DATA logic]

state_transition_rules: []   # nav.js is stateless beyond "active = f(pathname)"

observability_contracts: []  # no external dependency; pure DOM
```

---

## VISUAL_CONTRACT

**Source:** design_brief.md + project-control/specs/v0-approved.md
**Status:** Locked — approved by Lloyd 2026-06-15

### Design Tokens (from design_brief.md — CSS custom properties, NOT Tailwind)
- Background: `var(--bg)` #0f1117 · Surface (bar): `var(--surface)` #1a1d27 · Inset: `var(--surface2)` #22263a
- Border: `var(--border)` #2e3347
- Accent: `var(--accent)` #6366f1 — active main tab fill, sub-tab underline, focus. Accent2 `var(--accent2)` #818cf8 — active sub-tab text, active utility text, "Revenue" wordmark.
- Typography: main tab 13.5px/600 · sub tab 13px/500 · brand 15px/700
- Density: dense — top bar 52px, sub-nav 42px, radius 8–10px
- Aesthetic reference: the app's own existing dark/indigo system

### Approved Visual (from v0-approved.md)
- Component: unified two-tier nav · Approved: 2026-06-15 (commit ae67a98)
- Layout: Tier-1 sticky top:0 (brand left, main tabs, utility right); Tier-2 sticky top:52px (underline sub-tabs), collapses when empty.
- Key treatments confirmed:
  - Main tab active = filled `--accent` bg + #fff text, radius 8px
  - Utility active = `--accent2` text + `--accent` border + rgba(99,102,241,0.08) bg (quieter than tabs)
  - Sub-tab active = `--accent2` text + 2px `--accent` bottom-border
  - Filled-vs-underline hierarchy is load-bearing (primary vs secondary nav)
- Deviations from brief: none

### Forbidden Patterns (fail /design-audit)
1. Emoji or icon anywhere in the nav (either tier) — nav is text-only
2. Hardcoded hex/rgb not in the token_allowlist above
3. A main tab and a sub-tab sharing the same active treatment (must keep filled vs underline)
4. Per-page divergent nav markup (must come from static/nav.js)
5. A second accent color
6. Interactive nav element with no hover/focus state
7. Underscore in any display string rendered to DOM

---

## Files in This Blueprint

| File | Action | Owns |
|---|---|---|
| `static/nav.js` | **NEW** | The entire shared top-nav (Tier-1): CSS injection + markup + active-by-URL. Single source of truth. |
| `static/dashboard.html` | MODIFY | Remove bespoke inline nav; add nav.js include; wrap existing 7-tab `.tabs` as sticky `.subbar`. |
| `static/pnl.html` | MODIFY | Remove `.nav-links` block + its CSS; add nav.js include. No Tier-2. |
| `static/deals.html` | MODIFY | Remove `.nav-brand` emoji nav + CSS; add nav.js include; restyle `wl-lens` → `.subbar/.stab` underline. |
| `static/expenses.html` | MODIFY | Remove `.nav-links` block + CSS; add nav.js include (de-linked from nav, still navigable). |
| `static/data-quality.html` | MODIFY | Add nav.js include (Data Quality utility active). |
| `static/sync-history.html` | MODIFY | Add nav.js include (Sync utility active). |
| `static/slwa-dashboard.html` | MODIFY | Add nav.js include (nothing active; not in nav). |
| `static/debug.html` | MODIFY | Add nav.js include (nothing active; dev page). |

---

## SKELETON — `static/nav.js` (NEW)

```
# static/nav.js
# Owns: the global Tier-1 top navigation — its styles, markup, and active-state.
# Dependencies: none (vanilla JS, runs on DOMContentLoaded)

const QS_NAV_MAIN = [ {key:'sales',label:'Sales',href:'/dashboard'},
                      {key:'pnl',label:'P&L',href:'/pnl'},
                      {key:'deals',label:'Deals',href:'/deals'} ];
const QS_NAV_UTIL = [ {key:'dq',label:'Data Quality',href:'/data-quality'},
                      {key:'sync',label:'Sync',href:'/sync-history'} ];

function activeKeyForPath(pathname):
    """Return the nav key that the given URL path maps to, or null."""
    # '/' or '/dashboard' -> 'sales'; '/pnl' -> 'pnl'; '/deals' -> 'deals';
    # '/data-quality' -> 'dq'; '/sync-history' -> 'sync'; else null
    // TODO: implement (pure function — exact-match table, no logic beyond lookup)

function injectStyles():
    """Inject the nav stylesheet exactly once."""
    # guard on document.getElementById('qs-nav-styles'); inject <style id="qs-nav-styles"> with
    # .topbar/.brand/.main-tabs/.mtab/.util-group/.utab + .subbar/.stab (tier-2 shared styling), tokens via var(--*)
    // TODO: implement

function renderTopbar(activeKey):
    """Insert the Tier-1 bar as the first child of body, exactly once."""
    # guard on existing .topbar; build brand + main tabs (filled-active) + utility (outline-active); prepend to body
    // TODO: implement

function initNav():
    """Compose the nav on load."""
    # injectStyles(); renderTopbar(activeKeyForPath(window.location.pathname))
    // TODO: implement

# auto-run: if document.readyState === 'loading' addEventListener('DOMContentLoaded', initNav) else initNav()
```

**Purpose test:** each function is one sentence, no "and". `activeKeyForPath` = lookup; `injectStyles` = inject-once; `renderTopbar` = insert-once; `initNav` = compose. ✓
**Dependency direction:** vanilla, no imports. ✓

---

## REFACTOR SCOPE (brownfield — per modified file)

### static/nav.js — NEW (no current structure)

### static/dashboard.html (Sales)
```
CURRENT STRUCTURE
─────────────────
~~bespoke inline-styled nav links (line ~235 /pnl link with huge inline style; line ~237 #last-synced /sync-history link)~~
.tabs / .tab-btn (line ~272) — the 7 sub-tabs (Sales Performance…Upsells) via setTab()   [KEEP behavior]
CALL SITES: setTab() called by the .tab-btn onclick handlers — UNCHANGED.

REPLACEMENT SKELETON
────────────────────
+ <script src="/static/nav.js"></script>  (before </body>)
- remove the bespoke inline nav links (now provided by Tier-1 bar)
~ wrap the existing <nav class="tabs"> in the shared sticky <div class="subbar"> and alias .tab-btn → .stab
  (the existing .tab-btn underline style already matches .stab — restyle is a class rename + sticky position, NOT new behavior)
  setTab() and tab-content switching: UNCHANGED.
```
- Redundant: the inline /pnl + sync links (Tier-1 replaces them).
- Inconsistent: bespoke inline styling vs every other page.
- Misleading: none.

### static/pnl.html
```
CURRENT STRUCTURE
─────────────────
~~.nav-links / .nav-link CSS (lines ~47-53)~~
~~<nav class="nav-links"> Sales · P&L(active) · Deals · SLWA(/slwa-dashboard, BROKEN) · Sync (lines ~187-192)~~

REPLACEMENT SKELETON
────────────────────
+ <script src="/static/nav.js"></script>
- remove the <nav class="nav-links"> block AND its now-orphaned CSS
- NO Tier-2 (P&L is a single page; revenue+expenses already render inline — UNCHANGED)
```
- Redundant: the whole hand-written nav + its CSS. Misleading: the broken SLWA href.

### static/deals.html
```
CURRENT STRUCTURE
─────────────────
~~.nav-brand + emoji nav links (lines ~325-330): QS · 💰P&L · 📋Expenses · 🤝Deals(active) · 🔍Data Quality~~
.wl-lens (line ~349) — Live/Historical segmented toggle   [KEEP behavior, RESTYLE to underline]

REPLACEMENT SKELETON
────────────────────
+ <script src="/static/nav.js"></script>
- remove the .nav-brand emoji nav block + its CSS
~ move .wl-lens into the sticky <div class="subbar"> and restyle the two lens buttons to .stab underline tabs
  (DATA behavior of the lens — month nav, Live/Historical fetch — UNCHANGED; styling-only change)
```
- Redundant: the emoji nav. Inconsistent: emoji nav + segmented lens vs the unified underline treatment.
- Note: this is the one deliberate VISUAL change to recently-built work (lens: segmented → underline) — per design_brief Deals Tier-2 = underline.

### static/expenses.html
```
CURRENT STRUCTURE
─────────────────
~~.nav-links / .nav-link CSS (lines ~39-45)~~
~~<nav class="nav-links"> Sales · Expenses(active) · SLWA(BROKEN) · Sync (lines ~136-140)~~

REPLACEMENT SKELETON
────────────────────
+ <script src="/static/nav.js"></script>   (page de-linked from nav but still navigable away from)
- remove the <nav class="nav-links"> block + orphaned CSS
```

### static/data-quality.html · sync-history.html · slwa-dashboard.html · debug.html
```
CURRENT STRUCTURE
─────────────────
(no top-nav markup — these pages currently have no nav)

REPLACEMENT SKELETON
────────────────────
+ <script src="/static/nav.js"></script>
  data-quality → Data Quality utility active; sync-history → Sync utility active; slwa/debug → nothing active.
```

---

## How this fails / scale / rollback (required)

- **How it fails:** (1) a page forgets the nav.js include → that page has no nav. Caught by acceptance criterion "every page renders Tier-1". (2) nav.js inserts a duplicate bar if a page already has a leftover .topbar → idempotent guard prevents it. (3) active-tab wrong on a path → activeKeyForPath unit criteria catch it.
- **At scale:** nav is static; no data, no N+1, no load concern. Adding a future page = one include + (optional) a row in QS_NAV_*.
- **Rollback (<5 min):** Railway redeploy of the previous commit. No schema, no migration, fully reversible.

---

## Verification method (for Lloyd)
After deploy: load each of the 8 pages on the Railway URL; confirm the identical bar appears, the correct tab/utility is active, no SLWA/Expenses entries exist, and clicking each destination navigates correctly. `/design-drive` will machine-compare the rendered bar against `prototypes/nav-dashboard-mockup.html`.
```
```
