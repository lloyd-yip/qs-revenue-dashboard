# Blueprint — Unified Two-Tier Navigation (v2)

**Project:** qs-dashboard · **Type:** A (pure frontend) · **Author:** Claude (Opus) · 2026-06-15
**Design brief:** `design_brief.md` (locked) · **Approved mockup:** `prototypes/nav-dashboard-mockup.html`
**Supersedes:** `nav_redesign_blueprint_v1.md` (kept for history)
**Backend:** unaffected — no endpoints/schema/pipeline.

---

## Changes from V1 (all driven by the adversarial review)

1. **Missed nav markup (BLOCKER):** v1's audit grepped the wrong classes and MISSED "← Back to Dashboard" links on `data-quality.html:190`, `sync-history.html:117`, `debug.html:588`, `slwa-dashboard.html:174`. v2 captures all four — surgically (see SLWA carve-out).
2. **Sticky-thead clipping (BLOCKER):** `data-quality.html:121`, `sync-history.html:67`, `debug.html:227` have `thead{position:sticky;top:0}` that would hide behind the 52px bar. v2 adds a `top:var(--qs-nav-h)` offset on those 3 pages. The "content unchanged" claim is amended to allow this offset-only edit.
3. **"SLWA is dead" was wrong (BLOCKER):** `dashboard.html:804` deep-links `/channels/slwa` as a Sales channel drill-down (and `:797` `/debug` from metric tooltips). v2 reframes SLWA/Debug/Expenses as **detail views** that keep their PARENT tab active — never stranded.
4. **Deals lens kept SEGMENTED (decision):** v1 restyled the Live/Historical lens to underline; the brief itself says segmented = lens. v2 keeps the segmented toggle (relocated into the sticky strip); underline applies only to Sales' 7 genuine sub-tabs.
5. **dashboard `<header>` disposition (was under-specified):** v2 removes the redundant `.dq-btn` + inline `/pnl` link + `QS Analytics Dashboard` H1 (Tier-1 replaces them); keeps the `#last-synced` freshness indicator as a slim status line.
6. **Removal-side acceptance criterion added:** grep proves zero old-nav remnants per page (v1 only tested the additive half).
7. **`--qs-nav-h` var + z-index pinned:** one source for the 52px offset; `.topbar` z-index 500 / `.subbar` 499 (above sticky theads ≤15, below modals 1000).
8. **Vocab clash ("Deals" vs "Dead Deals") deferred** to a backlog copy fix (SCOPE_FENCE).

---

## INTENT

```
outcome_trigger: >
  The top nav is inconsistent (4 divergent inline implementations + 4 pages whose only
  nav is a hand-written "Back to Dashboard" link), has a broken SLWA link, and mixes
  top-level destinations with section detail-views.

success_state: >
  Every page renders ONE identical Tier-1 bar from a shared component; sub-navs use a
  consistent sticky strip; SLWA + Expenses are gone as top tabs but remain reachable as
  detail-views with their parent tab highlighted; no broken or duplicate nav links.

observable_done_state:
  - Every served page (/, /dashboard, /pnl, /deals, /expenses, /data-quality, /sync-history,
    /channels/slwa, /debug) renders the Tier-1 bar from static/nav.js — identical markup.
  - Tier-1 shows Sales · P&L · Deals (filled-active) + Data Quality · Sync (outline utility);
    active state = activeKeyForPath(location.pathname), incl. detail-view→parent mapping.
  - No page retains its own top-level nav markup (.nav-links/.nav-brand, the bespoke inline
    /pnl pill, .dq-btn, #last-synced-as-nav, or a static header back-link) — EXCEPT the
    documented SLWA/Debug JS-wired #back-link carve-out.
  - No link points to the dead /slwa-dashboard path; SLWA is reached only via /channels/slwa.
  - Sales shows 7 underline sub-tabs in the sticky strip; Deals shows its segmented
    Live/Historical lens in the same strip; P&L + utility pages render no strip (collapsed).
  - Sticky table headers on data-quality/sync-history/debug remain fully visible (offset to
    var(--qs-nav-h)), not clipped behind the bar.
  - No console error on any of the 9 page loads.
  - Page DATA/behavior is unchanged (no /api call, table logic, chart, or the Deals lens
    switching/month-nav altered — styling + nav-markup removal + thead-offset only).

depends_on: []
```

---

## SCOPE_FENCE

```
explicitly_excluded:
  - item: Retiring the /expenses route or the /channels/slwa page
    reason: De-linked as top tabs now; both stay reachable as detail-views. Route retirement = separate backlog.
  - item: Resolving the "Deals" (top tab) vs "Dead Deals"/"Pipeline" (Sales sub-tabs) name clash
    reason: Real clarity issue but a copy/IA decision — backlog item (suggested: rename "Dead Deals"→"Lost Analysis"). Not a nav-architecture change.
  - item: Verifying P&L fully covers expenses.html's vendor-level breakdown before route retirement
    reason: P&L renders expense totals but is NOT a 100% superset (expenses.html has more vendor detail). Backlog note before any /expenses retirement.
  - item: New backend endpoints/schema/migrations; mobile responsive redesign
    reason: Out of scope. Tier-2 uses overflow-x:auto as the only responsive concession.

conformance_rules:
  allowed_without_flagging:
    - Removing dead nav CSS orphaned after markup removal (.nav-links/.nav-link/.nav-brand, the bare `nav{position:sticky}` on deals)
    - Adding <script src="/static/nav.js"></script> to each page
    - Offsetting sticky thead/th from top:0 to top:var(--qs-nav-h) on data-quality/sync-history/debug
    - Relocating the existing Deals .wl-lens into the sticky .subbar (container change, NOT a restyle of the control)
  flag_for_review:
    - Any change to a page's data-fetching JS or content-rendering functions
    - Any change to the Deals lens switching (wlSetLens) or month-nav (wlStepMonth/loadWhopLive) BEHAVIOR
    - Any new color/hex not in the token_allowlist
  disallowed:
    - Per-page divergent nav markup (must come from static/nav.js)
    - Emoji/icons inside the nav (the Deals green live-dot is a status indicator, allowed)
    - A second accent color
    - Removing the SLWA/Debug table-cell `.back-link` (drill-down links) or the `.back-link` CSS class (shared)
    - Altering any /api/* call or response handling
```

---

## VERIFICATION_CONTRACTS

```
acceptance_criteria:
  - function: activeKeyForPath
    given: "/" or "/dashboard"
    then: returns "sales"
  - function: activeKeyForPath
    given: "/pnl"
    then: returns "pnl"
  - function: activeKeyForPath
    given: "/deals"
    then: returns "deals"
  - function: activeKeyForPath
    given: "/data-quality"
    then: returns "dq"
  - function: activeKeyForPath
    given: "/sync-history"
    then: returns "sync"
  - function: activeKeyForPath
    given: "/expenses" (detail view)
    then: returns "pnl"  (parent tab highlighted — user is "in P&L")
  - function: activeKeyForPath
    given: "/channels/slwa" or "/debug" (detail views)
    then: returns "sales" (parent tab highlighted — reached from Sales)
  - function: renderTopbar
    given: any page load
    then: inserts exactly ONE .topbar (idempotent guard); never a duplicate if called twice
  - function: injectStyles
    given: called >1x
    then: injects <style id="qs-nav-styles"> once; writes --qs-nav-h:52px to :root once
  - criterion: REMOVAL (the half v1 missed)
    given: each of the 9 pages, post-build
    then: grep finds ZERO of { 'class="nav-links"', 'class="nav-brand"', the bespoke inline /pnl pill,
          'class="dq-btn"', '#last-synced' used as a nav link, a STATIC header <a class="back-link"> on
          data-quality/sync-history } — SLWA/Debug JS-wired #back-link is the documented exception
  - criterion: NO-REGRESSION
    given: load all 9 pages
    then: zero console errors; the Deals lens still switches Live/Historical and steps months;
          sticky theads on data-quality/sync-history/debug are fully visible (not clipped)
  - criterion: ROUTE-RESOLVE
    given: GET "/" and GET "/dashboard"
    then: both return 200 serving the SAME Sales page (dashboard.html)

verifier_contracts:
  signatures:
    - name: activeKeyForPath
      params: "pathname: string"
      returns: "'sales' | 'pnl' | 'deals' | 'dq' | 'sync' | null"
    - name: renderTopbar
      params: "activeKey: string | null"
      returns: "void (DOM insertion, idempotent)"
    - name: injectStyles
      params: "(none)"
      returns: "void (idempotent; sets --qs-nav-h)"
  token_allowlist:
    - "var(--bg)" "var(--surface)" "var(--surface2)" "var(--border)"
    - "var(--accent)" "var(--accent2)" "var(--text)" "var(--text2)" "var(--green)"
    - "var(--qs-nav-h)"
    - "rgba(99,102,241,0.08)"   # utility-active bg (accent @ 0.08)
    - "rgba(34,197,94,0.25)"    # live-dot halo (green @ 0.25)

dependency_state:
  - name: "FastAPI static routes (api/main.py:117-174) — 9 HTML routes incl. / and /dashboard both serving dashboard.html"
    status: stable
    contract: "Each serves its existing static file; nav.js served from /static/nav.js (same _STATIC_DIR). No route changes."
  - name: "deals.html wlSetLens / wlStepMonth / loadWhopLive"
    status: stable
    contract: "The Live/Historical lens + month-nav behavior. nav work only RELOCATES + keeps the segmented .wl-lens; must not touch this JS."
```

---

## SYSTEM_CONTRACTS

```
preconditions:
  - static/nav.js reachable at /static/nav.js (lives in the existing _STATIC_DIR serving the .html files).

postconditions:
  success: One shared Tier-1 on every page; detail-views map to parent active; no duplicate/broken/clipped nav.
  failure: Purely additive JS + markup deletion + CSS offset; no transactional state. Bad deploy → Railway rollback.

ownership_boundaries:
  reads_from: [window.location.pathname]
  writes_to: [DOM — prepends .topbar; injects one <style>; sets --qs-nav-h on :root]
  must_never_touch: [any /api/* call, page data-rendering JS, the Deals lens/month-nav DATA logic,
                     the shared .back-link CSS class + SLWA/Debug table-cell .back-link links]

state_transition_rules: []
observability_contracts: []
```

---

## VISUAL_CONTRACT

**Source:** design_brief.md + specs/v0-approved.md · **Status:** Locked — approved 2026-06-15

### EXTRACTED TOKENS — copied VERBATIM from specs/v0-approved.md (getComputedStyle on the rendered mockup)
**design-drive grades the built nav against THESE. Theme/bg/surface parity is MANDATORY.**
```
theme:        dark
page_bg:      #0f1117   (body — MANDATORY)        surface_bar: #1a1d27   (Tier-1 — MANDATORY)
border:       #2e3347                              font:        Inter, system-ui, -apple-system, sans-serif
accent:       #6366f1   (main fill / underline / seg fill)     accent2: #818cf8   (wordmark / sub-tab text / utility text)
active_text:  #ffffff   live_dot: #22c55e   --qs-nav-h: 52px
```
coarse_ref: `project-control/specs/nav-coarse-ref.png` (screenshot-parity reference)

### Authoring tokens (how to write them — these resolve to the extracted values above)
- bg `var(--bg)` · bar `var(--surface)` · inset `var(--surface2)` · border `var(--border)`
- accent `var(--accent)` · accent2 `var(--accent2)` · green `var(--green)` (live-dot)
- **`--qs-nav-h: 52px`** — single source for bar height; `.subbar` + the 3 thead offsets all reference it.
- z-index: `.topbar` 500 · `.subbar` 499 (above sticky theads ≤15, below modals 1000 & metric-tooltip 99999)
- `.topbar { box-sizing:border-box; height:var(--qs-nav-h) }`

### Approved Visual (mockup: prototypes/nav-dashboard-mockup.html)
- Tier-1 sticky top:0 (brand "QS Revenue Dashboard" left · main tabs · utility right).
- Tier-2 sticky top:var(--qs-nav-h):
  - **Sales** = 7 underline sub-tabs (`.stab`, active = accent2 text + 2px accent underline)
  - **Deals** = segmented lens (`.seg`, bordered box, active = filled accent + #fff, green live-dot on Live)
  - **P&L / utility** = collapsed (no strip)
- Main tab active = filled `--accent` + #fff (radius 8px); Utility active = `--accent2` text + `--accent` border + rgba(99,102,241,0.08) bg.

### Forbidden (fail /design-audit)
1. Emoji/icons in the nav (live-dot status indicator is allowed) · 2. Off-allowlist hex/rgb ·
3. Main-tab and sub-tab sharing the same active treatment · 4. Per-page divergent nav markup ·
5. A second accent · 6. Interactive nav element with no hover/focus · 7. Underscore in a rendered display string.

---

## Files in This Blueprint

| File | Action | Owns / Change |
|---|---|---|
| `static/nav.js` | **NEW** | Entire Tier-1: CSS injection (+ `--qs-nav-h`, z-index), markup, active-by-URL (incl. detail→parent). |
| `static/dashboard.html` | MODIFY | Remove `<header>` redundant nav (`.dq-btn`, inline `/pnl` pill, "QS Analytics" H1); keep `#last-synced` as slim status; add nav.js; wrap existing 7-tab `.tabs` as sticky `.subbar` (alias `.tab-btn`→`.stab`). |
| `static/pnl.html` | MODIFY | Remove `.nav-links` block + CSS; add nav.js. No Tier-2. |
| `static/deals.html` | MODIFY | Remove `.nav-brand` emoji nav + its CSS **incl. the bare `nav{position:sticky;top:0}` rule**; add nav.js; relocate `.wl-lens` (KEEP segmented) into the sticky `.subbar`. |
| `static/expenses.html` | MODIFY | Remove `.nav-links` block + CSS; add nav.js (→ P&L active). |
| `static/data-quality.html` | MODIFY | Remove static header `.back-link` anchor (190) + its CSS (53-57); offset `thead{top:0}`→`top:var(--qs-nav-h)` (121); add nav.js (→ DQ utility active). Keep `.dq-header` as page title. |
| `static/sync-history.html` | MODIFY | Remove static header `.back-link` anchor (117) + CSS (36-40); offset `th{top:0}`→`top:var(--qs-nav-h)` (67); add nav.js (→ Sync utility active). Keep `<h1>Sync History</h1>`. |
| `static/slwa-dashboard.html` | MODIFY | Add nav.js (→ Sales active). **CARVE-OUT:** keep the JS-wired `#back-link` (header link + `buildBackLink()`/:416) AS-IS — removing it would null-throw; harmless with Sales active. Do NOT touch the table-cell `.back-link` (464) or the `.back-link` CSS. |
| `static/debug.html` | MODIFY | Add nav.js (→ Sales active); offset `thead{top:0}`→`top:var(--qs-nav-h)` (227). Treat `#back-link` (588) like SLWA — carve-out keep (it has `id="back-link"`; verify-or-keep to avoid null-throw). |

---

## SKELETON — `static/nav.js` (NEW)

```
# static/nav.js
# Owns: the global Tier-1 top navigation — styles (incl. --qs-nav-h + z-index), markup, active-state.
# Dependencies: none (vanilla JS on DOMContentLoaded)

const QS_NAV_MAIN = [ {key:'sales',label:'Sales',href:'/dashboard'},
                      {key:'pnl',label:'P&L',href:'/pnl'},
                      {key:'deals',label:'Deals',href:'/deals'} ];
const QS_NAV_UTIL = [ {key:'dq',label:'Data Quality',href:'/data-quality'},
                      {key:'sync',label:'Sync',href:'/sync-history'} ];
# detail-view → parent active key
const QS_NAV_DETAIL = { '/expenses':'pnl', '/channels/slwa':'sales', '/debug':'sales' };

function activeKeyForPath(pathname):
    """Return the nav key the given URL path maps to, or null."""
    # exact table: '/'|'/dashboard'->'sales'; '/pnl'->'pnl'; '/deals'->'deals';
    # '/data-quality'->'dq'; '/sync-history'->'sync'; QS_NAV_DETAIL[path] for detail-views; else null
    // TODO: implement (pure lookup — no logic beyond the tables)

function injectStyles():
    """Inject the nav stylesheet and the --qs-nav-h root var exactly once."""
    # guard on getElementById('qs-nav-styles'); set :root{--qs-nav-h:52px}; .topbar(box-sizing/height/z-index:500)
    # /.brand/.main-tabs/.mtab/.util-group/.utab + .subbar(top:var(--qs-nav-h);z-index:499)/.stab + .seg/.seg-btn/.live-dot
    // TODO: implement

function renderTopbar(activeKey):
    """Insert the Tier-1 bar as body's first child exactly once."""
    # guard on existing .topbar; build brand + main tabs (filled-active) + utility (outline-active); prepend to body
    // TODO: implement

function initNav():
    """Compose the nav on load."""
    # injectStyles(); renderTopbar(activeKeyForPath(window.location.pathname))
    // TODO: implement

# auto-run: DOMContentLoaded ? addEventListener : initNav()
```

**Purpose test:** activeKeyForPath=lookup · injectStyles=inject-once · renderTopbar=insert-once · initNav=compose. ✓
**Note:** nav.js owns Tier-1 ONLY. Tier-2 (Sales `.tabs`, Deals `.wl-lens`) stays page-owned but uses the `.subbar`/`.stab`/`.seg` classes nav.js provides — decoupled (nav.js never needs to know a page's sub-tabs or their switching logic).

---

## REFACTOR SCOPE (per modified file — strikethrough = removed)

### static/dashboard.html
```
CURRENT
  <header> (231-238): ~~<h1>QS Analytics Dashboard</h1>~~ · ~~<button class="dq-btn">🔍 Data Quality</button>~~ ·
                      ~~inline-styled <a href="/pnl">💰 P&L</a> (235)~~ · <a id="last-synced" href="/sync-history">(237)
  <nav class="tabs"> (272): 7 .tab-btn via setTab()   [KEEP behavior]
REPLACEMENT
  + <script src="/static/nav.js"></script>
  - remove the H1, .dq-btn, and inline /pnl pill (Tier-1 replaces all three)
  ~ keep #last-synced as a slim status line (freshness info, not primary nav — still links /sync-history)
  ~ wrap <nav class="tabs"> in sticky <div class="subbar">; .tab-btn → .stab (existing underline style already matches; sticky position + class rename, NOT new behavior). setTab() UNCHANGED.
Redundant: H1 (brand dup), .dq-btn + /pnl pill (now Tier-1). Inconsistent: bespoke inline styling.
```

### static/pnl.html
```
CURRENT  ~~.nav-links CSS (47-53)~~ · ~~<nav class="nav-links"> Sales·P&L·Deals·SLWA(broken)·Sync (187-192)~~
REPLACEMENT  + nav.js include · - remove the nav + orphaned CSS · NO Tier-2 (revenue+expenses already inline — UNCHANGED)
```

### static/deals.html
```
CURRENT  ~~.nav-brand emoji nav (325-330) + its CSS incl. `nav{position:sticky;top:0;z-index:100}` (34-57)~~
         .wl-lens segmented Live/Historical toggle (349) + wlSetLens/wlStepMonth   [KEEP behavior + segmented look]
REPLACEMENT
  + nav.js include
  - remove the .nav-brand emoji nav block AND the bare `nav{...sticky...}` CSS rule (deals' only top:0 sticky)
  ~ relocate .wl-lens into the sticky <div class="subbar"> (container move only; KEEP segmented styling + .wl-dot; lens DATA logic UNCHANGED)
Inconsistent: emoji nav. Note: per decision, the lens stays segmented (NOT restyled to underline).
```

### static/expenses.html
```
CURRENT  ~~.nav-links CSS (39-45)~~ · ~~<nav class="nav-links"> Sales·Expenses·SLWA(broken)·Sync (136-140)~~
REPLACEMENT  + nav.js (→ P&L active) · - remove nav + CSS (page de-linked, reachable as P&L detail-view)
```

### static/data-quality.html
```
CURRENT  ~~<a class="back-link" href="/">← Back to Dashboard</a> (190) + CSS (53-57)~~ ·
         thead{position:sticky;top:0} (121) · .dq-header page title [KEEP]
REPLACEMENT  + nav.js (→ DQ utility active) · - remove the back-link anchor + its CSS ·
             ~ thead top:0 → top:var(--qs-nav-h) · keep .dq-header as page title
```

### static/sync-history.html
```
CURRENT  ~~<a class="back-link" href="/">← Back to Dashboard</a> (117) + CSS (36-40)~~ ·
         th{position:sticky;top:0} (67) · <h1>Sync History</h1> [KEEP]
REPLACEMENT  + nav.js (→ Sync utility active) · - remove back-link anchor + CSS · ~ th top:0 → top:var(--qs-nav-h)
```

### static/slwa-dashboard.html  (SURGICAL)
```
CURRENT  header <a class="back-link" id="back-link"> (174) ←wired by buildBackLink()/:416 ·
         table-cell .back-link drill-down links (464) [MUST KEEP] · .back-link CSS [shared — MUST KEEP]
REPLACEMENT  + nav.js (→ Sales active)
  CARVE-OUT: keep the JS-wired #back-link header link AS-IS (removing it null-throws renderSubtitle()).
  Harmless redundancy with Sales-active. DO NOT remove the table-cell .back-link (464) or the .back-link CSS.
  (Backlog: clean up the redundant header back-link later, after neutralizing the #back-link JS.)
```

### static/debug.html
```
CURRENT  <a class="back-link" id="back-link" href="/"> (588) + CSS (100-112) · thead{sticky;top:0} (227)
REPLACEMENT  + nav.js (→ Sales active) · ~ thead top:0 → top:var(--qs-nav-h) ·
  #back-link: CARVE-OUT keep (has id="back-link"; verify-no-JS or keep to avoid null-throw). Dev page — low stakes.
```

---

## How it fails / scale / rollback

- **Fails:** (1) a page omits the include → no nav (caught by "every page renders Tier-1"). (2) duplicate bar → idempotent guard. (3) wrong active → activeKeyForPath unit cases. (4) SLWA `#back-link` removed without its JS → null-throw (prevented by carve-out + NO-REGRESSION console-error criterion). (5) thead clip → caught by NO-REGRESSION + design-drive.
- **Scale:** static; no data/load concern. New page = one include + a row in QS_NAV_* (or QS_NAV_DETAIL).
- **Rollback (<5 min):** Railway redeploy previous commit. No schema/migration. Fully reversible.

## Verification (for Lloyd)
Post-deploy: load all 9 pages on Railway; confirm the identical bar, correct active (incl. expenses→P&L, slwa/debug→Sales), no SLWA/Expenses top tabs, sticky table headers fully visible on data-quality/sync-history/debug, Deals lens still switches, zero console errors. `/design-drive` machine-compares the bar against the mockup.
