/* static/theme.js — shared theme controller (single source of truth for every page).
   Three choices, persisted in localStorage under 'qs-theme':
     'auto'  → follow the OS via prefers-color-scheme (default), live-updates on system change
     'light' → force the light palette
     'dark'  → force the dark palette (the original design; lives in each page's :root)
   The resolved theme is written to <html data-theme="light|dark"> as early as possible
   (this file is loaded in <head>) so there is no dark-then-light flash on load.
   Light is expressed purely as token overrides on :root[data-theme="light"]; dark stays
   the page default, so nothing about the existing dark design changes. */
(function () {
  'use strict';

  var STORAGE_KEY = 'qs-theme';
  var VALID = { auto: 1, light: 1, dark: 1 };
  var mql = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
  var buttons = []; // toggle buttons to keep in sync (one nav, but be safe)

  function getChoice() {
    try { var v = localStorage.getItem(STORAGE_KEY); return VALID[v] ? v : 'auto'; }
    catch (e) { return 'auto'; }
  }
  function systemDark() { return !!(mql && mql.matches); }
  function resolve(choice) {
    if (choice === 'auto') return systemDark() ? 'dark' : 'light';
    return choice;
  }
  function applyResolved() {
    document.documentElement.setAttribute('data-theme', resolve(getChoice()));
  }

  // 1) Apply immediately — runs during <head> parse, before the body paints.
  applyResolved();

  // 2) Inject the light palette + toggle styles once.
  function injectStyles() {
    if (document.getElementById('qs-theme-styles')) return;
    var css = [
      // native form controls / scrollbars follow the theme
      ":root{color-scheme:dark;}",
      ':root[data-theme="light"]{color-scheme:light;}',
      // ── Light palette: overrides every token the pages define in their dark :root.
      //    Higher specificity than :root, so it wins on every page (incl. deals' own accent). ──
      ':root[data-theme="light"]{',
        "--bg:#f4f6fa;--surface:#ffffff;--surface2:#eef1f6;--border:#dce2ec;",
        "--accent:#4f46e5;--accent2:#6366f1;",
        "--text:#1e293b;--text2:#64748b;--text3:#94a3b8;--muted:#64748b;",
        "--green:#16a34a;--red:#dc2626;--yellow:#d97706;--orange:#ea580c;--blue:#2563eb;",
        // pnl.html tints, recomputed against the light status colors
        "--green-dim:rgba(22,163,74,0.10);--green-border:rgba(22,163,74,0.28);",
        "--red-dim:rgba(220,38,38,0.10);--red-border:rgba(220,38,38,0.28);",
        "--yellow-dim:rgba(217,119,6,0.12);",
        "--blue-dim:rgba(37,99,235,0.10);--blue-border:rgba(37,99,235,0.28);",
      "}",
      // ── Segmented Auto / Light / Dark control (lives in the shared nav util group) ──
      ".qs-theme-seg{display:inline-flex;align-items:center;gap:2px;margin-left:8px;padding:2px;",
        "border:1px solid var(--border,#2e3347);border-radius:8px;background:var(--bg,#0f1117);}",
      ".qs-theme-seg button{display:inline-flex;align-items:center;justify-content:center;width:28px;height:24px;",
        "padding:0;border:none;border-radius:6px;background:none;color:var(--text2,#94a3b8);cursor:pointer;",
        "transition:background .15s,color .15s;}",
      ".qs-theme-seg button:hover{color:var(--text,#e2e8f0);background:var(--surface2,#22263a);}",
      ".qs-theme-seg button.active{color:#fff;background:var(--accent,#6366f1);}",
      ".qs-theme-seg svg{width:15px;height:15px;display:block;}"
    ].join('');
    var style = document.createElement('style');
    style.id = 'qs-theme-styles';
    style.textContent = css;
    (document.head || document.documentElement).appendChild(style);
  }
  injectStyles();

  // 3) Keep 'auto' live when the OS theme flips.
  function onSystemChange() { if (getChoice() === 'auto') { applyResolved(); refreshButtons(); } }
  if (mql) {
    if (mql.addEventListener) mql.addEventListener('change', onSystemChange);
    else if (mql.addListener) mql.addListener(onSystemChange); // older Safari
  }

  function setChoice(choice) {
    if (!VALID[choice]) choice = 'auto';
    try { localStorage.setItem(STORAGE_KEY, choice); } catch (e) {}
    applyResolved();
    refreshButtons();
  }

  function refreshButtons() {
    var choice = getChoice();
    buttons.forEach(function (b) {
      var on = b.getAttribute('data-choice') === choice;
      b.classList.toggle('active', on);
      b.setAttribute('aria-pressed', on ? 'true' : 'false');
    });
  }

  var ICONS = {
    auto: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/></svg>',
    light: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>',
    dark: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>'
  };

  // Returns a fresh segmented control; the shared nav appends this into its util group.
  function createToggle() {
    var seg = document.createElement('div');
    seg.className = 'qs-theme-seg';
    seg.setAttribute('role', 'group');
    seg.setAttribute('aria-label', 'Color theme');
    [['auto', 'Auto (match system)'], ['light', 'Light'], ['dark', 'Dark']].forEach(function (pair) {
      var key = pair[0];
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.setAttribute('data-choice', key);
      btn.title = pair[1];
      btn.setAttribute('aria-label', pair[1]);
      btn.innerHTML = ICONS[key];
      btn.addEventListener('click', function () { setChoice(key); });
      buttons.push(btn);
      seg.appendChild(btn);
    });
    refreshButtons();
    return seg;
  }

  window.QSTheme = { get: getChoice, set: setChoice, createToggle: createToggle };
})();
