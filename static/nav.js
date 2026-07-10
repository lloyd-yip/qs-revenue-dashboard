/* static/nav.js — shared Tier-1 top navigation (single source of truth for every page).
   Owns: nav styles (incl. --qs-nav-h + z-index), markup, and active-by-URL state.
   Tier-2 (Sales underline sub-tabs, Deals segmented lens) stays page-owned but uses the
   .subbar / .stab classes injected here. Tokens use var(--x, #fallback) so the bar renders
   identically even on a page whose :root is incomplete. */
(function () {
  'use strict';

  var MAIN = [
    { key: 'sales', label: 'Sales', href: '/dashboard' },
    { key: 'pnl',   label: 'P&L',   href: '/pnl' },
    { key: 'deals', label: 'Deals', href: '/deals' }
  ];
  var UTIL = [
    { key: 'dq',       label: 'Data Quality', href: '/data-quality' },
    { key: 'sync',     label: 'Sync',         href: '/sync-history' },
    { key: 'settings', label: 'Settings',     href: '/settings' }
  ];
  // detail views: dropped from the top nav but reachable from within a section → keep PARENT active
  var DETAIL = { '/expenses': 'pnl', '/channels/slwa': 'sales', '/debug': 'sales' };

  function activeKeyForPath(pathname) {
    var p = (pathname || '/').split('?')[0].split('#')[0];
    if (p.length > 1 && p.charAt(p.length - 1) === '/') p = p.slice(0, -1);
    if (p === '' || p === '/' || p === '/dashboard') return 'sales';
    if (p === '/pnl') return 'pnl';
    if (p === '/deals') return 'deals';
    if (p === '/data-quality') return 'dq';
    if (p === '/sync-history') return 'sync';
    if (p === '/settings') return 'settings';
    if (Object.prototype.hasOwnProperty.call(DETAIL, p)) return DETAIL[p];
    return null;
  }

  function injectStyles() {
    if (document.getElementById('qs-nav-styles')) return;
    var css = [
      ":root{--qs-nav-h:52px;}",
      ".qs-topbar{box-sizing:border-box;height:var(--qs-nav-h);display:flex;align-items:center;gap:6px;",
        "padding:0 22px;background:var(--surface,#1a1d27);border-bottom:1px solid var(--border,#2e3347);",
        "position:sticky;top:0;z-index:500;font-family:var(--font,'Inter',system-ui,-apple-system,sans-serif);}",
      ".qs-brand{font-weight:700;font-size:15px;letter-spacing:-0.2px;margin-right:20px;white-space:nowrap;color:var(--text,#e2e8f0);}",
      ".qs-brand .qs-mk{color:var(--accent2,#818cf8);}",
      ".qs-main{display:flex;gap:4px;}",
      ".qs-mtab{background:none;border:none;color:var(--text2,#94a3b8);font-family:inherit;font-size:13.5px;font-weight:600;",
        "padding:7px 15px;border-radius:8px;cursor:pointer;text-decoration:none;transition:background .15s,color .15s;}",
      ".qs-mtab:hover{background:var(--surface2,#22263a);color:var(--text,#e2e8f0);}",
      ".qs-mtab.active{background:var(--accent,#6366f1);color:#fff;}",
      ".qs-util-group{margin-left:auto;display:flex;gap:4px;align-items:center;}",
      ".qs-utab{background:none;border:1px solid transparent;color:var(--text2,#94a3b8);font-family:inherit;font-size:12.5px;",
        "font-weight:500;padding:6px 12px;border-radius:8px;cursor:pointer;text-decoration:none;transition:all .15s;}",
      ".qs-utab:hover{background:var(--surface2,#22263a);border-color:var(--border,#2e3347);color:var(--text,#e2e8f0);}",
      ".qs-utab.active{color:var(--accent2,#818cf8);border-color:var(--accent,#6366f1);background:rgba(99,102,241,0.08);}",
      /* shared Tier-2 container + underline idiom (pages populate these; Deals keeps its own .wl-lens) */
      ".subbar{display:flex;gap:0;padding:0 22px;min-height:42px;align-items:stretch;background:var(--bg,#0f1117);",
        "border-bottom:1px solid var(--border,#2e3347);position:sticky;top:var(--qs-nav-h);z-index:499;overflow-x:auto;}",
      ".subbar.qs-hidden{display:none;}",
      ".stab{background:none;border:none;border-bottom:2px solid transparent;color:var(--text2,#94a3b8);",
        "font-family:var(--font,'Inter',system-ui,sans-serif);font-size:13px;font-weight:500;padding:0 16px;cursor:pointer;",
        "white-space:nowrap;transition:color .15s,border-color .15s;}",
      ".stab:hover{color:var(--text,#e2e8f0);}",
      ".stab.active{color:var(--accent2,#818cf8);border-bottom-color:var(--accent,#6366f1);}"
    ].join('');
    var style = document.createElement('style');
    style.id = 'qs-nav-styles';
    style.textContent = css;
    document.head.appendChild(style);
  }

  function renderTopbar(activeKey) {
    if (document.querySelector('.qs-topbar')) return;
    var bar = document.createElement('nav');
    bar.className = 'qs-topbar';
    var html = '<span class="qs-brand">QS <span class="qs-mk">Revenue</span> Dashboard</span><div class="qs-main">';
    MAIN.forEach(function (m) {
      html += '<a class="qs-mtab' + (activeKey === m.key ? ' active' : '') + '" href="' + m.href + '">' + m.label + '</a>';
    });
    html += '</div><div class="qs-util-group">';
    UTIL.forEach(function (u) {
      html += '<a class="qs-utab' + (activeKey === u.key ? ' active' : '') + '" href="' + u.href + '">' + u.label + '</a>';
    });
    html += '</div>';
    bar.innerHTML = html;
    // Theme toggle (Auto/Light/Dark) lives at the right edge of the util group.
    if (window.QSTheme && typeof window.QSTheme.createToggle === 'function') {
      var util = bar.querySelector('.qs-util-group');
      if (util) util.appendChild(window.QSTheme.createToggle());
    }
    document.body.insertBefore(bar, document.body.firstChild);
  }

  function initNav() {
    injectStyles();
    renderTopbar(activeKeyForPath(window.location.pathname));
  }

  window.QSNav = { activeKeyForPath: activeKeyForPath }; // exposed for unit checks

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initNav);
  } else {
    initNav();
  }
})();
