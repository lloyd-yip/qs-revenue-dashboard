/* Shared instant tooltip — pops on hover with NO delay, follows the cursor, and
   flips to stay on-screen. Matches the Deals page style. Works on any element
   carrying either data-tip="…" or a native title="…". Native titles are absorbed
   (moved to data-tip and the title removed) so the browser's own delayed tooltip
   never fires. Include once per page: <script src="/static/tooltip.js"></script> */
(function () {
  if (window.__gtipInit) return;
  window.__gtipInit = true;

  var style = document.createElement('style');
  style.textContent =
    '#__gtip{position:fixed;z-index:100000;max-width:340px;background:#13131f;color:#e2e8f0;' +
    'border:1px solid rgba(255,255,255,0.14);border-radius:8px;padding:10px 13px;font-size:12px;' +
    'line-height:1.55;white-space:pre-line;box-shadow:0 8px 28px rgba(0,0,0,0.5);pointer-events:none;' +
    'opacity:0;transform:translateY(3px);transition:opacity .07s ease,transform .07s ease;text-align:left;}' +
    '#__gtip.show{opacity:1;transform:translateY(0);}' +
    '[data-tip],[title]{cursor:help;}';
  document.head.appendChild(style);

  var tip = document.createElement('div');
  tip.id = '__gtip';
  function attach() { if (document.body && !tip.parentNode) document.body.appendChild(tip); }
  if (document.body) attach(); else document.addEventListener('DOMContentLoaded', attach);

  var active = null;

  function textFor(el) {
    var t = el.getAttribute('data-tip');
    if (t == null || t === '') {
      var nt = el.getAttribute('title');
      if (nt) { el.setAttribute('data-tip', nt); el.removeAttribute('title'); t = nt; }
    }
    return t;
  }

  function place(e) {
    var pad = 14, tw = tip.offsetWidth, th = tip.offsetHeight;
    var x = e.clientX + pad, y = e.clientY + pad;
    if (x + tw > window.innerWidth - 8) x = e.clientX - tw - pad;
    if (y + th > window.innerHeight - 8) y = e.clientY - th - pad;
    tip.style.left = Math.max(8, x) + 'px';
    tip.style.top = Math.max(8, y) + 'px';
  }

  document.addEventListener('mouseover', function (e) {
    var el = e.target.closest && e.target.closest('[data-tip],[title]');
    if (!el) return;
    var t = textFor(el);
    if (!t || t === '–' || t === '—') return;
    attach();
    active = el;
    tip.textContent = t;
    tip.classList.add('show');
    place(e);
  });
  document.addEventListener('mousemove', function (e) { if (active) place(e); });
  document.addEventListener('mouseout', function (e) {
    if (active && (e.target === active || active.contains(e.target)) && !active.contains(e.relatedTarget)) {
      active = null;
      tip.classList.remove('show');
    }
  });
})();
