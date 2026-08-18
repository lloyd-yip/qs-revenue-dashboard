/* static/metric-colors.js — distribution-relative metric colouring.

   WHY NOT FIXED THRESHOLDS
   A hard-coded cutoff ("green above 60%") only ever suits the one metric it was
   written for. Applied to close rate it painted every single row red, because no
   high-ticket close rate is anywhere near 60% — the colour stopped meaning
   "good or bad" and just meant "this is a close rate".

   WHAT THIS DOES INSTEAD
   Grade each row against the rest of the column actually on screen:

     centre = the POOLED rate — Σ numerator ÷ Σ denominator across the graded
              rows, i.e. the table's real average (the same number the TOTAL row
              shows). Pooled, not the mean of the per-row rates, so a segment
              with 3 calls cannot tug the centre as hard as one with 300.
     band   = K × MAD, the MEDIAN absolute deviation from that centre. The median
              is the outlier-resistant part, and the reason a couple of
              spectacular or dreadful rows can't throw the scale off: they would
              stretch a mean/standard-deviation band far enough to wash the whole
              column out, but they barely move a median.
     green  = better than centre + band · red = worse than centre − band ·
              amber = the middle of the pack. "Better" flips for metrics where
              low is good (cost per close, avg cycle, reschedule rate).

   GUARDS — the things that stop this producing confident nonsense
     - A row is only graded once its DENOMINATOR reaches MIN_SAMPLE. One close
       out of three calls is 33%, which is noise, not excellence.
     - Ungraded rows are excluded from the centre and the band too, so noise
       never gets to define the scale it would then be judged by.
     - Below MIN_ROWS graded rows there is no distribution at all, so nothing in
       that column is coloured — better blank than invented.
     - The band never shrinks below MIN_REL_SPREAD of the centre, so when the
       whole team is bunched together the column stays honestly amber instead of
       magnifying decimal-point gaps into green-vs-red.

   Every graded cell carries a title explaining the scale it was judged on, so
   the colour is never unexplained.

   Usage:
     var s = MetricColors.build(rows, {
       value: r => r.close_rate,            // the number shown in the cell
       num:   r => r.units_closed,          // pooled-centre numerator   (optional)
       den:   r => r.shows_1st,             // pooled-centre denominator (optional)
       sample: r => r.shows_1st,            // volume gate               (optional)
       higherIsBetter: true,                // default true
       format: MetricColors.fmt.pct,
     });
     '<td class="' + s.grade(row) + '" title="' + s.explain(row) + '">'
*/
(function () {
  'use strict';

  var MIN_SAMPLE = 8;      // denominator a row needs before it is graded at all
  var MIN_ROWS = 3;        // graded rows needed before a distribution exists
  var K = 1;               // how many MADs from the centre earns green / red
  var MIN_REL_SPREAD = 0.10; // band floor, as a fraction of the centre

  function median(xs) {
    if (!xs.length) return null;
    var s = xs.slice().sort(function (a, b) { return a - b; });
    var m = Math.floor(s.length / 2);
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
  }

  function finite(v) {
    return typeof v === 'number' && isFinite(v) ? v : null;
  }

  // ── Formatters — used only in the explanatory tooltip ──
  var fmt = {
    pct:   function (v) { return (v * 100).toFixed(1) + '%'; },
    pctPt: function (v) { return v.toFixed(1) + '%'; },   // value already 0–100
    days:  function (v) { return v.toFixed(1) + 'd'; },
    money: function (v) { return '$' + Math.round(v).toLocaleString(); },
    mult:  function (v) { return v.toFixed(2) + 'x'; },
    plain: function (v) { return (Math.round(v * 10) / 10).toLocaleString(); },
  };

  function build(rows, spec) {
    var higher = spec.higherIsBetter !== false;
    var format = spec.format || fmt.plain;
    // Per-column override: metrics denominated in closed deals need a lower bar
    // than metrics denominated in calls — nobody closes 8 deals a month.
    var minSample = spec.minSample === undefined ? MIN_SAMPLE : spec.minSample;

    function sampleOK(r) {
      if (!spec.sample) return true;
      var n = finite(spec.sample(r));
      return n !== null && n >= minSample;
    }

    var graded = [];
    (rows || []).forEach(function (r) {
      var v = finite(spec.value(r));
      if (v === null || !sampleOK(r)) return;
      graded.push({ row: r, v: v });
    });

    var scale = { n: graded.length, centre: null, band: null, higher: higher };

    if (graded.length >= MIN_ROWS) {
      var centre = null;
      if (spec.num && spec.den) {
        var tn = 0, td = 0;
        graded.forEach(function (g) {
          tn += finite(spec.num(g.row)) || 0;
          td += finite(spec.den(g.row)) || 0;
        });
        if (td > 0) centre = tn / td;
      }
      if (centre === null) centre = median(graded.map(function (g) { return g.v; }));
      var mad = median(graded.map(function (g) { return Math.abs(g.v - centre); })) || 0;
      scale.centre = centre;
      scale.band = Math.max(mad * K, Math.abs(centre) * MIN_REL_SPREAD);
    }

    scale.grade = function (r) {
      if (scale.centre === null) return '';
      var v = finite(spec.value(r));
      if (v === null || !sampleOK(r)) return '';
      if (v >= scale.centre + scale.band) return higher ? 'rate-good' : 'rate-bad';
      if (v <= scale.centre - scale.band) return higher ? 'rate-bad' : 'rate-good';
      return 'rate-warn';
    };

    // Plain text (no quotes) — safe to drop straight into a title="" attribute.
    scale.explain = function (r) {
      var v = finite(spec.value(r));
      if (v === null) return '';
      if (scale.centre === null) {
        return 'Not graded — fewer than ' + MIN_ROWS + ' rows in view carry enough volume to compare against.';
      }
      if (!sampleOK(r)) {
        var n = finite(spec.sample(r));
        return 'Not graded — only ' + (n === null ? 0 : n) + ' in the denominator (needs ' + minSample
             + '), too few to tell performance from noise.';
      }
      var hi = format(scale.centre + scale.band);
      var lo = format(scale.centre - scale.band);
      return 'Graded against the ' + scale.n + ' rows in view · average ' + format(scale.centre)
           + ' · green ' + (higher ? 'from ' + hi + ' up' : 'from ' + lo + ' down')
           + ', red ' + (higher ? 'from ' + lo + ' down' : 'from ' + hi + ' up')
           + ' · ' + (higher ? 'higher is better' : 'lower is better');
    };

    return scale;
  }

  /* Build many scales at once: specs is { key: spec }, returns { key: scale }. */
  function buildAll(rows, specs) {
    var out = {};
    Object.keys(specs).forEach(function (k) { out[k] = build(rows, specs[k]); });
    return out;
  }

  window.MetricColors = {
    build: build,
    buildAll: buildAll,
    fmt: fmt,
    MIN_SAMPLE: MIN_SAMPLE,
    MIN_ROWS: MIN_ROWS,
  };
})();
