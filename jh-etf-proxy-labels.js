/* Labels for data/etf-flows.json only. Legacy enums remain calculation inputs. */
(function (root) {
  'use strict';
  var labels = {
    HEAVY_INFLOW: 'High trading dollar-volume / price up (proxy)',
    HEAVY_OUTFLOW: 'High trading dollar-volume / price down (proxy)',
    ROTATION_IN: 'Rising trading activity / price up (proxy)',
    ROTATION_OUT: 'Rising trading activity / price down (proxy)',
    UNUSUAL_VOL: 'Unusual trading activity (proxy)',
    QUIET: 'No flagged trading-activity pattern (proxy)'
  };
  function label(signal) {
    return typeof signal === 'string' && Object.prototype.hasOwnProperty.call(labels, signal)
      ? labels[signal] : 'Trading-activity proxy unavailable';
  }
  function describe(row) {
    row = row && typeof row === 'object' ? row : {};
    var stamp = row.price_volume_measurement && row.price_volume_measurement.as_of;
    var clock = typeof stamp === 'string' && Number.isFinite(Date.parse(stamp))
      ? 'Reported daily bar ' + stamp : 'Bar time unavailable';
    return label(row.flow_signal) + ' · ' + clock;
  }
  var api = {label: label, describe: describe};
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.JHEtfProxyLabels = api;
}(typeof globalThis !== 'undefined' ? globalThis : this));
