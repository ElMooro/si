/**
 * JustHodl Auth Config
 *
 * Fill in your Supabase project values below, then set enabled=true.
 * Both values are SAFE to expose in client code (that's their design):
 *   - supabaseUrl:     Supabase → Project Settings → API → Project URL
 *   - supabaseAnonKey: Supabase → Project Settings → API → anon/public key
 *
 * The anon key is row-level-security-scoped; it cannot bypass your DB rules.
 * NEVER put the service_role key here — that one stays server-side only.
 */
window.JUSTHODL_AUTH_CONFIG = {
  supabaseUrl: "https://bdmjenqcyvzouusfcgow.supabase.co",
  supabaseAnonKey: "sb_publishable_W6V6OaQ9aXvpVVV9k4Lrpg_pCPuBLvB",

  // Flip to true after pasting your keys above.
  // When false, the app runs in anonymous mode (device-ID storage, no login UI).
  enabled: true,

  // Data sync worker (per-user KV-backed storage)
  syncBase: "https://justhodl-data-proxy.raafouis.workers.dev",

  // Stripe publishable key (safe client-side). Set after creating Stripe products.
  stripePublishableKey: "",

  // Map your Stripe Price IDs → tier names (set after creating products in Stripe)
  stripePrices: {
    // "price_xxx": "pro",
    // "price_yyy": "elite",
  },
};
