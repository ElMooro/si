/**
 * JustHodl Auth — shared authentication + entitlements module
 *
 * Drop into any page with:
 *   <script src="/auth-config.js"></script>
 *   <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
 *   <script src="/auth.js"></script>
 *
 * Then use:
 *   JustHodlAuth.init()                  // call once on page load
 *   JustHodlAuth.openSignIn()            // open the sign-in modal
 *   JustHodlAuth.signOut()
 *   JustHodlAuth.getUser()               // {id, email, ...} or null
 *   JustHodlAuth.getTier()               // 'free' | 'pro' | 'elite'
 *   JustHodlAuth.hasAccess('feature')    // bool, gating helper
 *   JustHodlAuth.onChange(cb)            // cb(user) when auth state changes
 *   JustHodlAuth.userKey()               // stable storage key (user id or device id)
 *
 * Gracefully degrades: if config.enabled=false or Supabase not loaded,
 * everything still works in anonymous mode (returns null user, 'free' tier).
 */
(function () {
  const CFG = window.JUSTHODL_AUTH_CONFIG || {};
  const ENABLED = !!CFG.enabled
    && CFG.supabaseUrl && !CFG.supabaseUrl.startsWith("REPLACE")
    && CFG.supabaseAnonKey && !CFG.supabaseAnonKey.startsWith("REPLACE");

  let supabase = null;
  let currentUser = null;
  let currentTier = "free";
  const changeListeners = [];

  // ── Tier → entitlements map (which features each tier unlocks) ──
  // Edit freely as your product evolves.
  const ENTITLEMENTS = {
    free: new Set([
      "chart_basic", "watchlists_own", "screener_basic", "signals_delayed",
    ]),
    pro: new Set([
      "chart_basic", "watchlists_own", "screener_basic", "signals_delayed",
      "signals_realtime", "cascade_alerts", "trade_tickets", "ai_thesis",
      "telegram_alerts", "multi_chart", "options_flow",
    ]),
    elite: new Set([
      "chart_basic", "watchlists_own", "screener_basic", "signals_delayed",
      "signals_realtime", "cascade_alerts", "trade_tickets", "ai_thesis",
      "telegram_alerts", "multi_chart", "options_flow",
      "insider_clusters", "activist_13d", "thirteen_f", "risk_desk",
      "crisis_kb", "liquidity_intel", "api_access", "weekly_memo",
      "calibration_panel", "all_horizons",
    ]),
  };

  function notify() {
    changeListeners.forEach((cb) => { try { cb(currentUser, currentTier); } catch (e) {} });
  }

  // ── Device ID fallback (anonymous mode) ──
  function deviceId() {
    let did = localStorage.getItem("jh_device_id");
    if (!did) {
      did = "d" + Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
      localStorage.setItem("jh_device_id", did);
    }
    return did;
  }

  const JustHodlAuth = {
    async init() {
      this._injectCSS();
      this._ensureSlot();
      if (!ENABLED) { this._renderAuthUI(); return; }
      if (typeof window.supabase === "undefined" || !window.supabase.createClient) {
        console.warn("[auth] Supabase JS not loaded; running anonymous.");
        this._renderAuthUI();
        return;
      }
      try {
        supabase = window.supabase.createClient(CFG.supabaseUrl, CFG.supabaseAnonKey);
        // Restore session (bounded so a network stall can't hang the page)
        const sessRes = await Promise.race([
          supabase.auth.getSession(),
          new Promise((res) => setTimeout(() => res({ data: { session: null } }), 3500)),
        ]);
        const session = sessRes && sessRes.data ? sessRes.data.session : null;
        if (session) {
          currentUser = session.user;
          await this._loadTier();
        }
        supabase.auth.onAuthStateChange(async (_event, s) => {
          currentUser = s ? s.user : null;
          if (currentUser) await this._loadTier();
          else currentTier = "free";
          this._renderAuthUI();
          notify();
        });
      } catch (e) {
        console.error("[auth] init error:", e);
      }
      this._renderAuthUI();
      notify();
    },

    async _loadTier() {
      // Real plan lives in the 'profiles' table (the Stripe webhook writes it).
      // Fall back to metadata, then 'free'. Bounded so it never hangs the page.
      currentTier = "free";
      try {
        if (supabase && currentUser) {
          const q = supabase.from("profiles").select("plan").eq("id", currentUser.id).single();
          const res = await Promise.race([
            q, new Promise((r) => setTimeout(() => r({ data: null }), 3000)),
          ]);
          const plan = res && res.data && res.data.plan;
          if (plan) { currentTier = plan; return; }
        }
        // ops 3366 (additive): server-authoritative fallback. The worker's
        // /plan/self reads the Stripe-webhook KV cache + profiles via the
        // service role — works even if client RLS reads are misconfigured.
        if (supabase && currentUser && CFG.syncBase) {
          try {
            const tk = await Promise.race([
              supabase.auth.getSession().then((r) => r.data.session && r.data.session.access_token),
              new Promise((r) => setTimeout(() => r(null), 1500)),
            ]);
            if (tk) {
              const pr = await Promise.race([
                fetch(CFG.syncBase + "/plan/self", { headers: { "Authorization": "Bearer " + tk } })
                  .then((r) => (r.ok ? r.json() : null)),
                new Promise((r) => setTimeout(() => r(null), 2500)),
              ]);
              if (pr && pr.plan) { currentTier = pr.plan; return; }
            }
          } catch (e) {}
        }
        currentTier = (currentUser && currentUser.user_metadata && currentUser.user_metadata.tier)
          || (currentUser && currentUser.app_metadata && currentUser.app_metadata.tier)
          || "free";
      } catch (e) { currentTier = "free"; }
    },

    getUser() { return currentUser; },
    getTier() { return currentTier; },
    isAuthed() { return !!currentUser; },

    hasAccess(feature) {
      const set = ENTITLEMENTS[currentTier] || ENTITLEMENTS.free;
      return set.has(feature);
    },

    // Stable per-user storage key — Supabase user id when logged in, else device id
    userKey() {
      return currentUser ? ("u_" + currentUser.id.replace(/-/g, "").slice(0, 32)) : deviceId();
    },

    onChange(cb) { changeListeners.push(cb); },

    // ── Auth actions ──
    async signInWithEmail(email) {
      if (!supabase) return { error: "auth disabled" };
      const { error } = await supabase.auth.signInWithOtp({
        email,
        options: { emailRedirectTo: window.location.href },
      });
      return { error: error ? error.message : null, sent: !error };
    },

    async signInWithPassword(email, password) {
      if (!supabase) return { error: "auth disabled" };
      const { error } = await supabase.auth.signInWithPassword({ email, password });
      return { error: error ? error.message : null };
    },

    async signUpWithPassword(email, password) {
      if (!supabase) return { error: "auth disabled" };
      const { error } = await supabase.auth.signUp({
        email, password,
        options: { emailRedirectTo: window.location.href },
      });
      return { error: error ? error.message : null };
    },

    async signInWithGoogle() {
      if (!supabase) return { error: "auth disabled" };
      const { error } = await supabase.auth.signInWithOAuth({
        provider: "google",
        options: { redirectTo: window.location.href },
      });
      return { error: error ? error.message : null };
    },

    async signInWithFacebook() {
      if (!supabase) return { error: "auth disabled" };
      const { error } = await supabase.auth.signInWithOAuth({
        provider: "facebook",
        options: { redirectTo: window.location.href },
      });
      return { error: error ? error.message : null };
    },

    async signOut() {
      if (supabase) await supabase.auth.signOut();
      currentUser = null; currentTier = "free";
      this._renderAuthUI(); notify();
    },

    getAccessToken() {
      // For authenticated calls to the Worker (entitlement checks)
      return supabase ? supabase.auth.getSession().then(r => r.data.session?.access_token) : Promise.resolve(null);
    },

    // ── Self-injecting slot: if the page defines its own [data-auth-slot], use it;
    // otherwise auto-create a small floating one in the top-right corner. This is what
    // makes Sign In actually visible on pages that never added the slot markup —
    // matches jh-nav-drawer.js's zero-per-page-setup pattern. ──
    _ensureSlot() {
      if (document.querySelector("[data-auth-slot]")) return;
      const host = document.createElement("div");
      host.setAttribute("data-auth-slot", "");
      host.id = "jh-auth-auto-slot";
      host.style.cssText = "position:fixed;top:14px;right:14px;z-index:999995";
      document.body.appendChild(host);
    },

    // ── UI: inject a sign-in button / user menu into [data-auth-slot] ──
    _injectCSS() {
      if (document.getElementById("jh-auth-css")) return;
      const s = document.createElement("style"); s.id = "jh-auth-css";
      s.textContent = [
        ".jh-signin-btn{background:#26ffaf;color:#0a0e14;border:none;border-radius:7px;padding:7px 16px;font-weight:700;font-size:13px;cursor:pointer;font-family:inherit}",
        ".jh-signin-btn:hover{background:#1be89a}",
        ".jh-auth-anon{font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;border:1px solid #2a3550;padding:3px 8px;border-radius:6px}",
        ".jh-user-menu{position:relative}",
        ".jh-user-trigger{display:flex;align-items:center;gap:7px;cursor:pointer;padding:4px 8px;border:1px solid #2a3550;border-radius:8px}",
        ".jh-avatar{width:24px;height:24px;border-radius:50%;background:#22d3ee;color:#0a0e14;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:12px}",
        ".jh-caret{color:#6f7b91;font-size:9px}",
        ".jh-tier-badge{font-family:ui-monospace,monospace;font-size:9px;font-weight:700;padding:2px 7px;border-radius:10px;border:1px solid}",
        ".jh-tier-badge.pro{color:#22d3ee;border-color:#22d3ee}.jh-tier-badge.elite{color:#a78bfa;border-color:#a78bfa}",
        ".jh-user-dropdown{position:absolute;right:0;top:120%;background:#0c1018;border:1px solid #2a3550;border-radius:10px;padding:10px;min-width:200px;display:none;z-index:9999;box-shadow:0 12px 40px rgba(0,0,0,.5)}",
        ".jh-user-menu.open .jh-user-dropdown{display:block}",
        ".jh-user-email{font-size:12px;color:#e1e8f4;font-family:ui-monospace,monospace;padding:4px 6px;word-break:break-all}",
        ".jh-user-tier{font-size:11px;color:#6f7b91;padding:4px 6px;border-bottom:1px solid #1c2433;margin-bottom:4px}",
        ".jh-menu-link{display:block;padding:8px 6px;color:#a8b3c7;font-size:13px;text-decoration:none;border-radius:5px}",
        ".jh-menu-link:hover{background:#131929;color:#fff}",
        // ── Sign-in modal (classes match _buildModal exactly) ──
        ".jh-auth-modal{position:fixed;inset:0;background:rgba(4,6,10,.72);backdrop-filter:blur(4px);z-index:99999;display:none;align-items:center;justify-content:center}",
        ".jh-auth-modal.open{display:flex}",
        ".jh-auth-card{position:relative;background:#0c1018;border:1px solid #2a3550;border-radius:16px;padding:28px 26px;width:min(380px,92vw);font-family:-apple-system,system-ui,sans-serif;box-shadow:0 24px 80px rgba(0,0,0,.6)}",
        ".jh-auth-close{position:absolute;top:14px;right:18px;background:none;border:none;color:#6f7b91;cursor:pointer;font-size:24px;line-height:1}",
        ".jh-auth-logo{font-family:ui-monospace,Menlo,monospace;font-weight:800;font-size:18px;color:#fff;margin-bottom:4px}",
        ".jh-auth-logo span{color:#6f7b91}",
        ".jh-auth-sub{color:#a8b3c7;font-size:13px;margin-bottom:18px;line-height:1.45}",
        ".jh-oauth-btn{width:100%;display:flex;align-items:center;justify-content:center;gap:9px;background:#fff;color:#1a1a1a;border:none;border-radius:9px;padding:11px;font-weight:600;font-size:14px;cursor:pointer;margin-bottom:9px}",
        ".jh-oauth-btn:hover{opacity:.92}",
        ".jh-oauth-btn.facebook{background:#1877F2;color:#fff}",
        ".jh-auth-divider{text-align:center;color:#6f7b91;font-size:11px;margin:14px 0;font-family:ui-monospace,monospace}",
        ".jh-auth-input{width:100%;background:#0f1420;border:1px solid #2a3550;border-radius:8px;color:#e1e8f4;padding:11px 13px;font-size:14px;margin-bottom:10px}",
        ".jh-auth-input:focus{outline:none;border-color:#22d3ee}",
        ".jh-auth-primary{width:100%;background:#26ffaf;color:#0a0e14;border:none;border-radius:9px;padding:12px;font-weight:700;font-size:14px;cursor:pointer;margin-bottom:8px}",
        ".jh-auth-primary:hover{background:#1be89a}",
        ".jh-auth-row{display:flex;gap:8px}.jh-auth-row .jh-auth-primary{margin-bottom:0}",
        ".jh-auth-err{color:#ff5577;font-size:12px;min-height:16px;margin-bottom:4px}",
        ".jh-auth-legal{color:#566072;font-size:10.5px;margin-top:12px;line-height:1.4;text-align:center}",
      ].join("");
      document.head.appendChild(s);
    },

    _renderAuthUI() {
      const slots = document.querySelectorAll("[data-auth-slot]");
      slots.forEach((slot) => {
        if (!ENABLED) {
          slot.innerHTML = `<span class="jh-auth-anon" title="Sign-in not yet configured">anon</span>`;
          return;
        }
        if (currentUser) {
          const email = currentUser.email || "account";
          const initial = (email[0] || "U").toUpperCase();
          const tierBadge = currentTier !== "free"
            ? `<span class="jh-tier-badge ${currentTier}">${currentTier.toUpperCase()}</span>` : "";
          slot.innerHTML = `
            <div class="jh-user-menu" id="jh-user-menu">
              <div class="jh-user-trigger">
                <span class="jh-avatar">${initial}</span>
                ${tierBadge}
                <span class="jh-caret">▼</span>
              </div>
              <div class="jh-user-dropdown">
                <div class="jh-user-email">${email}</div>
                <div class="jh-user-tier">Plan: <b>${currentTier.toUpperCase()}</b></div>
                <a href="/pricing.html" class="jh-menu-link">Manage Subscription</a>
                <a href="#" class="jh-menu-link" id="jh-signout">Sign Out</a>
              </div>
            </div>`;
          const trigger = slot.querySelector(".jh-user-trigger");
          const menu = slot.querySelector(".jh-user-menu");
          trigger.addEventListener("click", () => menu.classList.toggle("open"));
          slot.querySelector("#jh-signout").addEventListener("click", (e) => {
            e.preventDefault(); this.signOut();
          });
        } else {
          slot.innerHTML = `<button class="jh-signin-btn" id="jh-signin-btn">Sign In</button>`;
          slot.querySelector("#jh-signin-btn").addEventListener("click", () => this.openSignIn());
        }
      });
    },

    openSignIn() {
      if (!ENABLED) {
        alert("Sign-in isn't configured yet. (Set your Supabase keys in auth-config.js)");
        return;
      }
      let modal = document.getElementById("jh-auth-modal");
      if (!modal) { modal = this._buildModal(); document.body.appendChild(modal); }
      modal.classList.add("open");
    },

    closeSignIn() {
      const modal = document.getElementById("jh-auth-modal");
      if (modal) modal.classList.remove("open");
    },

    _buildModal() {
      const modal = document.createElement("div");
      modal.id = "jh-auth-modal";
      modal.className = "jh-auth-modal";
      modal.innerHTML = `
        <div class="jh-auth-card">
          <button class="jh-auth-close" id="jh-auth-close">×</button>
          <div class="jh-auth-logo">JustHodl<span>.AI</span></div>
          <div class="jh-auth-sub">Sign in to save your watchlists, settings & favorites</div>

          <button class="jh-oauth-btn google" id="jh-google">
            <svg viewBox="0 0 24 24" width="16" height="16"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>
            Continue with Google
          </button>
          <button class="jh-oauth-btn facebook" id="jh-facebook">
            <svg viewBox="0 0 24 24" width="16" height="16" fill="#1877F2"><path d="M24 12.07C24 5.4 18.63 0 12 0S0 5.4 0 12.07c0 6.02 4.39 11.01 10.13 11.93v-8.44H7.08v-3.49h3.05V9.41c0-3.02 1.79-4.69 4.53-4.69 1.31 0 2.68.24 2.68.24v2.97h-1.51c-1.49 0-1.96.93-1.96 1.89v2.25h3.33l-.53 3.49h-2.8V24C19.61 23.08 24 18.09 24 12.07z"/></svg>
            Continue with Facebook
          </button>

          <div class="jh-auth-divider"><span>or</span></div>

          <input type="email" class="jh-auth-input" id="jh-email" placeholder="you@email.com" autocomplete="email" />
          <input type="password" class="jh-auth-input" id="jh-password" placeholder="Password (min 6 chars)" autocomplete="current-password" />
          <div class="jh-auth-err" id="jh-auth-err"></div>
          <button class="jh-auth-primary" id="jh-signin-pw">Sign In</button>
          <div class="jh-auth-row">
            <a href="#" id="jh-signup">Create account</a>
            <a href="#" id="jh-magic">Email me a magic link</a>
          </div>
          <div class="jh-auth-legal">By continuing you agree to the Terms & Privacy Policy.</div>
        </div>`;

      const err = modal.querySelector("#jh-auth-err");
      const showErr = (m) => { err.textContent = m || ""; err.style.display = m ? "block" : "none"; };

      modal.querySelector("#jh-auth-close").addEventListener("click", () => this.closeSignIn());
      modal.addEventListener("click", (e) => { if (e.target === modal) this.closeSignIn(); });
      modal.querySelector("#jh-google").addEventListener("click", () => this.signInWithGoogle());
      modal.querySelector("#jh-facebook").addEventListener("click", () => this.signInWithFacebook());

      modal.querySelector("#jh-signin-pw").addEventListener("click", async () => {
        showErr("");
        const email = modal.querySelector("#jh-email").value.trim();
        const pw = modal.querySelector("#jh-password").value;
        if (!email || !pw) { showErr("Enter email and password."); return; }
        const r = await this.signInWithPassword(email, pw);
        if (r.error) showErr(r.error); else this.closeSignIn();
      });
      modal.querySelector("#jh-signup").addEventListener("click", async (e) => {
        e.preventDefault(); showErr("");
        const email = modal.querySelector("#jh-email").value.trim();
        const pw = modal.querySelector("#jh-password").value;
        if (!email || pw.length < 6) { showErr("Email + password (6+ chars) required."); return; }
        const r = await this.signUpWithPassword(email, pw);
        if (r.error) showErr(r.error);
        else showErr("Check your email to confirm your account.");
      });
      modal.querySelector("#jh-magic").addEventListener("click", async (e) => {
        e.preventDefault(); showErr("");
        const email = modal.querySelector("#jh-email").value.trim();
        if (!email) { showErr("Enter your email first."); return; }
        const r = await this.signInWithEmail(email);
        if (r.error) showErr(r.error);
        else showErr("Magic link sent — check your email.");
      });
      return modal;
    },
  };

  window.JustHodlAuth = JustHodlAuth;
})();
