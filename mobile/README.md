# JustHodl AI — Mobile App (iOS + Android)

A [Capacitor](https://capacitorjs.com) app that wraps the live JustHodl.AI
platform in a native shell with a bottom tab bar, push notifications, offline
handling, and your app icon/splash. Because it loads the **live** site, new
tools appear without an app-store re-review.

```
mobile/
  capacitor.config.ts   app id (ai.justhodl.app), name, splash, push config
  www/index.html        native shell (tab bar, webview, splash, offline)
  www/app.js            tabs, navigation, push registration, back button
  resources/            icon + splash source art (1024px / 2732px)
```

## What I (the build) produced
- Full app code, icons, splash, and a CI workflow that builds the **Android
  bundle automatically** on a `mobile-v*` tag → attached to the GitHub Release.

## What only YOU can do (accounts + submission)
App-store publishing requires your identity, payment, and a Mac for iOS:

### One-time setup
- **Google Play:** create a [Play Console](https://play.google.com/console) account ($25 one-time).
- **Apple:** enroll in the [Apple Developer Program](https://developer.apple.com/programs/) ($99/year). iOS builds require a **Mac with Xcode**.

### Build locally (first time)
```bash
cd mobile
npm install
npx cap add ios          # Mac only
npx cap add android
npx capacitor-assets generate   # icons + splash from resources/
npx cap sync
```

### Android → Google Play
```bash
npm run open:android     # opens Android Studio
# Build > Generate Signed Bundle/APK > Android App Bundle (.aab)
# Create/keep your upload keystore safe.
```
Then in Play Console: create the app, upload the `.aab`, fill the store
listing (use `mobile/store/` copy below), set content rating + privacy policy,
submit for review (~hours–1 day).

### iOS → App Store (Mac required)
```bash
npm run open:ios         # opens Xcode
# Set your Team/signing, then Product > Archive > Distribute App > App Store Connect
```
Then in App Store Connect: create the app, fill the listing, submit. Apple
reviews finance apps strictly — be clear it's research/analytics, **not advice**.

## Push notifications
- The app registers a device token and POSTs it to
  `…/register-push` (stored in Cloudflare KV). Wiring the alert pusher to send
  to those tokens needs **Firebase Cloud Messaging** (Android) and **APNs**
  (iOS) credentials — set those up once in each console and add the keys.

## Updating
- **Content/tools:** nothing to do — the app loads the live site.
- **Native shell changes:** bump the version and re-submit (rare).
