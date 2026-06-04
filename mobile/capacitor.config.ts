import { CapacitorConfig } from '@capacitor/cli';

const config: CapacitorConfig = {
  appId: 'ai.justhodl.app',
  appName: 'JustHodl AI',
  // Bundled local shell (www/) is the entry; it loads the live platform inside.
  webDir: 'www',
  backgroundColor: '#0a0e14',
  plugins: {
    SplashScreen: {
      launchShowDuration: 1200,
      backgroundColor: '#0a0e14',
      showSpinner: false,
      androidScaleType: 'CENTER_CROP',
    },
    PushNotifications: {
      presentationOptions: ['badge', 'sound', 'alert'],
    },
    StatusBar: {
      style: 'DARK',
      backgroundColor: '#0a0e14',
    },
  },
  ios: {
    contentInset: 'always',
    backgroundColor: '#0a0e14',
  },
  android: {
    backgroundColor: '#0a0e14',
    allowMixedContent: false,
  },
};

export default config;
