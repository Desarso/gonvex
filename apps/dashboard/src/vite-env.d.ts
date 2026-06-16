/// <reference types="vite/client" />

declare module "@heroui/styles/css";

interface ImportMetaEnv {
  readonly VITE_GONVEX_URL?: string;
  readonly VITE_GONVEX_RUNTIME_URL?: string;
  readonly VITE_GONVEX_PROJECT_ID?: string;
  readonly VITE_GONVEX_AUTH_ENABLED?: string;
  readonly VITE_GONVEX_ALLOWED_EMAILS?: string;
  readonly VITE_GONVEX_ALLOW_UNLISTED_EMAILS?: string;
  readonly VITE_GONVEX_DEV_LOGIN_ENABLED?: string;
  readonly VITE_GONVEX_EMAIL_LOGIN_ENABLED?: string;
  readonly VITE_GONVEX_GOOGLE_LOGIN_ENABLED?: string;
  readonly VITE_GONVEX_PASSWORD_LOGIN_ENABLED?: string;
  readonly VITE_GONVEX_DATABASE?: string;
  readonly VITE_GONVEX_STORAGE_BUCKET?: string;
  readonly VITE_GONVEX_PROJECTS?: string;
  readonly VITE_FIREBASE_API_KEY?: string;
  readonly VITE_FIREBASE_AUTH_DOMAIN?: string;
  readonly VITE_FIREBASE_PROJECT_ID?: string;
  readonly VITE_FIREBASE_APP_ID?: string;
  readonly VITE_FIREBASE_MESSAGING_SENDER_ID?: string;
  readonly VITE_FIREBASE_STORAGE_BUCKET?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
