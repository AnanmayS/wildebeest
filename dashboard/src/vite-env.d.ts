/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Grafana link shown in the header (e.g. http://localhost:3300/d/wildebeest-overview). Unset = no link. */
  readonly VITE_GRAFANA_URL?: string;
}
