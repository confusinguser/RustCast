// Resolve API URLs relative to the path the app is served from
const BASE = new URL(".", document.baseURI).pathname;

/** Build a URL for an API path (leading slash optional). */
export function apiUrl(path: string): string {
  return BASE + path.replace(/^\//, "");
}
