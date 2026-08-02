import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { viteSingleFile } from "vite-plugin-singlefile";

// The Rust server embeds the built UI as a single self-contained file via
// `include_str!("../web/dist/index.html")`, so we inline all JS + CSS into one
// HTML file. In dev, `/api` is proxied to the running server on :8080.
export default defineConfig({
  plugins: [react(), viteSingleFile()],
  build: {
    outDir: "dist",
    cssCodeSplit: false,
    assetsInlineLimit: 100_000_000,
    chunkSizeWarningLimit: 100_000,
  },
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:8080",
        changeOrigin: true,
        // SSE stream must not be buffered.
        ws: true,
      },
    },
  },
});
