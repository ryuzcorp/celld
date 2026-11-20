import { defineConfig } from "blume";

export default defineConfig({
  title: "Cello",
  description:
    "Self-hosted, distributed Durable Objects. Run a Cloudflare Workers application on your own machines.",
  github: {
    owner: "ryuzcorp",
    repo: "cello",
  },
  footer: {
    copyright: "© 2026 Ryuz Corp.",
    links: [
      { label: "GitHub", href: "https://github.com/ryuzcorp/cello" },
      { label: "Changelog", href: "/changelog" },
    ],
  },
  deployment: {
    site: "https://cello.ryuz.eu",
  },
  redirects: [
    { from: "/operations", to: "/guides/run-fleet", status: 301 },
    { from: "/compatibility", to: "/guides/upgrade", status: 301 },
    { from: "/staged-invoke", to: "/guides/staged-versions", status: 301 },
    { from: "/telemetry", to: "/guides/telemetry", status: 301 },
    { from: "/security", to: "/guides/security", status: 301 },
    { from: "/guarantees", to: "/concepts/durability", status: 301 },
    { from: "/testing", to: "/concepts/testing", status: 301 },
    { from: "/cloudflare-compat", to: "/reference/cloudflare-compat", status: 301 },
    { from: "/limitations", to: "/reference/limits", status: 301 },
    { from: "/wasm", to: "/reference/wasm", status: 301 },
    { from: "/contributing", to: "/project/contributing", status: 301 },
    { from: "/what-cello-adds", to: "/project/what-cello-adds", status: 301 },
    { from: "/what-cello-adds/:slug*", to: "/project/what-cello-adds/:slug*", status: 301 },
  ],
});
