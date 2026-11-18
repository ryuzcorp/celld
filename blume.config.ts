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
    copyright: "© 2026 Ryuz Corp. Based on celld, © 2026 Deno Land Inc.",
    links: [
      { label: "GitHub", href: "https://github.com/ryuzcorp/cello" },
      { label: "Changelog", href: "/changelog" },
    ],
  },
  deployment: {
    site: "https://cello.ryuz.eu",
  },
});
