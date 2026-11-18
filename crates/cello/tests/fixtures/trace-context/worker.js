import { getCurrentTraceparent } from "cloudflare:workers";

export default {
  fetch(request) {
    return Response.json({ traceparent: getCurrentTraceparent() ?? null });
  },
};
