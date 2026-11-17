export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/fail") {
      return new Response("probe-response", { status: 503 });
    }
    return new Response("ok");
  },
};
