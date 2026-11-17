// Modified by ryuzcorp (ryuzcorp/celld): shared-fetch waiter regression fixture (upstream #316). Apache-2.0.

// B awaits A's shared subrequest, then arms its own timer/fetch: the
// waiter's next op must belong to the waiter's driver, not to the entry
// whose checkpoint resumed it. Adopted by the wrong driver, the op dies
// with that entry's answer and the waiter hangs to its budget.
const flights = new Map();

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/health") return new Response("ok");
    const key = url.searchParams.get("key") ?? "k";
    if (url.pathname === "/start") {
      // A's shared fetch: slow enough to stay in flight while B joins.
      const flight = fetch(url.searchParams.get("slow")).then((r) => r.text());
      flights.set(key, flight);
      const value = await flight;
      return Response.json({ value });
    }
    if (url.pathname === "/join") {
      let flight = flights.get(key);
      if (!flight) {
        // Wait for the starter to publish (bounded): the test gives A a
        // head start, and this covers a slow one without racing it.
        const start = Date.now();
        while (!flight && Date.now() - start < 5000) {
          await new Promise((r) => setTimeout(r, 10));
          flight = flights.get(key);
        }
        if (!flight) return Response.json({ error: "no-flight" }, { status: 500 });
      }
      const value = await flight;
      const then = url.searchParams.get("then");
      if (then === "timer") {
        await new Promise((r) => setTimeout(r, 20));
      } else if (then === "fetch") {
        const plain = new URL("/plain", url.searchParams.get("slow")).toString();
        const response = await fetch(plain);
        await response.text();
      }
      return Response.json({ value, then });
    }
    return new Response("not found", { status: 404 });
  },
};
