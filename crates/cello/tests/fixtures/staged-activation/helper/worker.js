// Staged-activation fixture: the service-binding target. Tests copy this
// project and rewrite worker.js to mint a new deployment version.
export default {
  async fetch() {
    return new Response("helper");
  },
};
