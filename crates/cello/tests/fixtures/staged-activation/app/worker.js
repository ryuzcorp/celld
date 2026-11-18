// Staged-activation fixture: the primary script, calling the helper over a
// service binding. Tests copy this project and rewrite worker.js to mint a
// new deployment version.
export default {
  async fetch(request, env) {
    return env.HELPER.fetch(request);
  },
};
