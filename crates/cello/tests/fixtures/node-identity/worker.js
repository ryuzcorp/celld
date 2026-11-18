export default {
  fetch(request, env) {
    return Response.json({ node: env.CELLD_NODE ?? null });
  },
};
