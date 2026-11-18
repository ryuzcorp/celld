export default {
  fetch(request, env) {
    return Response.json({ node: env.CELLO_NODE ?? null });
  },
};
