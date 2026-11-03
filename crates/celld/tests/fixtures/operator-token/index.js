// Modified by ryuzcorp (ryuzcorp/celld): operator token for the internal listener. Apache-2.0.
//
// Fixture for the operator-token conformance runner. The tests only need a
// booted node and its listeners, so the Worker answers a constant.
export default {
  async fetch() {
    return new Response("ok");
  },
};
