// Staged-activation fixture: a queue consumer, proving attachments move at
// activate time rather than at stage time.
export default {
  async fetch() {
    return new Response("consumer");
  },
  async queue(batch) {
    void batch;
  },
};
