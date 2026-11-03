// Noite's addition (ryuzcorp/noite, docker/celld/patches): R7 SvcRpc DO-stub fixture. Apache-2.0.
import { DurableObject, RpcTarget, WorkerEntrypoint } from 'cloudflare:workers';

class ControllerCap extends RpcTarget {
  async hello() { return 'controller-hello'; }
}

export class ControllerDO extends DurableObject {
  async ping(x) { return `pong:${x}`; }
}

export default class ControllerService extends WorkerEntrypoint {
  async getControllerStub(id) {
    return this.env.SANDBOX.getByName(id);
  }
  async pingBack(stub) {
    return { pong: await stub.ping('via-controller') };
  }
  async getCap() {
    return new ControllerCap();
  }
  async takeCap(cap) {
    return { hello: await cap.hello() };
  }
  async getStream() {
    return new ReadableStream({
      start(c) { c.enqueue(new TextEncoder().encode('x')); c.close(); },
    });
  }
  async takeStream(s) {
    const r = s.getReader();
    const out = await r.read();
    return { done: !!out.done };
  }
  async takeTwo(a, b) {
    return 'reached';
  }
  async inspectArg(x) {
    return { hasPing: typeof x?.ping };
  }
  // D3: the receiver reads the revived stub's name, which comes from the
  // lift marker's `n`. A sender-side `stub.name =` must not change it.
  async echoName(stub) {
    return { name: stub.name ?? null };
  }
}
