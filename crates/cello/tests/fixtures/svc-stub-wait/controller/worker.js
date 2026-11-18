// Modified by ryuzcorp (ryuzcorp/celld): cross-event retained-wait fixture. Apache-2.0.
import { DurableObject, RpcTarget, WorkerEntrypoint } from 'cloudflare:workers';

// A capability holding a promise minted by an earlier native op: `wait`
// returns the retained promise itself, `control` an already-resolved one.
class WaitCap extends RpcTarget {
  constructor(promise) {
    super();
    this.promise = promise;
  }
  async invoke(operation) {
    if (operation === 'wait') return this.promise;
    if (operation === 'control') return Promise.resolve('control-7');
    throw new TypeError(`unknown operation ${operation}`);
  }
}

const testWait = () => {
  const hook = globalThis.__celldTestWaitForTestOnly;
  if (hook === undefined) throw new Error('test waits are disabled on this node');
  return hook;
};

export class WaitDO extends DurableObject {
  // Mint one retained native wait and hand back a capability holding its
  // promise. The minting event retires with the reply; the promise must
  // still deliver in whatever later event awaits it.
  async makeWaiter(id) {
    const { promise } = testWait().makeWait(id);
    return { cap: new WaitCap(promise), id };
  }
  async fireWait(id, value) {
    testWait().fire(id, value);
    return 'fired';
  }
}

export default class WaitService extends WorkerEntrypoint {
  async getWaitStub(name) {
    return this.env.WAITS.getByName(name);
  }
}
