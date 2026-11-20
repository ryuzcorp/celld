# WebAssembly

A Worker bundle can import a `.wasm` file. As in Wrangler, the import
gives you the compiled module, not the bytes.

```js
import addModule from "./add.wasm";

const { exports } = new WebAssembly.Instance(addModule);

export default {
  fetch() {
    return new Response(String(exports.add(2, 3)));
  },
};
```

`cello deploy` uploads each imported wasm file beside the bundle, marks
the deployment with the `wasm-v1` feature, and reports success once the
upload lands. A node that predates this feature refuses the manifest when
it polls the deployment pointer, so a mixed fleet surfaces the failure at
adoption time, after the command has already succeeded. Check that the
nodes adopted the deployment (node logs, or `POST /reload` to adopt now)
instead of trusting the command result.

Cello compiles each wasm module once per process, so later isolates and
cell activations reuse it.

## Run the example

The [WebAssembly example](https://github.com/ryuzcorp/cello/tree/main/examples/wasm)
compiles a Rust Durable Object and imports its WebAssembly module.

<!-- cello-example: wasm -->

## Ship a prebuilt bundle

With `no_bundle: true`, Cello preserves your entry JavaScript byte for
byte. It applies Wrangler's default `**/*.wasm` and `**/*.wasm?module`
patterns below the directory that holds `main`. For example,
`main: "./dist/shim.mjs"` can import `"./add.wasm"` or
`"./lib/add.wasm"` from `dist`. The module names keep these relative
paths. This mode does not need esbuild.

The scan uploads WASM files that your JavaScript does not import, so use
a dedicated build output directory. It skips `.git`, `.cello`,
`.wrangler`, and symbolic links to directories. It refuses a symbolic link
whose name matches a WASM pattern, so copy the file into the build output
instead.

Cello does not implement the other
[Wrangler module discovery settings](https://developers.cloudflare.com/workers/wrangler/configuration/#find-additional-modules).
It finds no extra JavaScript modules and accepts no `rules`, `base_dir`,
or `find_additional_modules`. Your JavaScript must already be bundled, and
its WASM imports must sit relative to the entry directory.

## Build Rust with workers-rs

[workers-rs](https://github.com/cloudflare/workers-rs) builds a
JavaScript shim and a wasm file, and the shim is a normal entry point for
`cello deploy`.

1. Install the build tool: `cargo install worker-build`.
2. Build the crate: `worker-build --release`.
3. Point the config at the shim:

```jsonc
{
  "name": "my-app",
  "main": "./build/worker/shim.mjs",
  "compatibility_date": "2026-01-01",
}
```

4. Deploy: `cello deploy`.

Cello resolves the entrypoint and Durable Object classes through the
shim's Proxy wrapper. A workers-rs API that needs a runtime feature
missing from [Cloudflare compatibility](cloudflare-compat.md) does not
work.

## Write Python Workers instead

For Python Workers, see [Workers](../services/workers.md#python-workers):
that page owns the Pyodide runtime version, the build steps, and the list
of supported Python APIs. This page covers WebAssembly only.

## Dynamic Workers

Give a dynamically loaded worker wasm as `{ wasm: bytes }` in the
`modules` map, and the worker imports a compiled module. Cello refuses
bare bytes, as workerd does.

```js
const worker = env.loader.load({
  compatibilityDate: "2025-01-01",
  mainModule: "main.js",
  modules: {
    "main.js": `import m from "./add.wasm"; ...`,
    "add.wasm": { wasm: wasmBytes },
  },
});
```

## Mind the limits

Wasm bytes count against the deployment size limits like JavaScript
modules. A module that does not compile fails the importing module with a
`WebAssembly.CompileError` that names the file.
