// Modified by ryuzcorp (ryuzcorp/celld): node:url file URL helpers (upstream #280).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// node:url for cello: `pathToFileURL`/`fileURLToPath` plus the WHATWG URL
// re-exports. Cello runs on Linux, so only the POSIX path rules apply.
// Injected lazily, so an isolate that never imports node:url pays nothing.
(() => {
  "use strict";

  const urlError = (code, message) => {
    const error = new TypeError(message);
    error.code = code;
    return error;
  };

  // Resolve against `cwd` and normalize dot segments. Pure string work, the
  // same normalization the fs surface applies -- no host access.
  const posixResolve = (cwd, input) => {
    const absolute = input.startsWith("/") ? input : cwd + "/" + input;
    const parts = [];
    for (const part of absolute.split("/")) {
      if (!part || part === ".") continue;
      if (part === "..") parts.pop();
      else parts.push(part);
    }
    return "/" + parts.join("/");
  };

  const toPathInput = (value) => {
    if (typeof value === "string") return value;
    if (globalThis.Buffer?.isBuffer(value)) return value.toString();
    throw urlError(
      "ERR_INVALID_ARG_TYPE",
      "The \"path\" argument must be of type string or an instance of " +
      "Buffer or URL",
    );
  };

  const pathToFileURL = (path, options) => {
    if (options !== undefined && (options === null || typeof options !== "object")) {
      throw urlError(
        "ERR_INVALID_ARG_TYPE",
        "The \"options\" argument must be of type object",
      );
    }
    if (path instanceof URL) {
      if (path.protocol !== "file:") {
        throw urlError(
          "ERR_INVALID_URL_SCHEME",
          "The URL must be of scheme file",
        );
      }
      return new URL(path.href);
    }
    const input = toPathInput(path);
    if (input.includes("\0")) {
      throw urlError(
        "ERR_INVALID_ARG_VALUE",
        "The argument \"path\" must not contain null bytes",
      );
    }
    // Node keeps a trailing slash for directories; `posixResolve` above
    // strips it while normalizing, so re-add it from the input. Without
    // it a directory URL loses its marker and no longer round-trips.
    const absolute = posixResolve("/", input);
    const trail = absolute !== "/" && input.endsWith("/") ? "/" : "";
    const encoded = absolute.split("/").map((s) => encodeURIComponent(s));
    return new URL("file://" + encoded.join("/") + trail);
  };

  const fileURLToPath = (url, options) => {
    if (options !== undefined && (options === null || typeof options !== "object")) {
      throw urlError(
        "ERR_INVALID_ARG_TYPE",
        "The \"options\" argument must be of type object",
      );
    }
    let parsed;
    if (typeof url === "string") {
      try {
        parsed = new URL(url);
      } catch {
        throw urlError("ERR_INVALID_URL", "Invalid URL");
      }
    } else if (url instanceof URL) {
      parsed = url;
    } else {
      throw urlError(
        "ERR_INVALID_ARG_TYPE",
        "The \"url\" argument must be of type string or an instance of URL",
      );
    }
    if (parsed.protocol !== "file:") {
      throw urlError(
        "ERR_INVALID_URL_SCHEME",
        "The URL must be of scheme file",
      );
    }
    if (parsed.host !== "" && parsed.host !== "localhost") {
      throw urlError(
        "ERR_INVALID_FILE_URL_HOST",
        "File URL host must be \"localhost\" or empty",
      );
    }
    // An encoded slash must not silently become a separator. An encoded
    // backslash stays: on POSIX it is a valid filename character, and Node
    // rejects only `%2f` here.
    const pathname = parsed.pathname;
    if (pathname.toLowerCase().includes("%2f")) {
      throw urlError(
        "ERR_INVALID_FILE_URL_PATH",
        "File URL path must not include encoded slash characters",
      );
    }
    try {
      return decodeURIComponent(pathname);
    } catch {
      throw urlError("ERR_INVALID_FILE_URL_PATH", "Invalid file URL path");
    }
  };

  const { URL, URLSearchParams } = globalThis;
  __celld.__urlModule = { URL, URLSearchParams, pathToFileURL, fileURLToPath };
})();
