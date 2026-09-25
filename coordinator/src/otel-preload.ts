// Loaded with `node --import ./dist/otel-preload.js dist/index.js` (coordinator/Dockerfile).
//
// The coordinator is an ES module, so http/pg/ioredis can only be instrumented if the
// import-in-the-middle loader hook is registered before the app imports them; that is all this
// file does, and only when tracing is requested. Otherwise it returns at once and registers
// nothing: no loader hook, no SDK.

import { register } from "node:module";
import { tracingRequested } from "./otel-sdk.js";

if (tracingRequested()) {
  try {
    // Wrap only the modules an instrumentation asks for, not every module the app loads.
    const { createAddHookMessageChannel } = await import("import-in-the-middle");
    const { registerOptions, waitForAllMessagesAcknowledged } = createAddHookMessageChannel();
    register("import-in-the-middle/hook.mjs", import.meta.url, registerOptions);
    const { setupTracing } = await import("./otel-sdk.js");
    await setupTracing({ instrument: true });
    await waitForAllMessagesAcknowledged();
    console.log(
      `[otel] tracing on: OTLP ${process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT || process.env.OTEL_EXPORTER_OTLP_ENDPOINT}, ` +
        `sampler ${process.env.OTEL_TRACES_SAMPLER || "parentbased_always_on"}` +
        (process.env.OTEL_TRACES_SAMPLER_ARG ? ` (${process.env.OTEL_TRACES_SAMPLER_ARG})` : ""),
    );
  } catch (err) {
    // Observability must never stop the coordinator from starting.
    console.error(`[otel] tracing setup failed, continuing without it: ${(err as Error).message}`);
  }
}
