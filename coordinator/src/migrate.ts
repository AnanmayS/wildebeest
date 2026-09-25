// `npm run migrate`: apply pending migrations and exit (the server also does this at startup).
import { closePool, migrate } from "./db.js";

const applied = await migrate();
console.log(applied.length ? `applied: ${applied.join(", ")}` : "no pending migrations");
await closePool();
