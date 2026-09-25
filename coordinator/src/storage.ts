import {
  CreateBucketCommand,
  GetObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { config } from "./config.js";

// Two clients: one talks to MinIO on the internal network, the other exists only to sign URLs
// with the host the browser can reach (S3_PUBLIC_ENDPOINT). Signing is local; it makes no request.
let internal: S3Client | null = null;
let publicSigner: S3Client | null = null;

function makeClient(endpoint: string) {
  return new S3Client({
    endpoint,
    region: "us-east-1",
    forcePathStyle: true,
    credentials: { accessKeyId: config.s3AccessKey, secretAccessKey: config.s3SecretKey },
  });
}

function s3() {
  return (internal ??= makeClient(config.s3Endpoint));
}

function signer() {
  return (publicSigner ??= makeClient(config.s3PublicEndpoint));
}

/** Keys we have already written or seen in this process, so repeat jobs skip the HEAD request. */
const knownKeys = new Set<string>();

export const imageKey = (sha256: string) => `images/${sha256}.jpg`;

export async function ensureBucket(retries = 30): Promise<void> {
  for (let i = 0; ; i++) {
    try {
      try {
        await s3().send(new HeadBucketCommand({ Bucket: config.s3Bucket }));
      } catch (err: any) {
        const status = err?.$metadata?.httpStatusCode;
        if (status !== 404 && err?.name !== "NotFound") throw err;
        await s3().send(new CreateBucketCommand({ Bucket: config.s3Bucket }));
        console.log(`[storage] created bucket ${config.s3Bucket}`);
      }
      return;
    } catch (err) {
      if (i >= retries) throw err;
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

async function exists(key: string): Promise<boolean> {
  try {
    await s3().send(new HeadObjectCommand({ Bucket: config.s3Bucket, Key: key }));
    return true;
  } catch (err: any) {
    if (err?.$metadata?.httpStatusCode === 404 || err?.name === "NotFound") return false;
    throw err;
  }
}

/** Idempotent put: keys are content-addressed, so an existing object already has these bytes. */
export async function putIfMissing(key: string, body: Buffer | (() => Promise<Buffer>), contentType = "image/jpeg") {
  if (knownKeys.has(key)) return false;
  if (await exists(key)) {
    knownKeys.add(key);
    return false;
  }
  const bytes = typeof body === "function" ? await body() : body;
  await s3().send(new PutObjectCommand({ Bucket: config.s3Bucket, Key: key, Body: bytes, ContentType: contentType }));
  knownKeys.add(key);
  return true;
}

// Presigned URLs are cached per key for 50 min (signed for 60), so repeated worker_update and
// gallery responses carry the same URL and the browser can reuse its cached image.
const SIGN_TTL_S = 3600;
const REUSE_MS = 50 * 60 * 1000;
const signed = new Map<string, { url: string; at: number }>();

export async function presign(key: string | null | undefined): Promise<string | null> {
  if (!key) return null;
  const now = Date.now();
  const hit = signed.get(key);
  if (hit && now - hit.at < REUSE_MS) return hit.url;
  const url = await getSignedUrl(signer(), new GetObjectCommand({ Bucket: config.s3Bucket, Key: key }), {
    expiresIn: SIGN_TTL_S,
  });
  if (signed.size > 50_000) signed.clear();
  signed.set(key, { url, at: now });
  return url;
}
