import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { ValueError, type Payload, type PayloadCodec } from "@temporalio/common";

// AES-256-GCM payload codec for PII / sensitive business data. This module lives outside the
// workflow sandbox (codecs run in the Client/Worker runtime) and is intentionally only reachable
// through the `wflow-core/codec` subpath so `node:crypto` never enters a workflow bundle.
export const ENCRYPTED_ENCODING = "binary/encrypted";
export const METADATA_ENCODING_KEY = "encoding";
export const METADATA_ENCRYPTION_KEY_ID = "encryption-key-id";
// The payload's pre-encryption encoding (json/plain, binary/plain, ...) travels next to the
// ciphertext so decode can restore it; an absent value falls back to binary/plain.
export const METADATA_PLAIN_ENCODING = "plain-encoding";
export const DEFAULT_KEY_ID = "default";
const KEY_BYTES = 32;
const NONCE_BYTES = 12;
const TAG_BYTES = 16;

export type AesGcmKey = Uint8Array | string;
export type AesGcmPayloadCodecOptions =
  | { key: AesGcmKey }
  | { keys: Readonly<Record<string, AesGcmKey>>; activeKeyId: string };

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function keyBytes(value: AesGcmKey, keyId: string): Uint8Array {
  const bytes = typeof value === "string" ? new Uint8Array(Buffer.from(value, "base64")) : value;
  if (bytes.length !== KEY_BYTES) throw new ValueError(`AES-GCM payload codec: key "${keyId}" must be 32 bytes, received ${bytes.length}`);
  return bytes;
}

function metadataText(metadata: Payload["metadata"], key: string): string | undefined {
  const value = metadata?.[key];
  return value === undefined ? undefined : decoder.decode(value);
}

/**
 * Creates an AES-256-GCM {@link PayloadCodec}. `{ key }` registers a single key under the
 * `default` id; `{ keys, activeKeyId }` supports rotation (the active key encrypts, every map key
 * can decrypt older payloads). Keys are 32 raw bytes or base64 strings.
 *
 * Wire format per payload: `nonce(12) || ciphertext || tag(16)` in `data`; metadata carries
 * `encoding: "binary/encrypted"`, `encryption-key-id` and `plain-encoding` (the original
 * encoding). Payloads that are already `binary/encrypted` or carry no data pass through untouched.
 * Malformed, tampered or unknown-key payloads throw a {@link ValueError} with a clear message.
 */
export function createAesGcmPayloadCodec(options: AesGcmPayloadCodecOptions): PayloadCodec {
  const registration = "keys" in options
    ? options
    : { keys: { [DEFAULT_KEY_ID]: options.key }, activeKeyId: DEFAULT_KEY_ID };
  const keyIds = Object.keys(registration.keys);
  if (!keyIds.length) throw new ValueError("AES-GCM payload codec: at least one key is required");
  if (!keyIds.includes(registration.activeKeyId)) throw new ValueError(`AES-GCM payload codec: active key "${registration.activeKeyId}" is not registered`);
  const keys = new Map(keyIds.map((keyId) => [keyId, keyBytes(registration.keys[keyId]!, keyId)]));
  const activeKeyId = registration.activeKeyId;
  const activeKey = keys.get(activeKeyId)!;

  return {
    async encode(payloads: Payload[]): Promise<Payload[]> {
      return payloads.map((payload) => {
        const encoding = metadataText(payload.metadata, METADATA_ENCODING_KEY);
        if (encoding === ENCRYPTED_ENCODING || payload.data === undefined || payload.data === null) return payload;
        const nonce = randomBytes(NONCE_BYTES);
        const cipher = createCipheriv("aes-256-gcm", activeKey, nonce);
        const ciphertext = Buffer.concat([cipher.update(payload.data), cipher.final()]);
        const metadata: Record<string, Uint8Array> = { ...(payload.metadata ?? {}) };
        if (encoding !== undefined) metadata[METADATA_PLAIN_ENCODING] = encoder.encode(encoding);
        metadata[METADATA_ENCODING_KEY] = encoder.encode(ENCRYPTED_ENCODING);
        metadata[METADATA_ENCRYPTION_KEY_ID] = encoder.encode(activeKeyId);
        return { metadata, data: new Uint8Array(Buffer.concat([nonce, ciphertext, cipher.getAuthTag()])) };
      });
    },

    async decode(payloads: Payload[]): Promise<Payload[]> {
      return payloads.map((payload) => {
        if (metadataText(payload.metadata, METADATA_ENCODING_KEY) !== ENCRYPTED_ENCODING) return payload;
        const keyId = metadataText(payload.metadata, METADATA_ENCRYPTION_KEY_ID);
        if (!keyId) throw new ValueError("AES-GCM payload codec: encrypted payload is missing encryption-key-id");
        const key = keys.get(keyId);
        if (!key) throw new ValueError(`AES-GCM payload codec: unknown encryption key id "${keyId}"`);
        const data = payload.data;
        if (!data || data.length < NONCE_BYTES + TAG_BYTES) throw new ValueError("AES-GCM payload codec: encrypted payload is malformed");
        const nonce = data.subarray(0, NONCE_BYTES);
        const tag = data.subarray(data.length - TAG_BYTES);
        const ciphertext = data.subarray(NONCE_BYTES, data.length - TAG_BYTES);
        let plaintext: Uint8Array;
        try {
          const decipher = createDecipheriv("aes-256-gcm", key, nonce);
          decipher.setAuthTag(tag);
          plaintext = new Uint8Array(Buffer.concat([decipher.update(ciphertext), decipher.final()]));
        } catch (error) {
          throw new ValueError("AES-GCM payload codec: failed to decrypt payload", error);
        }
        const metadata: Record<string, Uint8Array> = { ...(payload.metadata ?? {}) };
        delete metadata[METADATA_ENCRYPTION_KEY_ID];
        delete metadata[METADATA_PLAIN_ENCODING];
        metadata[METADATA_ENCODING_KEY] = encoder.encode(metadataText(payload.metadata, METADATA_PLAIN_ENCODING) ?? "binary/plain");
        return { metadata, data: plaintext };
      });
    },
  };
}
