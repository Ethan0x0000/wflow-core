import { ValueError } from "@temporalio/common";
import { describe, expect, it } from "vitest";
import { createAesGcmPayloadCodec, ENCRYPTED_ENCODING, METADATA_ENCRYPTION_KEY_ID, METADATA_PLAIN_ENCODING } from "../src/codec.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const key = (fill: number) => new Uint8Array(32).fill(fill);
const payload = (data: string, encoding = "json/plain") => ({ metadata: { encoding: encoder.encode(encoding) }, data: encoder.encode(data) });
const text = (value: Uint8Array | undefined) => value === undefined ? undefined : decoder.decode(value);

describe("AES-256-GCM payload codec", () => {
  it("round-trips a JSON payload with a single key and restores its encoding", async () => {
    const codec = createAesGcmPayloadCodec({ key: key(1) });
    const [encoded] = await codec.encode([payload('{"secret":"value"}')]);
    expect(text(encoded!.metadata!.encoding)).toBe(ENCRYPTED_ENCODING);
    expect(text(encoded!.metadata![METADATA_ENCRYPTION_KEY_ID])).toBe("default");
    expect(text(encoded!.metadata![METADATA_PLAIN_ENCODING])).toBe("json/plain");
    expect(decoder.decode(encoded!.data!)).not.toContain("secret");
    const [decoded] = await codec.decode([encoded!]);
    expect(decoder.decode(decoded!.data!)).toBe('{"secret":"value"}');
    expect(text(decoded!.metadata!.encoding)).toBe("json/plain");
    expect(decoded!.metadata![METADATA_ENCRYPTION_KEY_ID]).toBeUndefined();
    expect(decoded!.metadata![METADATA_PLAIN_ENCODING]).toBeUndefined();
  });

  it("rotates keys: the active key encrypts, older keys still decrypt", async () => {
    const old = createAesGcmPayloadCodec({ keys: { "2024": key(1) }, activeKeyId: "2024" });
    const [legacy] = await old.encode([payload("legacy")]);
    const rotating = createAesGcmPayloadCodec({ keys: { "2024": key(1), "2025": key(2) }, activeKeyId: "2025" });
    const [fresh] = await rotating.encode([payload("fresh")]);
    expect(text(fresh!.metadata![METADATA_ENCRYPTION_KEY_ID])).toBe("2025");
    expect(decoder.decode((await rotating.decode([legacy!]))[0]!.data!)).toBe("legacy");
    expect(decoder.decode((await rotating.decode([fresh!]))[0]!.data!)).toBe("fresh");
  });

  it("rejects a tampered payload with a clear decrypt error", async () => {
    const codec = createAesGcmPayloadCodec({ key: key(1) });
    const [encoded] = await codec.encode([payload("secret")]);
    const tampered = { ...encoded!, data: Uint8Array.from(encoded!.data!, (byte, index) => index === encoded!.data!.length - 1 ? byte ^ 0xff : byte) };
    await expect(codec.decode([tampered])).rejects.toThrow(ValueError);
    await expect(codec.decode([tampered])).rejects.toThrow(/failed to decrypt/);
  });

  it("rejects unknown key ids and malformed encrypted payloads", async () => {
    const [encoded] = await createAesGcmPayloadCodec({ keys: { a: key(1) }, activeKeyId: "a" }).encode([payload("secret")]);
    const foreign = createAesGcmPayloadCodec({ key: key(2) });
    await expect(foreign.decode([encoded!])).rejects.toThrow(/unknown encryption key id "a"/);
    await expect(foreign.decode([{ metadata: { encoding: encoder.encode(ENCRYPTED_ENCODING) }, data: encoder.encode("x") }])).rejects.toThrow(/missing encryption-key-id/);
    await expect(foreign.decode([{ metadata: { encoding: encoder.encode(ENCRYPTED_ENCODING), [METADATA_ENCRYPTION_KEY_ID]: encoder.encode("default") }, data: new Uint8Array(4) }])).rejects.toThrow(/malformed/);
  });

  it("encrypts non-binary/plain payloads consistently and passes already-encrypted or data-less payloads through", async () => {
    const codec = createAesGcmPayloadCodec({ key: key(1) });
    const already = { metadata: { encoding: encoder.encode(ENCRYPTED_ENCODING), [METADATA_ENCRYPTION_KEY_ID]: encoder.encode("other") }, data: encoder.encode("ciphertext") };
    expect((await codec.encode([already]))[0]).toBe(already);
    const empty = { metadata: { encoding: encoder.encode("binary/plain") } };
    expect((await codec.encode([empty]))[0]).toBe(empty);
    const [plain] = await codec.encode([payload("bytes", "binary/plain")]);
    expect(text(plain!.metadata!.encoding)).toBe(ENCRYPTED_ENCODING);
    expect(text((await codec.decode([plain!]))[0]!.metadata!.encoding)).toBe("binary/plain");
    const [bare] = await codec.encode([{ data: encoder.encode("bare") }]);
    expect(text((await codec.decode([bare!]))[0]!.metadata!.encoding)).toBe("binary/plain");
  });

  it("accepts a base64 key and validates key material at construction", async () => {
    expect(() => createAesGcmPayloadCodec({ key: new Uint8Array(16) })).toThrow(/32 bytes/);
    expect(() => createAesGcmPayloadCodec({ keys: { a: key(1) }, activeKeyId: "missing" })).toThrow(/active key/);
    const encoded64 = Buffer.from(key(1)).toString("base64");
    const codec = createAesGcmPayloadCodec({ key: encoded64 });
    const [encoded] = await codec.encode([payload("base64 key")]);
    expect(decoder.decode((await codec.decode([encoded!]))[0]!.data!)).toBe("base64 key");
    const bytes = createAesGcmPayloadCodec({ key: key(1) });
    expect(decoder.decode((await bytes.decode([encoded!]))[0]!.data!)).toBe("base64 key");
  });
});
