import { z } from "zod";
import { defineAction, parseDefinition } from "../src/index.js";

export const purchaseDefinition = parseDefinition({
  schemaVersion: 1, id: "purchase", version: 1, name: "Purchase approval",
  inputFields: [{ key: "amount", type: "number", required: true }],
  nodes: [
    { id: "approve", type: "approval", assignees: { type: "resolver", name: "finance-managers" }, mode: "all", fields: [{ key: "comment", type: "string" }] },
    { id: "book", type: "action", action: "purchase.book", input: { amount: { path: ["amount"] } }, resultKey: "bookingId" },
  ],
});

export const bookPurchase = defineAction({
  input: z.strictObject({ amount: z.number().positive() }), output: z.strictObject({ bookingId: z.string() }),
  async execute(_context, input) { return { bookingId: `booking-${input.amount}` }; },
});
