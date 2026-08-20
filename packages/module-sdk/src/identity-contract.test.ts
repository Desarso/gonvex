import type { Account, Member, QueryContext, Tenant } from "./index.js";

// Compile-time contract coverage for the JSON identity shape emitted by the
// V8 bootstrap. This file intentionally has no runtime dependencies; the
// package's typecheck is the test runner for the declaration-only SDK.
declare const context: QueryContext;

const account: Account | null = context.auth.account;
const tenant: Tenant | null = context.tenant;
const member: Member | null = context.member;

void account;
void tenant;
void member;

if (context.auth.account) {
  const accountID: string = context.auth.account.id;
  void accountID;
}

if (context.member) {
  const memberID: string = context.member.id;
  const permissions = context.member.permissions;
  void memberID;
  void permissions;
}
