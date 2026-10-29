import { describe, expect, it } from 'vitest';
import { objectiveSchema } from '../server/objective-contracts.js';

const base = {
  project: 'demo',
  mode: 'execute' as const,
  workspaces: ['demo/app'],
  idempotencyKey: 'demo-objective-1',
};

describe('objective title', () => {
  it('accepts an explicit title', () => {
    const parsed = objectiveSchema.parse({ ...base, message: 'faça algo', title: 'Loja nova' });
    expect(parsed.title).toBe('Loja nova');
  });

  it('stays optional, so existing callers are unaffected', () => {
    expect(objectiveSchema.parse({ ...base, message: 'faça algo' }).title).toBeUndefined();
  });

  it('rejects a title that is itself a pasted prompt', () => {
    const long = 'x'.repeat(121);
    expect(() => objectiveSchema.parse({ ...base, message: 'faça algo', title: long })).toThrow();
  });
});
