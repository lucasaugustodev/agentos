export type ModelPolicy = ({ version: 1; mode: 'glm-only'; model: 'glm-5.3-flash' } | { version: 2; mode: 'exclusive-model'; model: 'deepseek-v4-pro-0813' }) & { accountId: string; bridgeId: string; requestLimit: 6; sessionLimit: 12 };
export function readModelPolicy(): ModelPolicy | null;
export function validateModelPolicy(value: unknown): ModelPolicy;
export function modelRouteAllowed(accountId: string, model: string, policy?: ModelPolicy | null): boolean;
export function assertModelRoute(accountId: string, model: string, policy?: ModelPolicy | null): void;
export function usesSharedRequestGate(accountId: string, policy?: ModelPolicy | null): boolean;
