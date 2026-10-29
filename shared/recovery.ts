/** Set by the planner when three revisions bring no newly validated work. */
export const PLAN_STALL_REASON = 'Três revisões sem avanço validado; é necessária uma hipótese ou informação nova.';

/** Provider availability is not evidence that an implementation failed. */
export function providerAvailabilityFailure(code?: string | null) {
  return /^(AUTH_REQUIRED|QUOTA_EXCEEDED|RATE_LIMIT|RATE_LIMITED)$/.test(code ?? '');
}

/** Human action must have an explicit question, or a current authentication error. */
export function humanRecoveryRequest(continuation: any, errorCode?: string | null) {
  const request = continuation?.humanRequest;
  return (continuation?.owner === 'user' &&
    ['authentication', 'information', 'authorization'].includes(request?.kind) &&
    typeof request.question === 'string' && request.question.trim().length > 0) ||
    (errorCode === 'AUTH_REQUIRED' && continuation?.state === 'needs_action');
}
