export type SourceInput = { id: string; name: string; digest: string; base64: string };
export function materializeSourceInputs(
  inputs?: SourceInput[],
  options?: Partial<{ files: number; fileBytes: number; extraBytes: number; totalBytes: number }>,
): {
  inputs: SourceInput[];
  instruction: string;
  telemetry: { materialized: number; skipped: Record<string, number>; bytes?: number; indexId?: string };
};
