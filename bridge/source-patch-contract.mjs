// workspace-manager.py builds the canonical source patch as text: any file that
// differs from the snapshot base is decoded as UTF-8 on both sides. A binary
// that is added, modified OR deleted rejects the whole delivery, while an
// untouched one passes. The agent only learns that after the attempt ends, so
// state the rule and ship the exact check (mounted read-only in the sandbox).
export const SOURCE_BINARY_CHECK = 'node /opt/agentos-bridge/source-binary-check.mjs';

export const sourcePatchInstruction =
  '\n\nSource delivery is a text patch against the snapshot base. Outside data/, evidence/ and assets/, never add, modify or delete a non-UTF-8 file (image, database, archive, upload or test output): any such change makes the entire delivery fail with PATCH_BINARY_SOURCE, however correct the code is. ' +
  'Binary files already present in the base must stay byte-for-byte untouched, even if they look like junk; do not clean them up. ' +
  'Point runtime-written storage at data/ (for example data/uploads/), keep new test fixtures and outputs under data/, and use SVG or CSS for new static images. ' +
  `Before your final answer, run ${SOURCE_BINARY_CHECK} and do what it prints for every line (RESTORE puts the base file back, MOVE relocates a new binary) until it prints OK.`;
