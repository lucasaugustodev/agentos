# Benign fake Vite recreates the previously writable build-local runner path.
from pathlib import Path
path = Path.cwd() / 'node_modules/vitest/vitest.mjs'
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text("raise RuntimeError('MUTABLE_BUILD_RUNNER_EXECUTED')\n")
print('TRUSTED_FAKE_VITE')
