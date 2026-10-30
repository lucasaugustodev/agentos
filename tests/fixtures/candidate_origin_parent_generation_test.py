"""Regression: a reused parent workspace name is not the copied generation.

Run only in the explicitly isolated development validation environment. All
filesystem mutations below belong to CandidateOriginTests' temporary root.
"""
import hashlib
import importlib.util
import pathlib
import shutil
import unittest

FIXTURE = pathlib.Path(__file__).with_name('candidate_origin_test.py')
spec = importlib.util.spec_from_file_location('candidate_origin_generation_fixture', FIXTURE)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


class ParentGenerationTests(unittest.TestCase):
    def test_reprepared_parent_cannot_launder_old_child_as_current_publication(self):
        harness = fixture.CandidateOriginTests()
        harness.setUp()
        try:
            parent = harness.prepare('ocgenerationparent')
            harness.put(fixture.manager.workspace(parent) / 'server/main.ts', b'export const version = 2;\n')
            child = harness.prepare('ocgenerationchild', parent)
            original_child = fixture.manager.sources(fixture.manager.workspace(child))
            old_marker = (fixture.manager.workspace(parent) / '.agentos-snapshot.json').read_bytes()
            self.assertTrue(harness.capture(child)['publishEligible'])

            # A newer publication has a fix the old child never copied.
            harness.publish(harness.files | {'server/security.ts': b'export const preservedFix = true;\n'})
            self.assertFalse(harness.capture(child)['publishEligible'])
            self.assertNotIn('server/security.ts', original_child)

            # Simulate legitimate GC/recovery followed by canonical prepare of
            # the same parent ID. The child's own marker and baseline survive.
            parent_root = fixture.manager.workspace(parent).resolve(strict=True)
            self.assertTrue(parent_root.is_relative_to(harness.root.resolve(strict=True)))
            self.assertNotEqual(parent_root, harness.root)
            shutil.rmtree(parent_root)
            self.assertEqual(harness.prepare('ocgenerationparent'), parent)
            new_marker = (fixture.manager.workspace(parent) / '.agentos-snapshot.json').read_bytes()
            self.assertNotEqual(hashlib.sha256(old_marker).digest(), hashlib.sha256(new_marker).digest())
            self.assertEqual(fixture.manager.sources(fixture.manager.workspace(child)), original_child)

            proof = harness.capture(child)
            self.assertFalse(proof['publishEligible'],
                             'Reusing a parent name must not authorize a child that predates the current fix')
            with self.assertRaisesRegex(ValueError, 'REBASE_REQUIRED'):
                fixture.origin.require_publish_origin(harness.record(proof), harness.layout)
        finally:
            harness.tearDown()


if __name__ == '__main__':
    unittest.main()
