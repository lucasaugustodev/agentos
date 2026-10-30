import hashlib, importlib.util, pathlib, tempfile, unittest

spec=importlib.util.spec_from_file_location('source_output_manager',pathlib.Path(__file__).resolve().parents[2]/'bridge/workspace-manager.py')
manager=importlib.util.module_from_spec(spec);spec.loader.exec_module(manager)

class SourceOutputInventory(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.temp.name)
        manager.ROOT=self.root/'workspaces';manager.DATA=self.root/'data'
        self.target=manager.ROOT/'project'/'ocandidate';self.target.mkdir(parents=True)
        self.baseline=manager.DATA/'workspace-baselines'/manager.digest_key('project/ocandidate');self.baseline.mkdir(parents=True)
        self.put(self.baseline/'src/winner.ts',b'known-good\n');self.put(self.target/'src/winner.ts',b'known-good\n')
    def tearDown(self):self.temp.cleanup()
    def put(self,path,raw):path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
    def inspect(self,**extra):return manager.inspect({'workspace':'project/ocandidate',**extra})
    def test_legacy_shape_is_unchanged(self):
        self.assertNotIn('baseFiles',self.inspect())
    def test_runtime_reads_original_baseline_and_current_source_separately(self):
        self.put(self.target/'src/winner.ts',b'regression\n')
        value=self.inspect(includeBaseFiles=True)
        self.assertEqual(value['baseFiles'],[{'path':'src/winner.ts','sha256':hashlib.sha256(b'known-good\n').hexdigest()}])
        self.assertEqual(value['files'],[{'path':'src/winner.ts','sha256':hashlib.sha256(b'regression\n').hexdigest()}])
        self.assertNotEqual(value['baseDigest'],value['candidateDigest'])
    def test_evidence_only_source_never_becomes_a_delivered_file(self):
        self.put(self.target/'evidence/integrated/src/feature.ts',b'implemented\n')
        value=self.inspect(includeBaseFiles=True)
        self.assertEqual(value['baseFiles'],value['files']);self.assertEqual(value['baseDigest'],value['candidateDigest'])
        self.assertEqual(value['patch'],'')
    def test_explicit_source_promotion_enters_the_runtime_inventory(self):
        self.put(self.target/'src/feature.ts',b'implemented\n')
        value=self.inspect(includeBaseFiles=True)
        self.assertIn({'path':'src/feature.ts','sha256':hashlib.sha256(b'implemented\n').hexdigest()},value['files'])
        self.assertNotIn('src/feature.ts',[f['path'] for f in value['baseFiles']])
        self.assertIn('src/feature.ts',value['patch'])
    def test_source_symlink_is_refused(self):
        (self.target/'src/feature.ts').symlink_to(self.baseline/'src/winner.ts')
        with self.assertRaises(OSError):self.inspect(includeBaseFiles=True)

if __name__=='__main__':unittest.main()
