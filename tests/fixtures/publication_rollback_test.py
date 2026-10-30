"""Actual guard functions with isolated files and mocked external boundaries."""
import contextlib, importlib.util, io, json, os, pathlib, subprocess, sys, tempfile, types, unittest, uuid
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('rollback_guard',ROOT/'scripts/agentos-release-action.py')
g=importlib.util.module_from_spec(spec);spec.loader.exec_module(g)

class RollbackTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  self.root=pathlib.Path(self.temp.name);self.state=self.root/'state';self.state.mkdir()
  self.before=self.root/'releases'/('managed-'+str(uuid.uuid4()));self.before.mkdir(parents=True)
  self.after=self.before.with_name('managed-'+str(uuid.uuid4()));self.after.mkdir()
  (self.before/'file').write_text('before');(self.after/'file').write_text('after')
  (self.before/'node_modules/tsx/dist').mkdir(parents=True)
  (self.before/'node_modules/tsx/dist/loader.mjs').write_text('fixture')
  (self.root/'current').symlink_to(self.before)
  self.id=str(uuid.uuid4());self.receipt=self.state/(self.id+'.json')
  self.record={'candidate':str(self.after),'base':str(self.before),'digest':g.digest(self.after),
    'executorSha256':'a'*64,'candidateOrigin':{'published':{'release':str(self.before),'releaseDigest':g.digest(self.before)}}}
  self.receipt.write_text(json.dumps(self.record))
  self.authority={'path':'registered fixture','sha256':'b'*64}
  self.stack=contextlib.ExitStack();self.addCleanup(self.stack.close)
  for key,value in [('ROOT',self.root),('STATE',self.state)]:self.stack.enter_context(patch.object(g,key,value))
 def publish(self):
  origin=types.SimpleNamespace(require_publish_origin=lambda _:None)
  with patch.object(g,'trusted_origin_module',return_value=origin),patch.object(g,'publication_executor',return_value=self.authority),\
       patch.object(sys,'argv',['guard','publish']),patch.object(sys,'stdin',io.StringIO(json.dumps({'occurrenceId':self.id}))),\
       contextlib.redirect_stdout(io.StringIO()):g.main()
 def test_failed_health_and_incompatible_schema_preserve_candidate_and_validation(self):
  before=self.receipt.read_bytes()
  with patch.object(g,'run',side_effect=RuntimeError('health')),patch.object(g,'require_rollback_readiness',side_effect=ValueError('MIGRATION_REQUIRED')):
   with self.assertRaisesRegex(ValueError,'ROLLBACK_BLOCKED_RECONCILE'):self.publish()
  self.assertEqual((self.root/'current').resolve(),self.after);self.assertEqual(self.receipt.read_bytes(),before)
  self.assertFalse((self.state/'pending-workers.json').exists())
  self.assertEqual(json.loads(self.receipt.with_name(self.id+'.rollback.json').read_bytes())['state'],'blocked')
  with patch.object(g,'run') as run:
   with self.assertRaisesRegex(ValueError,'EFFECT_UNCONFIRMED'):self.publish()
   run.assert_not_called()
 def test_readiness_precedes_rollback_and_never_runs_installer(self):
  order=[]
  def run(args):
   order.append(str((self.root/'current').resolve()))
   if len(order)==1:raise RuntimeError('health')
  def check(previous,record):
   self.assertEqual((self.root/'current').resolve(),self.after);order.append('readonly');return {'ok':True}
  with patch.object(g,'run',side_effect=run),patch.object(g,'require_rollback_readiness',side_effect=check):
   with self.assertRaisesRegex(RuntimeError,'health'):self.publish()
  self.assertEqual(order,[str(self.after),'readonly',str(self.before)])
  self.assertEqual((self.root/'current').resolve(),self.before)
  observation=self.receipt.with_name(self.id+'.rollback.json').read_bytes()
  with patch.object(g,'run') as run:
   with self.assertRaisesRegex(ValueError,'ROLLBACK_RECONCILE'):self.publish()
   run.assert_not_called()
  self.assertEqual(self.receipt.with_name(self.id+'.rollback.json').read_bytes(),observation)
 def test_observation_write_failure_never_changes_pointer_back(self):
  with patch.object(g,'run',side_effect=RuntimeError()),patch.object(g,'require_rollback_readiness',return_value={'ok':True}),\
       patch.object(g,'persist_rollback_observation',side_effect=OSError('disk')):
   with self.assertRaises(OSError):self.publish()
  self.assertEqual((self.root/'current').resolve(),self.after)
 def check(self,result):
  (self.root/'current').unlink();(self.root/'current').symlink_to(self.after)
  with patch.object(g,'protected_tool',side_effect=lambda path,**_:path),\
       patch.object(g.pwd,'getpwnam',return_value=types.SimpleNamespace(pw_uid=os.getuid())),\
       patch.object(g.subprocess,'run',return_value=result) as run:
   value=g.require_rollback_readiness(self.before,self.record)
   args,kw=run.call_args
   command=args[0]
   self.assertNotIn('--check',command)
   self.assertFalse(any('prepare-schema' in value for value in command[:-1]))
   self.assertEqual(command[-1],g.ROLLBACK_READINESS_JS)
   self.assertIn('default_transaction_read_only=on',kw['env']['PGOPTIONS'])
   self.assertNotIn('DATABASE_URL',kw['env']);self.assertEqual(kw['cwd'],self.before)
   return value
 def result(self,**changes):
  return types.SimpleNamespace(**({'returncode':0,'stderr':'','stdout':json.dumps({'version':1,'ok':True,'readOnly':True,
    'gates':['base','backlog','taskExecution','runCriteria','panel']})}|changes))
 def test_readiness_command_requires_five_exact_readonly_gates(self):self.assertTrue(self.check(self.result())['ok'])
 def test_partial_gate_exit_stderr_or_malformed_output_cannot_prove_readiness(self):
  for result in [self.result(returncode=1),self.result(stderr='unexpected'),self.result(stdout='{}'),self.result(stdout='not json')]:
   with self.subTest(result=result),self.assertRaises(ValueError):self.check(result)
 def test_changed_old_source_is_rejected_before_node_dispatch(self):
  (self.before/'file').write_text('changed')
  with patch.object(g.subprocess,'run') as run:
   with self.assertRaisesRegex(ValueError,'SOURCE_UNBOUND'):g.require_rollback_readiness(self.before,self.record)
   run.assert_not_called()
 def test_timeout_is_not_rollback_authority(self):
  with patch.object(g,'run',side_effect=RuntimeError()),\
       patch.object(g,'require_rollback_readiness',side_effect=subprocess.TimeoutExpired('node',40)):
   with self.assertRaisesRegex(ValueError,'ROLLBACK_BLOCKED_RECONCILE'):self.publish()
  self.assertEqual((self.root/'current').resolve(),self.after)
 def test_registered_identity_is_preserved_on_success(self):
  response=types.SimpleNamespace(status=200)
  @contextlib.contextmanager
  def opened(*_,**__):yield response
  with patch.object(g,'run'),patch.object(g.urllib.request,'urlopen',side_effect=opened),patch.object(g,'publish_source'):
   self.publish()
  record=json.loads(self.receipt.read_bytes())
  self.assertEqual(record['executorSha256'],'a'*64);self.assertEqual(record['publicationExecutor'],self.authority)

if __name__=='__main__':
 if sys.argv[1:]==['--readiness-js']:print(g.ROLLBACK_READINESS_JS)
 else:unittest.main()
