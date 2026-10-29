import base64,hashlib,importlib.util,io,json,pathlib,subprocess,sys,tempfile,unittest,uuid
ROOT=pathlib.Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('candidate_validation',ROOT/'scripts/agentos-candidate-validation.py')
broker=importlib.util.module_from_spec(spec);spec.loader.exec_module(broker)

class CandidateValidationTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.temp.name)
  self.manager=broker.load_workspace_manager();self.manager.ROOT=self.root/'workspaces';self.manager.DATA=self.root/'bridge'
  (self.manager.ROOT/'agentos/otest').mkdir(parents=True)
  (self.manager.ROOT/'agentos/otest/server').mkdir()
  (self.manager.ROOT/'agentos/otest/server/file.ts').write_text('exact candidate\n')
  self.directory=self.root/'receipt';self.directory.mkdir()
  source=self.manager.sources(self.manager.ROOT/'agentos/otest');digest=self.manager.source_hash(source)
  candidate={'baseDigest':digest,'candidateDigest':digest,'files':[{'path':k,'sha256':broker.sha(v)} for k,v in sorted(source.items())]}
  raw=json.dumps(candidate).encode()
  self.request={key:str(uuid.uuid4()) for key in ['actionId','managerRunId','goalId','taskId','runId','runFence','contextVersion']}
  self.request.update(expectedAttempt=1,workspace='agentos/otest',candidateDigest=digest,candidateArtifactDigest=broker.sha(raw),candidateSize=len(raw),candidateBase64=base64.b64encode(raw).decode(),contractDigest='a'*64)
  self.request.update(project='agentos',frameworkAuthorization={'kind':'registered_agentos_source','project':'agentos','goalId':self.request['goalId']})
  self.request['decisionProof']={'coordinationArtifactId':str(uuid.uuid4()),'coordinationArtifactDigest':'b'*64,'coordinationSize':100,'actionIndex':0,'actionDigest':'c'*64}
 def tearDown(self):self.temp.cleanup()
 def test_canonical_bytes_match_json_stringify_utf8_unicode_vector(self):
  value={'z':'🚀','reason':'Validar publicação e critérios com ação correta.'}
  expected='{"reason":"Validar publicação e critérios com ação correta.","z":"🚀"}'.encode('utf-8')
  self.assertEqual(broker.canonical(value),expected)
  self.assertEqual(broker.sha(broker.canonical(value)),hashlib.sha256(expected).hexdigest())
 def test_unicode_request_receipt_binds_exact_utf8_digest(self):
  self.request['reason']='Validar publicação e critérios com ação correta. 🚀'
  self.directory=self.root/'states'/self.request['actionId'];self.directory.mkdir(parents=True)
  expected=json.dumps(self.request,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode('utf-8')
  target=self.freeze();result=self.call('receipt')
  self.assertEqual(result['state'],'frozen');self.assertEqual(result['frozenWorkspace'],target)
  self.assertEqual(result['requestDigest'],hashlib.sha256(expected).hexdigest())
 def freeze(self,physical=lambda r,m:None):return broker.freeze(self.request,self.directory,self.manager,physical)
 def test_exact_frozen_copy_and_duplicate_idempotency(self):
  target=self.freeze();self.assertEqual(target,self.freeze())
  self.assertEqual(self.manager.source_hash(self.manager.sources(self.manager.workspace(target))),self.request['candidateDigest'])
  self.assertEqual((self.directory/'candidate.json').read_bytes(),base64.b64decode(self.request['candidateBase64']))
 def test_live_actor_has_no_clone(self):
  def live(r,m):raise ValueError('STOP_UNCONFIRMED')
  with self.assertRaisesRegex(ValueError,'STOP_UNCONFIRMED'):self.freeze(live)
  self.assertFalse(self.manager.workspace('agentos/ocv'+self.request['actionId'].replace('-','')).exists())
 def test_changed_candidate_refused(self):
  (self.manager.ROOT/'agentos/otest/server/file.ts').write_text('changed')
  with self.assertRaisesRegex(ValueError,'VALIDATION_CANDIDATE_CHANGED'):self.freeze()
 def test_changed_canonical_bytes_refused(self):
  self.request['candidateBase64']=base64.b64encode(b'{}').decode()
  with self.assertRaisesRegex(ValueError,'VALIDATION_ARTIFACT_CHANGED'):self.freeze()
 def test_symlink_and_cross_project_refused(self):
  self.request['workspace']='another/otest'
  with self.assertRaisesRegex(ValueError,'WORKSPACE_INVALID'):self.freeze()
  self.request['workspace']='agentos/otest';(self.manager.ROOT/'agentos/otest/bad').symlink_to('/etc/passwd')
  with self.assertRaises(OSError):self.freeze()
 def test_change_during_copy_does_not_publish_clone(self):
  probes=0
  def physical(r,m):
   nonlocal probes
   probes+=1
   if probes==2:raise ValueError('STOP_UNCONFIRMED')
  with self.assertRaisesRegex(ValueError,'STOP_UNCONFIRMED'):self.freeze(physical)
  self.assertFalse(self.manager.workspace('agentos/ocv'+self.request['actionId'].replace('-','')).exists())
 def test_actual_subprocess_failure_never_passes(self):
  result=broker.execute(self.request,self.directory,'agentos/ocvtest',{'command':sys.executable,'args':['-c','import sys; sys.exit(7)']},stop_probe=lambda r:{'unit':'fixture','mainPID':0})
  self.assertEqual(result['state'],'uncertain');self.assertFalse(result['passed']);self.assertEqual(result['exitCode'],7)
  self.assertEqual(json.loads((self.directory/'receipt.json').read_text())['exitCode'],7)
 def test_unconfirmed_validation_stop_stays_uncertain_and_cannot_rerun(self):
  self.directory=self.root/'states'/self.request['actionId'];self.directory.mkdir(parents=True)
  command='import sys; print("ValueError: VALIDATION_STOP_UNCONFIRMED",file=sys.stderr); sys.exit(1)'
  result=broker.execute(self.request,self.directory,'agentos/ocvtest',{'command':sys.executable,'args':['-c',command]},stop_probe=lambda r:{'unit':'fixture','mainPID':0,'loadState':'not-found'})
  self.assertEqual(result['state'],'uncertain');self.assertEqual(result['code'],'VALIDATION_STOP_UNCONFIRMED')
  self.assertFalse(result['passed'])
  self.assertEqual(self.call('execute'),result)
 def test_registered_check_failure_keeps_discriminant_and_redacts_url(self):
  output={'ok':True,'output':{'passed':False},'artifacts':[{'name':'validation.log','content':'ERROR test.ts\npostgresql://user:secret@db/test'}]}
  command='import json; print('+repr(json.dumps(output))+')'
  result=broker.execute(self.request,self.directory,'agentos/ocvtest',{'command':sys.executable,'args':['-c',command]},stop_probe=lambda r:{'unit':'fixture','mainPID':0})
  self.assertEqual(result['code'],'VALIDATION_CHECKS_FAILED');self.assertFalse(result['passed'])
  self.assertIn('test.ts',result['diagnostic']);self.assertNotIn('secret',result['diagnostic'])
 def test_guard_killed_after_effect_does_not_release_ci_ownership(self):
  self.directory=self.root/'states'/self.request['actionId'];self.directory.mkdir(parents=True)
  marker=self.root/'effect-count.txt'
  command='import os,pathlib,signal; p=pathlib.Path('+repr(str(marker))+'); p.write_text("one effect"); os.kill(os.getpid(),signal.SIGKILL)'
  result=broker.execute(self.request,self.directory,'agentos/ocvtest',{'command':sys.executable,'args':['-c',command]},stop_probe=lambda r:{'unit':'fixture','mainPID':0,'loadState':'not-found'})
  self.assertEqual(result['state'],'uncertain');self.assertEqual(result['code'],'VALIDATION_GUARD_INTERRUPTED')
  self.assertEqual(marker.read_text(),'one effect')
  self.assertEqual(self.call('execute'),result)
 def test_malformed_guard_output_does_not_prove_launcher_quiescence(self):
  command='print("{truncated")'
  result=broker.execute(self.request,self.directory,'agentos/ocvtest',{'command':sys.executable,'args':['-c',command]},stop_probe=lambda r:(_ for _ in ()).throw(ValueError('VALIDATION_STOP_UNCONFIRMED')))
  self.assertEqual(result['state'],'uncertain')
  second=self.root/'another-action';second.mkdir()
  request=dict(self.request,actionId=str(uuid.uuid4()))
  result=broker.execute(request,second,'agentos/ocvtest',{'command':sys.executable,'args':['-c',command]},stop_probe=lambda r:{'unit':'fixture','mainPID':0})
  self.assertEqual(result['state'],'uncertain');self.assertFalse(result['passed'])
  self.assertIn('validationStopProof',result)
 def test_real_systemd_stop_probe_refuses_wrong_remote_identity(self):
  state=self.manager.DATA/'worker-runs'/self.request['runId'];state.mkdir(parents=True)
  (state/'state.json').write_text(json.dumps({'runId':self.request['runId'],'workspace':'agentos/other','stopped':True}))
  with self.assertRaisesRegex(ValueError,'PHYSICAL_IDENTITY_CHANGED'):broker.stopped(self.request,self.manager)
 def test_production_profile_remains_validate_only(self):
  self.assertIn(broker.sha((ROOT/'scripts/agentos-release-action.py').read_bytes()),broker.provenance.REVIEWED_EXECUTORS)
  self.assertNotIn("'publish'",(ROOT/'scripts/agentos-candidate-validation.py').read_text())
 def call(self,mode):
  original_state,original_stdin,original_argv=broker.STATE,sys.stdin,sys.argv
  original_manager=broker.load_workspace_manager
  broker.STATE=self.root/'states';sys.stdin=io.StringIO(json.dumps(self.request));sys.argv=['broker',mode]
  broker.load_workspace_manager=lambda:self.manager
  captured=io.StringIO()
  try:
   with broker.contextlib.redirect_stdout(captured):broker.main()
   return json.loads(captured.getvalue())
  finally:broker.STATE,sys.stdin,sys.argv=original_state,original_stdin,original_argv;broker.load_workspace_manager=original_manager
 def test_lost_freeze_reply_recovers_exact_receipt_without_recloning(self):
  self.directory=self.root/'states'/self.request['actionId'];self.directory.mkdir(parents=True)
  target=self.freeze();result=self.call('receipt')
  self.assertEqual(result['state'],'frozen');self.assertEqual(result['frozenWorkspace'],target)
  self.assertEqual(result['requestDigest'],broker.sha(broker.canonical(self.request)))
 def test_command_crash_durable_marker_never_reexecutes(self):
  directory=self.root/'states'/self.request['actionId'];directory.mkdir(parents=True)
  marker={'state':'running','actionId':self.request['actionId'],'candidateDigest':self.request['candidateDigest'],'requestDigest':broker.sha(broker.canonical(self.request))}
  broker.atomic(directory/'receipt.json',marker)
  result=self.call('execute');self.assertEqual(result['state'],'uncertain');self.assertEqual(result['code'],'VALIDATION_OWNER_INTERRUPTED')
  self.assertFalse((directory/'result.json').exists())
 def test_explicit_framework_project_registry_allows_same_goal_namespace(self):
  project='harness-improvement-team'
  registry={'version':1,'enabled':True,'project':project,'role':'framework','sourceKind':'agentos_published_runtime','goalIds':[self.request['goalId']]}
  self.request.update(project=project,workspace=project+'/otest',frameworkAuthorization={'kind':'registered_framework_project','project':project,'goalId':self.request['goalId'],'registryId':'framework-project:'+project,'registry':registry,'registryDigest':broker.sha(broker.canonical(registry))})
  (self.manager.ROOT/project).mkdir();(self.manager.ROOT/'agentos/otest').rename(self.manager.ROOT/project/'otest')
  target=self.freeze();self.assertTrue(target.startswith('agentos/ocv'))
  self.assertEqual(self.manager.source_hash(self.manager.sources(self.manager.workspace(target))),self.request['candidateDigest'])
  self.request['frameworkAuthorization']['goalId']=str(uuid.uuid4())
  with self.assertRaisesRegex(ValueError,'PROJECT_UNREGISTERED'):self.freeze()
 def test_project_registry_does_not_allow_wildcards_traversal_or_wrong_source(self):
  self.request['project']='*'
  with self.assertRaisesRegex(ValueError,'PROJECT_UNREGISTERED'):self.freeze()
  self.request['project']='agentos';self.request['workspace']='agentos/../other'
  with self.assertRaisesRegex(ValueError,'WORKSPACE_INVALID'):self.freeze()

if __name__=='__main__':unittest.main()
