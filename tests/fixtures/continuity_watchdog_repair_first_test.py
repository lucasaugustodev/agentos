"""Server-only adversarial tests for short, authorized repair planning batches."""
import copy
import difflib
import importlib.util
import json
import pathlib
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('base_repair_first_fixture',
    pathlib.Path(__file__).with_name('continuity_watchdog_test.py'))
B = importlib.util.module_from_spec(spec)
spec.loader.exec_module(B)
W = B.W


class RepairFirstTests(unittest.TestCase):
    problem = 'muse_decode_layer'

    def setUp(self):
        self.f = B.WatchTests(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        initial = self.f.repairable_operation()
        self.statuses = [initial]
        template = self.f.w.state['experiments'][0]
        original = self.f.client.occurrences[template['occurrenceId']]
        for number in [1, 2]:
            inp = copy.deepcopy(original['occurrence']['input'])
            inp['experiment']['hypothesis'] = 'Distinct existing candidate requiring authenticated evidence repair ' + str(number)
            row = self.f.client.make_occurrence(template['cycleId'],
                {'requestKey': 'prior-producer-' + str(number), 'input': inp})
            self.f.w.state['experiments'].append({**template, 'occurrenceId': row['id'],
                                                'experimentId': 'prior-producer-' + str(number)})
            status = self.f.complete_repair_source(row['id'])
            detail = self.f.client.occurrences[row['id']]
            base = detail['occurrence']['input']['experiment']['sourceArtifact']
            base_raw = self.f.client.artifacts[base['id']]
            candidate = ('# distinct preserved candidate ' + str(number) + '\n').encode()
            patch = ''.join(difflib.unified_diff(base_raw.decode().splitlines(True), candidate.decode().splitlines(True),
                fromfile='base-candidate.py', tofile='candidate.py')).encode()
            for artifact in detail['artifacts']:
                name = artifact['name']; raw = None
                if name == 'candidate.py': raw = candidate
                elif name == 'implementation.patch': raw = patch
                elif name in ['verification.json', 'qa-verification.json']:
                    report = json.loads(self.f.client.artifacts[artifact['id']])
                    report['candidateSHA256'] = W.digest(candidate); raw = W.canonical(report)
                if raw is not None:
                    self.f.client.artifacts[artifact['id']] = raw
                    artifact.update(size=len(raw), digest=W.digest(raw))
            status['candidateSHA256'] = W.digest(candidate)
            status['qaSHA256'] = next(a['digest'] for a in detail['artifacts'] if a['name'] == 'qa-verification.json')
            self.statuses.append(status)
        self.extra_seed = self.f.add_seed(self.problem, b'# unrelated existing available source\n')
        self.f.enable_mixed_plan_contract()
        self.f.config.update(plannerMode='board-lanes-v1', targetNativeWorkers=8,
                             readyLowWatermark=2, repairFirstPlanning=True)
        self.bind_control()
        self.f.install_diagnostic_fixture(self.statuses)
        self.f.w.save(); self.f.reload()

    def bind_control(self):
        self.f.config['controlDefinitionSHA256'] = W.digest(W.supervisor_definition(self.f.config))
        self.f.client.cycles[self.f.config['controlCycleId']]['definition'] = W.supervisor_definition(self.f.config)
        W.validate_config(self.f.config)

    @property
    def w(self): return self.f.w

    @property
    def client(self): return self.f.client

    def open_review(self):
        self.w.tick()
        reviews = [r for r in self.w.state['supervisions'] if r.get('planningProblem') == self.problem and r['state'] == 'dispatched']
        self.assertEqual(len(reviews), 1)
        return reviews[0]

    def plan(self, review):
        inp = self.w.supervision_input(review)
        selected = inp['planningScope']['repairSourceArtifactIds']
        return {'version': 2, 'evidenceFingerprint': review['fingerprint'], 'decision': 'actions',
            'reason': 'Dispatch the small authenticated repair batch before researching new optimization variants.',
            'experiments': [{'id': 'repair-existing-' + str(i), 'kind': 'repair_evidence', 'problem': self.problem,
                'sourceArtifactId': source, 'hypothesis': 'Repair authenticated evidence for the immutable selected candidate ' + str(i),
                'instructions': 'Preserve the exact candidate and canonical patch; replay the bound checker and produce complete fresh independent QA.'}
                for i, source in enumerate(selected)],
            'capacityReview': [{'problem': p, 'availableSlots': n, 'plannedActions': len(selected) if p == self.problem else 0,
                'reason': 'This deliberately short repair batch leaves remaining work for the next automatic planning pass.'}
                for p, n in inp['boardAvailableCandidateSlots'].items()]}

    def original_view(self, review):
        return {'fingerprint': review['fingerprint'], **self.w.supervision_input(review)}

    def finish(self, review, plan=None):
        self.client.complete(review['occurrenceId'], {'continuation-plan.json': self.plan(review) if plan is None else plan})

    def test_short_view_selects_two_authorized_targets_with_original_capacity_and_source_proof(self):
        full = self.w.observe(); lane = self.w.lane_observation(self.w.admission_with_ready(full), self.problem)
        before = copy.deepcopy(full); review = self.open_review(); inp = self.w.supervision_input(review)
        scope = inp['planningScope']; selected = set(scope['repairSourceArtifactIds'])
        self.assertEqual(scope['batchPurpose'], 'repair_backlog'); self.assertEqual(len(selected), 2)
        all_repairs = {r['candidateArtifactId']: r for r in full['autoCheckDiagnostics']['repairableEvidence']}
        self.assertTrue(selected.issubset(all_repairs))
        self.assertEqual(len(all_repairs), 3)
        self.assertEqual(inp['boardAvailableCandidateSlots'], lane['boardAvailableCandidateSlots'])
        self.assertGreater(inp['boardAvailableCandidateSlots'][self.problem], 2)
        self.assertEqual(inp['pipelineUsed'], lane['pipelineUsed'])
        self.assertEqual(inp['pipelineOwners'], lane['pipelineOwners'])
        allowed = selected | {all_repairs[s]['originalBaseArtifact']['id'] for s in selected}
        self.assertTrue({a['id'] for a in inp['catalog']}.issubset(allowed))
        capsule_ids = {i for c in inp['sourceCapsules']['sources'] for i in c['artifactIds']}
        self.assertTrue(selected.issubset(capsule_ids)); self.assertTrue(capsule_ids.issubset(allowed))
        self.assertEqual({r['candidateArtifactId'] for r in inp['autoCheckDiagnostics']['repairableEvidence']}, selected)
        proof = W.action_evidence(lane, self.f.config)
        self.assertEqual(inp['actionEvidence']['authoritySHA256'], proof['authoritySHA256'])
        for source in selected: self.assertEqual(inp['actionEvidence']['sources'][source], proof['sources'][source])
        self.assertEqual(full, before)
        self.assertEqual(W.validate_plan(self.plan(review), self.original_view(review), self.f.config), self.plan(review))

    def test_default_off_keeps_unrestricted_plan_contract_and_definition(self):
        config = copy.deepcopy(self.f.config); config.pop('repairFirstPlanning')
        expected = W.supervisor_definition(config)
        config['repairFirstPlanning'] = False
        self.assertEqual(W.supervisor_definition(config), expected)
        self.f.config.pop('repairFirstPlanning'); self.bind_control()
        self.f.install_diagnostic_fixture(self.statuses)
        review = self.open_review(); inp = self.w.supervision_input(review)
        self.assertNotIn('batchPurpose', inp['planningScope'])
        self.assertNotIn('repairSourceArtifactIds', inp['planningScope'])
        self.assertEqual(len(inp['autoCheckDiagnostics']['repairableEvidence']), 3)
        self.assertGreater(len(inp['catalog']), 3)

    def test_optin_requires_boolean_and_scoped_v2_contract(self):
        for field, value in [('repairFirstPlanning', 1), ('repairFirstPlanning', 'true'),
                             ('planVersion', 1), ('plannerMode', None)]:
            config = copy.deepcopy(self.f.config)
            if value is None: config.pop(field)
            else: config[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError): W.validate_config(config)

    def test_repair_batch_rejects_experiment_unselected_target_and_third_action(self):
        review = self.open_review(); inp = self.w.supervision_input(review)
        plan = self.plan(review)
        for change in ['kind', 'unselected', 'third']:
            bad = copy.deepcopy(plan)
            if change == 'kind': bad['experiments'][0]['kind'] = 'experiment'
            elif change == 'unselected':
                bad['experiments'][0]['sourceArtifactId'] = next(r['candidateArtifactId']
                    for r in self.w.observe()['autoCheckDiagnostics']['repairableEvidence']
                    if r['candidateArtifactId'] not in inp['planningScope']['repairSourceArtifactIds'])
            else:
                bad['experiments'].append({**bad['experiments'][0], 'id': 'third-unrequested-repair'})
                bad['capacityReview'][0]['plannedActions'] += 1
            with self.subTest(change=change), self.assertRaises(ValueError):
                W.validate_plan(bad, self.original_view(review), self.f.config)

    def test_two_repairs_dispatch_through_existing_outbox_and_independent_qa(self):
        review = self.open_review(); self.finish(review)
        self.w.tick()
        records = [r for r in self.w.state['experiments'] if r['supervisorOccurrenceId'] == review['occurrenceId']]
        self.assertEqual(len(records), 2)
        for record in records:
            detail = self.client.occurrences[record['occurrenceId']]
            self.assertIn('repairOf', record)
            self.assertEqual([s['key'] for s in detail['occurrence']['definition']['steps']], ['produce', 'independent-qa'])
            target = detail['occurrence']['input']['experiment']['repairTarget']
            self.assertEqual(target['candidateSHA256'], record['repairCandidateSHA256'])
            self.assertEqual(target['canonicalPatch']['sha256'], record['repairPatchSHA256'])
        before = len(self.client.posts); self.w.tick()
        self.assertEqual(len([r for r in self.w.state['experiments'] if r['supervisorOccurrenceId'] == review['occurrenceId']]), 2)
        self.assertTrue(all(a['state'] == 'owned' for a in review['outbox'].values()))
        self.assertLessEqual(len(self.client.posts)-before, 1)  # At most a new planner, never duplicate workers.

    def test_quota_or_uncertain_ownership_cannot_be_reclassified_as_fast_repair(self):
        for state, code in [('quota_wait', 'CHECK_QUOTA'), ('uncertain_owned', 'POST_UNCERTAIN_RECONCILE_ONLY')]:
            rows = copy.deepcopy(self.statuses); rows[0].update(state=state, code=code)
            self.f.install_diagnostic_fixture(rows)
            before = len(self.client.posts); result = self.w.tick()
            self.assertEqual(len(self.client.posts), before)
            self.assertEqual(result['plannerPending'], 0)
            self.assertEqual(result['waits'][self.problem], 'no_admissible_capacity_or_source')

    def test_matching_cross_origin_qa_still_invalidates_only_selected_action(self):
        review = self.open_review(); plan = self.plan(review)
        review['approvedPlan'] = plan
        review['outbox'] = {a['id']: {'state': 'ready'} for a in plan['experiments']}
        selected = plan['experiments'][0]['sourceArtifactId']
        source = next(a for a in self.w.supervision_input(review)['catalog'] if a['id'] == selected)
        self.client.complete(self.extra_seed, {'candidate.py': self.client.artifacts[selected],
            'qa-verification.json': {'passed': False, 'candidateSHA256': source['digest'],
                'approvedForOfficialReview': False, 'gpuValidated': False,
                'blockingFindings': [{'id': 'new-exact-source-failure', 'detail': 'New independent exact-source finding.'}]}})
        fresh = self.w.observe()
        with self.assertRaisesRegex(ValueError, 'PLAN_OBSERVATION_CHANGED'):
            self.w.lane_action_current(review, plan['experiments'][0], fresh)
        self.w.lane_action_current(review, plan['experiments'][1], fresh)

    def test_unknown_run_receipt_stays_get_only_with_short_batch(self):
        review = self.open_review(); self.finish(review)
        self.client.timeout_before_commit_number = len(self.client.posts) + 2
        with self.assertRaises(TimeoutError): self.w.tick()
        before = len(self.client.posts)
        markers = {p.name: p.read_bytes() for p in self.w.directory.glob('intent-*.json')}
        result = self.w.tick()
        self.assertEqual(result['state'], 'owned_dispatch_uncertain')
        self.assertEqual(len(self.client.posts), before)
        self.assertEqual({p.name: p.read_bytes() for p in self.w.directory.glob('intent-*.json')}, markers)

    def test_ready_candidate_is_reserved_before_another_repair_batch_is_selected(self):
        review = self.open_review(); plan = self.plan(review)
        review['approvedPlan'] = {**plan, 'experiments': plan['experiments'][:1]}
        action = plan['experiments'][0]
        review['outbox'] = {action['id']: {'state': 'ready'}}
        full = self.w.observe(); admitted = self.w.admission_with_ready(full)
        self.assertEqual(admitted['pipelineUsed'], full['pipelineUsed'] + 1)
        self.assertEqual(admitted['boardAvailableCandidateSlots'][self.problem],
                         full['boardAvailableCandidateSlots'][self.problem] - 1)
        lane = self.w.repair_first_observation(admitted, self.problem)
        self.assertNotIn(action['sourceArtifactId'], lane['planningScope']['repairSourceArtifactIds'])
        self.assertEqual(len(lane['planningScope']['repairSourceArtifactIds']), 2)
        self.assertEqual(len(self.w.ready_actions()), 1)
        review['approvedPlan'] = plan
        review['outbox'] = {a['id']: {'state': 'ready'} for a in plan['experiments']}
        lane = self.w.repair_first_observation(self.w.admission_with_ready(full), self.problem)
        self.assertNotIn('batchPurpose', lane['planningScope'])

    def test_missing_original_base_or_unverified_repair_authority_does_not_offer_short_batch(self):
        full = self.w.observe()
        bases = {r['originalBaseArtifact']['id'] for r in full['autoCheckDiagnostics']['repairableEvidence']}
        for change in ['base', 'authority']:
            observation = copy.deepcopy(full)
            if change == 'base': observation['catalog'] = [a for a in observation['catalog'] if a['id'] not in bases]
            else: observation['autoCheckDiagnostics']['repairAuthority'] = False
            before = copy.deepcopy(self.w.state); posts = len(self.client.posts)
            with self.subTest(change=change):
                lane = self.w.repair_first_observation(self.w.admission_with_ready(observation), self.problem)
                self.assertNotIn('batchPurpose', lane['planningScope'])
                self.assertEqual(self.w.state, before)
                self.assertEqual(len(self.client.posts), posts)

    def test_unavailable_selected_sources_never_launch_repair_planner_from_base_alone(self):
        lane = self.w.repair_first_observation(self.w.admission_with_ready(self.w.observe()), self.problem)
        selected = set(lane['planningScope']['repairSourceArtifactIds'])
        original = self.w.verified_artifact
        def missing_selected(row, *args, **kwargs):
            if row['id'] in selected: raise ValueError('ARTIFACT_UNAVAILABLE')
            return original(row, *args, **kwargs)
        posts = len(self.client.posts); reviews = len(self.w.state['supervisions'])
        with mock.patch.object(self.w, 'verified_artifact', side_effect=missing_selected):
            result = self.w.submit_supervisor(lane)
        self.assertEqual(result['state'], 'supervisor_sources_unavailable')
        self.assertEqual(len(self.client.posts), posts)
        self.assertEqual(len(self.w.state['supervisions']), reviews)
        self.assertTrue(selected.issubset({r['artifactId'] for r in result['omitted']}))

    def test_hard_board_reservations_are_not_reinterpreted_as_repair_capacity(self):
        full = self.w.observe()
        full['pipelineOwners'] = [{'problem': self.problem, 'ownerKey': 'pending-' + str(i),
            'stages': ['official_waiting'], 'occurrenceIds': [], 'jobIds': ['job-' + str(i)],
            'candidateSHA256': W.digest(('other candidate ' + str(i)).encode())} for i in range(8)]
        full['pipelineUsed'] = 8
        admitted = self.w.admission_with_ready(full)
        lane = self.w.repair_first_observation(admitted, self.problem)
        self.assertEqual(lane['pipelineUsed'], 8)
        self.assertEqual(lane['boardAvailableCandidateSlots'][self.problem], 0)
        self.assertNotIn(self.problem, lane['eligibleProblems'])
        self.assertNotIn('batchPurpose', lane['planningScope'])

    def test_short_batch_survives_archived_input_restart_without_duplicate_dispatch(self):
        self.f.config['statePayloadStorage'] = W.STATE_PAYLOAD_STORAGE
        self.bind_control(); self.f.install_diagnostic_fixture(self.statuses)
        self.f.w.save(); self.f.reload()
        review = self.open_review(); plan = self.plan(review)
        self.assertIn('inputRef', review); self.assertNotIn('input', review)
        original_input = copy.deepcopy(self.w.supervision_input(review))
        self.finish(review, plan); self.f.reload(); self.w.tick()
        record = next(r for r in self.w.state['supervisions'] if r['occurrenceId'] == review['occurrenceId'])
        self.assertEqual(self.w.supervision_input(record), original_input)
        owned = [r for r in self.w.state['experiments'] if r['supervisorOccurrenceId'] == review['occurrenceId']]
        self.assertEqual(len(owned), 2)
        self.f.reload(); self.w.tick()
        self.assertEqual([r for r in self.w.state['experiments']
            if r['supervisorOccurrenceId'] == review['occurrenceId']], owned)


if __name__ == '__main__': unittest.main()
