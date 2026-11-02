"""Server-only adversarial qualification of scoped planning and deterministic refill.

Uses the existing in-memory NativeAgentOS transport; never calls a provider,
official GPU endpoint, or the live controller. The original 235 tests remain a
separate suite so this fixture does not silently repeat their qualification.
"""
import copy
import importlib.util
import pathlib
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('base_watchdog_fixture',
    pathlib.Path(__file__).with_name('continuity_watchdog_test.py'))
B = importlib.util.module_from_spec(spec)
spec.loader.exec_module(B)
W = B.W


class BoardLaneTests(unittest.TestCase):
    def setUp(self):
        self.f = B.WatchTests()
        self.f.setUp()
        self.addCleanup(self.f.tearDown)
        self.f.add_seed('muse_prefill_layer', b'# prefill source\n')
        self.f.add_seed('muse_attention_decode', b'# attention source\n')
        self.f.enable_mixed_plan_contract()
        self.f.config.update(plannerMode='board-lanes-v1', targetNativeWorkers=8,
                             readyLowWatermark=2)
        self.f.config['controlDefinitionSHA256'] = W.digest(W.supervisor_definition(self.f.config))
        self.f.client.cycles[self.f.config['controlCycleId']]['definition'] = W.supervisor_definition(self.f.config)
        W.validate_config(self.f.config)
        self.f.reload()

    @property
    def w(self): return self.f.w

    @property
    def client(self): return self.f.client

    def scope(self, review):
        return self.w.supervision_input(review)['planningScope']['problem']

    def planners(self):
        return [r for r in self.w.state['supervisions']
                if self.w.supervision_input(r).get('planningScope')]

    def boot(self):
        for _ in range(3): self.w.tick()
        reviews = self.planners()
        self.assertEqual(len(reviews), 3)
        self.assertEqual({self.scope(r) for r in reviews}, set(W.PROBLEMS))
        return reviews

    def plan(self, review, count=3):
        inp = self.w.supervision_input(review)
        problem = inp['planningScope']['problem']
        source = next(a for a in inp['catalog'] if a['problem'] == problem
                      and a['id'] in inp['confirmedSourceArtifactIds'])
        actions = [{'id': 'variant-' + str(i), 'kind': 'experiment', 'problem': problem,
                    'sourceArtifactId': source['id'],
                    'hypothesis': 'Independent tile variant ' + str(i) + ' discriminates launch overhead for ' + problem,
                    'instructions': 'Change only the selected tile variant and execute a distinguishing CPU test with an explicit negative control.'}
                   for i in range(count)]
        return {'version': 2, 'evidenceFingerprint': review['fingerprint'], 'decision': 'actions',
                'reason': 'Independent source-bound CPU strategies have distinct falsifiers and preserve canonical rules.',
                'experiments': actions,
                'capacityReview': [{'problem': p, 'availableSlots': n,
                    'plannedActions': count if p == problem else 0,
                    'reason': 'This scoped planner selects useful independent work only inside its assigned board.'}
                    for p, n in inp['boardAvailableCandidateSlots'].items()]}

    def complete(self, review, count=3, plan=None):
        self.client.complete(review['occurrenceId'], {
            'continuation-plan.json': self.plan(review, count) if plan is None else plan})

    def close_worker(self, record):
        self.client.complete(record['occurrenceId'], {'review.txt': b'Negative result, actor stopped.'}, state='failed')

    def fill_native_target(self):
        # Bounded sweeps preserve a fresh observation between dispatches without
        # replaying the entire historical catalog eight times in one invocation.
        for _ in range(3):
            before = len(self.w.state['experiments'])
            self.w.tick()
            self.assertLessEqual(len(self.w.state['experiments']) - before, 3)

    def native_posts(self, rows):
        return [(route, body) for route, body in rows if route != '/api/cycles/' + self.f.config['controlCycleId'] + '/run']

    def test_three_scoped_planners_run_without_waiting_for_first_model(self):
        reviews = self.boot()
        self.assertEqual(W.supervisor_definition(self.f.config)['concurrency'], 3)
        for r in reviews:
            inp = self.w.supervision_input(r)
            self.assertEqual({a['problem'] for a in inp['catalog']}, {self.scope(r)})
            self.assertEqual(inp['eligibleProblems'], [self.scope(r)])
            self.assertEqual({a['problem'] for a in inp['sourceCapsules']['sources']}, {self.scope(r)})
            for key in ['outcomes', 'officialJobs', 'priorDispatches']:
                self.assertTrue(all(row['problem'] == self.scope(r) for row in inp.get(key, [])))
        self.assertEqual(len({r['requestKey'] for r in reviews}), 3)
        before = len(self.client.posts)
        self.w.tick()
        self.assertEqual(len(self.client.posts), before)

    def test_completed_lane_dispatches_while_two_other_planners_remain_running(self):
        reviews = self.boot()
        chosen = reviews[1]
        self.complete(chosen, 3)
        self.w.tick()
        self.assertEqual(len(self.w.state['experiments']), 3)
        self.assertEqual({r['problem'] for r in self.w.state['experiments']}, {self.scope(chosen)})
        for r in [reviews[0], reviews[2]]:
            self.assertEqual(self.client.occurrences[r['occurrenceId']]['occurrence']['state'], 'queued')

    def test_native_target_eight_keeps_approved_remainder_as_durable_backlog(self):
        reviews = self.boot()
        for r in reviews: self.complete(r, 6)
        self.fill_native_target()
        self.assertEqual(len(self.w.state['experiments']), 8)
        self.assertEqual(len(self.w.ready_actions()), 10)
        self.f.reload()
        self.assertEqual(len(self.w.state['experiments']), 8)
        self.assertEqual(len(self.w.ready_actions()), 10)
        before = len(self.client.posts)
        self.w.tick()
        self.assertEqual(len(self.w.state['experiments']), 8)
        self.assertEqual(len(self.client.posts), before)

    def test_refill_uses_approved_work_before_any_new_planner_request(self):
        reviews = self.boot()
        for r in reviews: self.complete(r, 6)
        self.fill_native_target()
        self.close_worker(self.w.state['experiments'][0])
        before = len(self.client.posts)
        self.w.tick()
        new = self.client.posts[before:]
        self.assertTrue(new)
        self.assertEqual(len(self.w.state['experiments']), 9)
        self.assertNotEqual(new[0][0], '/api/cycles/' + self.f.config['controlCycleId'] + '/run')
        self.assertEqual(len(self.w.ready_actions()), 9)

    def test_control_pause_blocks_all_lanes_and_all_ready_dispatches(self):
        reviews = self.boot()
        for r in reviews: self.complete(r, 6)
        self.fill_native_target()
        self.close_worker(self.w.state['experiments'][0])
        self.client.cycles[self.f.config['controlCycleId']]['state'] = 'paused'
        before = copy.deepcopy(self.w.state)
        posts = len(self.client.posts)
        self.assertEqual(self.w.tick()['state'], 'paused_by_control_cycle')
        self.assertEqual(self.w.state, before)
        self.assertEqual(len(self.client.posts), posts)

    def test_a_lane_cannot_submit_an_action_for_another_board(self):
        review = self.boot()[0]
        plan = self.plan(review)
        plan['experiments'][0]['problem'] = next(p for p in W.PROBLEMS if p != self.scope(review))
        self.complete(review, plan=plan)
        self.w.tick()
        self.assertEqual(self.w.state['experiments'], [])

    def test_sweep_observes_all_completed_reviews_before_refilling_any_lane(self):
        reviews = self.boot()
        for r in reviews: self.complete(r, 3)
        routes = []
        original = self.client.request
        def traced(method, route, body=None, binary=False):
            routes.append((method, route))
            return original(method, route, body, binary)
        with mock.patch.object(self.client, 'request', side_effect=traced): self.w.tick()
        first_post = next(i for i, row in enumerate(routes) if row[0] == 'POST')
        self.assertTrue(all(('GET', '/api/cycle-runs/' + r['occurrenceId']) in routes[:first_post]
                            for r in reviews))
        self.assertEqual(len(self.w.state['experiments']), 3)

    def test_archive_preserves_all_lane_inputs_and_approved_actions_after_restart(self):
        self.f.enable_payloads()
        reviews = self.boot()
        for r in reviews: self.complete(r, 6)
        self.fill_native_target()
        expected = copy.deepcopy(self.w.export_legacy_state())
        intents = {p.name: p.read_bytes() for p in self.w.directory.glob('intent-*.json')}
        self.f.reload()
        self.assertEqual(self.w.export_legacy_state(), expected)
        self.assertEqual(len(self.w.ready_actions()), 10)
        self.assertEqual({p.name: p.read_bytes() for p in self.w.directory.glob('intent-*.json')}, intents)

    def test_full_approved_backlog_reserves_twenty_four_without_starting_twenty_four_workers(self):
        reviews = self.boot()
        for r in reviews: self.complete(r, 8)
        self.fill_native_target()
        ready = self.w.ready_actions()
        self.assertEqual(len(self.w.state['experiments']), 8)
        self.assertEqual(len(ready), 16)
        counts = {p: sum(r['problem'] == p for r in self.w.state['experiments']) +
                  sum(r['experiment']['problem'] == p for r in ready) for p in W.PROBLEMS}
        self.assertEqual(counts, {p: 8 for p in W.PROBLEMS})
        posts = len(self.client.posts)
        self.w.tick()
        self.assertEqual(len(self.client.posts), posts)
        self.assertEqual(len(self.planners()), 3)

    def test_lost_worker_ack_is_reconciled_without_second_create_or_run(self):
        review = self.boot()[0]
        self.complete(review, 1)
        self.client.timeout_after_commit_number = len(self.client.posts) + 2
        try: self.w.tick()
        except TimeoutError: pass
        native_before = self.native_posts(self.client.posts)
        self.assertEqual(len(native_before), 2)
        self.client.timeout_after_commit_number = None
        self.f.reload()
        self.w.tick()
        self.assertEqual(self.native_posts(self.client.posts), native_before)
        self.assertEqual(len(self.w.state['experiments']), 1)
        self.assertTrue(any(route.startswith('/api/cycles/') for route in self.client.gets))

    def test_unknown_worker_effect_blocks_all_new_posts_and_keeps_reservation(self):
        reviews = self.boot()
        for r in reviews: self.complete(r, 3)
        self.client.timeout_before_commit_number = len(self.client.posts) + 2
        try: self.w.tick()
        except TimeoutError: pass
        posts = copy.deepcopy(self.client.posts)
        intents = {p.name: p.read_bytes() for p in self.w.directory.glob('intent-*.json')}
        self.client.timeout_before_commit_number = None
        self.f.reload()
        self.w.tick()
        self.assertEqual(self.client.posts, posts)
        self.assertEqual(self.w.state['experiments'], [])
        self.assertGreaterEqual(len(self.w.ready_actions()), 1)
        self.assertEqual({p.name: p.read_bytes() for p in self.w.directory.glob('intent-*.json')}, intents)

    def test_unknown_review_effect_prevents_other_lanes_from_creating_new_reviews(self):
        self.client.timeout_before_commit_number = 1
        try: self.w.tick()
        except TimeoutError: pass
        self.assertEqual(len(self.client.posts), 1)
        self.client.timeout_before_commit_number = None
        self.f.reload()
        result = self.w.tick()
        self.assertEqual(result['state'], 'owned_review_uncertain')
        self.assertEqual(len(self.client.posts), 1)
        self.assertEqual(self.w.state['experiments'], [])

    def test_crash_after_plan_approval_keeps_actions_for_restart(self):
        reviews = self.boot()
        for r in reviews: self.complete(r, 3)
        with mock.patch.object(self.w, 'dispatch_experiment', side_effect=RuntimeError('crash before first intent')):
            with self.assertRaisesRegex(RuntimeError, 'crash before first intent'): self.w.tick()
        self.f.reload()
        self.assertEqual(len(self.w.ready_actions()), 9)
        approved = {r['occurrenceId']: copy.deepcopy(r['approvedPlan']) for r in self.planners()}
        self.fill_native_target()
        self.assertEqual(len(self.w.state['experiments']), 8)
        self.assertEqual(len(self.w.ready_actions()), 1)
        for r in self.planners():
            if r['occurrenceId'] in approved:
                self.assertEqual(r['approvedPlan'], approved[r['occurrenceId']])

    def test_changed_selected_source_invalidates_only_its_ready_action(self):
        second = self.f.add_seed('muse_decode_layer', b'# second independent decode base\n')
        reviews = self.boot()
        review = next(r for r in reviews if self.scope(r) == 'muse_decode_layer')
        inp = self.w.supervision_input(review)
        sources = [a for a in inp['catalog'] if a['problem'] == 'muse_decode_layer']
        original_source = next(a for a in sources if a['occurrenceId'] == self.f.seed)
        second_source = next(a for a in sources if a['occurrenceId'] == second)
        plan = self.plan(review, 2)
        plan['experiments'][0]['sourceArtifactId'] = original_source['id']
        plan['experiments'][1]['sourceArtifactId'] = second_source['id']
        self.complete(review, plan=plan)
        qa = next(a for a in self.client.occurrences[self.f.seed]['artifacts'] if a['name'] == 'qa-verification.json')
        import json
        report = json.loads(self.client.artifacts[qa['id']])
        report['blockingFindings'] = ['New candidate-bound defect in the first source only.']
        raw = W.canonical(report); self.client.artifacts[qa['id']] = raw
        qa.update(size=len(raw), digest=W.digest(raw))
        self.w.tick()
        self.assertEqual([r['experimentId'] for r in self.w.state['experiments']], ['variant-1'])
        current = next(r for r in self.planners() if r['occurrenceId'] == review['occurrenceId'])
        self.assertEqual(current['outbox']['variant-0']['state'], 'invalidated')
        self.assertEqual(current['approvedPlan'], plan)

    def test_failed_lane_budget_does_not_suppress_ready_work_in_other_lane(self):
        reviews = self.boot()
        failed_scope = self.scope(reviews[0])
        self.complete(reviews[0], plan={})
        self.w.tick()
        replacements = [r for r in self.planners() if self.scope(r) == failed_scope
                        and r['state'] == 'dispatched']
        self.assertEqual(len(replacements), 1)
        self.complete(replacements[0], plan={})
        self.w.tick()
        self.assertEqual(len([r for r in self.planners() if self.scope(r) == failed_scope
                              and r['state'] == 'failed']), 2)
        self.assertFalse(any(r['state'] == 'dispatched' and self.scope(r) == failed_scope for r in self.planners()))
        self.complete(reviews[1], 3)
        self.w.tick()
        self.assertEqual(len(self.w.state['experiments']), 3)
        self.assertEqual({r['problem'] for r in self.w.state['experiments']}, {self.scope(reviews[1])})

    def test_unknown_review_handover_reconciles_original_control_and_never_posts(self):
        self.client.timeout_before_commit_number = 1
        try: self.w.tick()
        except TimeoutError: pass
        original_cycle = self.f.config['controlCycleId']
        original_requests = copy.deepcopy(self.w.state['reviewRequests'])
        original_intents = {p.name: p.read_bytes() for p in self.w.directory.glob('intent-*.json')}
        self.f.config['controlCycleId'] = B.uid()
        self.client.cycles[self.f.config['controlCycleId']] = {
            'id': self.f.config['controlCycleId'], 'state': 'active',
            'definition': W.supervisor_definition(self.f.config), 'request_key': 'new-control'}
        self.client.timeout_before_commit_number = None
        self.f.reload()
        result = self.w.tick()
        self.assertEqual(result['state'], 'owned_review_uncertain')
        self.assertEqual(result['originalControlCycleId'], original_cycle)
        self.assertIn('/api/cycles/' + original_cycle, self.client.gets)
        self.assertEqual(len(self.client.posts), 1)
        self.assertEqual(self.w.state['reviewRequests'], original_requests)
        self.assertEqual({p.name: p.read_bytes() for p in self.w.directory.glob('intent-*.json')}, original_intents)

    def test_eighteen_proposals_cannot_overcommit_eleven_fresh_native_owners(self):
        reviews = self.boot()
        for r in reviews: self.complete(r, 6)
        for i in range(11):
            problem = list(W.PROBLEMS)[i % 3]
            cid = B.uid(); self.client.cycles[cid] = {'id': cid, 'state': 'active', 'definition': {}}
            occurrence = self.client.make_occurrence(cid, {'requestKey': 'existing-' + str(i), 'input': {
                'experiment': {'problem': problem, 'sourceArtifact': {'digest': 'f' * 64},
                    'hypothesis': 'Previously admitted independent native owner ' + str(i)}}})
            self.w.state['experiments'].append({'occurrenceId': occurrence['id'], 'cycleId': cid,
                'problem': problem, 'supervisorOccurrenceId': B.uid(), 'experimentId': 'existing-' + str(i)})
        self.w.save()
        before = len(self.client.posts)
        self.w.tick()
        ready = self.w.ready_actions()
        self.assertEqual(len(self.w.state['experiments']), 11)
        self.assertLessEqual(11 + len(ready), 24)
        for problem in W.PROBLEMS:
            committed = sum(r['problem'] == problem for r in self.w.state['experiments'])
            committed += sum(r['experiment']['problem'] == problem for r in ready)
            self.assertLessEqual(committed, 8)
        self.assertEqual(self.native_posts(self.client.posts[before:]), [])

    def test_source_change_preserves_all_acknowledged_owners_and_invalidates_only_pending_action(self):
        second = self.f.add_seed('muse_decode_layer', b'# another confirmed decode base\n')
        reviews = self.boot()
        decode = next(r for r in reviews if self.scope(r) == 'muse_decode_layer')
        sources = self.w.supervision_input(decode)['catalog']
        first = next(a for a in sources if a['occurrenceId'] == self.f.seed)
        other = next(a for a in sources if a['occurrenceId'] == second)
        plan = self.plan(decode, 6)
        for i, exp in enumerate(plan['experiments']):
            exp['sourceArtifactId'] = first['id'] if i in [0, 3] else other['id']
        for r in reviews: self.complete(r, 6, plan if r is decode else None)
        self.fill_native_target()
        original = copy.deepcopy(self.w.state['experiments'])
        self.assertEqual(len(original), 8)
        self.assertTrue(any(r['supervisorOccurrenceId'] == decode['occurrenceId'] and
                            r['experimentId'] == 'variant-0' for r in original))
        qa = next(a for a in self.client.occurrences[self.f.seed]['artifacts'] if a['name'] == 'qa-verification.json')
        import json
        report = json.loads(self.client.artifacts[qa['id']]); report['blockingFindings'] = ['New exact-source defect.']
        raw = W.canonical(report); self.client.artifacts[qa['id']] = raw; qa.update(size=len(raw), digest=W.digest(raw))
        # Stale ready work must stop occupying the buffer even while all eight
        # native slots are full. Existing ACKs remain untouched.
        self.w.tick()
        current = next(r for r in self.planners() if r['occurrenceId'] == decode['occurrenceId'])
        self.assertEqual(self.w.state['experiments'], original)
        self.assertEqual(current['outbox']['variant-3']['state'], 'invalidated')
        for r in original: self.close_worker(r)
        self.w.tick()
        self.assertEqual(self.w.state['experiments'][:8], original)
        self.assertEqual(len({r['occurrenceId'] for r in self.w.state['experiments']}), len(self.w.state['experiments']))
        current = next(r for r in self.planners() if r['occurrenceId'] == decode['occurrenceId'])
        self.assertEqual(current['outbox']['variant-0']['state'], 'owned')
        self.assertEqual(current['outbox']['variant-3']['state'], 'invalidated')
        self.assertEqual(current['approvedPlan'], plan)

    def test_old_running_portfolio_planner_does_not_stall_new_lanes_or_lose_input(self):
        lane_config = copy.deepcopy(self.f.config)
        for key in ['plannerMode', 'targetNativeWorkers', 'readyLowWatermark']: self.f.config.pop(key)
        self.f.config['controlDefinitionSHA256'] = W.digest(W.supervisor_definition(self.f.config))
        self.client.cycles[self.f.config['controlCycleId']]['definition'] = W.supervisor_definition(self.f.config)
        self.f.reload(); self.w.tick()
        old = self.w.state['supervisions'][0]
        old_input = copy.deepcopy(self.w.supervision_input(old))
        old_occurrence = copy.deepcopy(self.client.occurrences[old['occurrenceId']])
        self.f.config.clear(); self.f.config.update(lane_config); self.f.config['controlCycleId'] = B.uid()
        self.client.cycles[self.f.config['controlCycleId']] = {'id': self.f.config['controlCycleId'],
            'state': 'active', 'definition': W.supervisor_definition(self.f.config), 'request_key': 'lane-handover'}
        self.f.reload(); self.w.tick()
        self.assertEqual(len(self.planners()), 3)
        self.assertEqual(self.w.supervision_input(self.w.state['supervisions'][0]), old_input)
        self.assertEqual(self.client.occurrences[old['occurrenceId']], old_occurrence)
        self.assertEqual(self.w.state['supervisions'][0]['state'], 'dispatched')

    def test_approved_ready_hypothesis_cannot_be_renamed_by_replenishment_planner(self):
        reviews = self.boot()
        for r in reviews: self.complete(r, 3)
        self.fill_native_target()
        ready = self.w.ready_actions()
        self.assertEqual(len(ready), 1)
        reserved = copy.deepcopy(ready[0]['experiment'])
        owner = ready[0]['supervisor']['occurrenceId']
        replacement = next(r for r in self.planners() if r['state'] == 'dispatched'
                           and self.scope(r) == reserved['problem'])
        plan = self.plan(replacement, 1)
        plan['experiments'][0].update(id='renamed-duplicate', sourceArtifactId=reserved['sourceArtifactId'],
            hypothesis='  ' + reserved['hypothesis'].upper() + '  ')
        self.complete(replacement, plan=plan)
        before = len(self.w.state['experiments'])
        self.w.tick()
        current = next(r for r in self.planners() if r['occurrenceId'] == replacement['occurrenceId'])
        self.assertEqual(current['state'], 'failed')
        self.assertEqual(current['errorCode'], 'PLAN_DUPLICATE_HYPOTHESIS')
        self.assertEqual(len(self.w.state['experiments']), before)
        self.assertEqual([(r['supervisor']['occurrenceId'], r['experiment']) for r in self.w.ready_actions()],
                         [(owner, reserved)])


if __name__ == '__main__': unittest.main()
