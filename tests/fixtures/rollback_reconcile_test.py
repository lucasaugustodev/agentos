"""Recovery state machine and real receipt binding with isolated external IO."""
import copy
import importlib.util
import json
import pathlib
import tempfile
import types
import unittest
import uuid
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('recovery', ROOT / 'scripts/agentos-rollback-reconcile.py')
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)
spec = importlib.util.spec_from_file_location('publisher', ROOT / 'scripts/agentos-release-action.py')
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


def request():
    return {'occurrenceId': str(uuid.uuid4()), 'operationId': str(uuid.uuid4()),
            'validationReceiptSha256': 'a' * 64, 'blockedReceiptSha256': 'b' * 64}


class FakeIO:
    def __init__(self):
        self.request = request()
        self.binding = {'candidate': 'candidate', 'base': 'base', 'baseDigest': 'c' * 64,
                        'request': self.request}
        self.files, self.effects = {}, []
        self.current = 'candidate'
        self.api = 'stopped'
        self.schema = None
        self.lose = None
        self.scope_changed = False
        self.chats = 0

    def get(self, key):
        return copy.deepcopy(self.files.get(key))

    def put(self, key, value):
        if key in self.files:
            raise ValueError('EXISTING_RECEIPT')
        self.files[key] = copy.deepcopy(value)

    def bind(self, value):
        if value != self.request:
            raise ValueError('RECOVERY_OWNER_CHANGED')
        return copy.deepcopy(self.binding)

    def pointer(self):
        return self.current

    def require_api_stopped(self):
        if self.api != 'stopped':
            raise ValueError('API_STOP_REQUIRED')

    def before_schema(self, binding):
        self.require_api_stopped()
        if self.scope_changed:
            raise ValueError('NEW_SCOPE_EFFECT_UNPROVEN')
        if self.chats:
            raise ValueError('API_CHAT_STOP_UNCONFIRMED')

    def launch_schema(self, value):
        self.effects.append('schema')
        if self.lose == 'schema':
            raise TimeoutError('lost response')

    def schema_result(self, binding):
        if self.schema == 'uncertain':
            raise ValueError('SCHEMA_EFFECT_UNCONFIRMED')
        if self.schema == 'failed':
            raise ValueError('SCHEMA_RECOVERY_FAILED')
        if self.schema is None:
            return None
        self.files['schema-result'] = copy.deepcopy(self.schema)
        return copy.deepcopy(self.schema)

    def require_rollback_readiness(self, binding):
        self.effects.append('guard')
        if self.lose == 'guard':
            raise ValueError('ROLLBACK_SCHEMA_NOT_READY')
        if self.current != 'candidate':
            raise AssertionError('Canonical pre-swap guard called after swap')
        return {'ok': True, 'source': 'base'}

    def check_readiness_record(self, proof, binding, schema):
        if (proof.get('binding') != binding or proof.get('schemaResultSha256') != g.sha(g.canonical(schema)) or
                proof.get('proof') != {'ok': True, 'source': 'base'}):
            raise ValueError('ROLLBACK_READINESS_RECEIPT_UNBOUND')

    def swap(self, binding):
        self.effects.append('swap')
        self.current = 'base'
        if self.lose == 'swap':
            raise TimeoutError('lost response')

    def verify_saved_swap(self, binding):
        proof, swap = self.get('readiness'), self.get('swap-intent')
        if not proof or not swap:
            raise ValueError('ROLLBACK_SWAP_UNPROVEN')
        self.check_readiness_record(proof, binding, self.schema)
        if swap.get('readinessSha256') != g.sha(g.canonical(proof)):
            raise ValueError('ROLLBACK_SWAP_UNPROVEN')

    def after_swap(self, binding):
        if self.current != 'base':
            raise ValueError('RECOVERY_POINTER_CHANGED')

    def start_api(self):
        self.effects.append('start')
        self.api = 'active'
        if self.lose == 'start':
            raise TimeoutError('lost response')

    def api_result(self, binding):
        if self.api == 'failed':
            raise ValueError('API_START_UNCONFIRMED')
        return {'healthy': True, 'source': 'base'} if self.api == 'active' else None

    def verify_complete(self, binding):
        self.verify_saved_swap(binding)
        self.after_swap(binding)
        if self.api_result(binding) is None:
            raise ValueError('RECOVERY_COMPLETION_UNPROVEN')
        return {'state': 'completed', 'readOnlyReplay': True}


class StateMachineTests(unittest.TestCase):
    def setUp(self):
        self.io = FakeIO()

    def run_step(self):
        return g.reconcile(self.io, self.io.request)

    def ready(self):
        self.assertEqual(self.run_step()['state'], 'schema_pending')
        self.io.schema = {'exitCode': 0, 'binding': self.io.binding}

    def complete(self):
        self.ready()
        self.assertEqual(self.run_step()['state'], 'api_pending')
        self.assertEqual(self.run_step()['state'], 'completed')

    def test_one_schema_one_swap_one_start_with_durable_completion(self):
        self.complete()
        for effect in ('schema', 'swap', 'start'):
            self.assertEqual(self.io.effects.count(effect), 1)

    def test_pending_schema_only_observes(self):
        for _ in range(3):
            self.assertEqual(self.run_step()['state'], 'schema_pending')
        self.assertEqual(self.io.effects, ['schema'])

    def test_lost_schema_launch_response_never_launches_twice(self):
        self.io.lose = 'schema'
        with self.assertRaises(TimeoutError): self.run_step()
        self.assertEqual(self.run_step()['state'], 'schema_pending')
        self.assertEqual(self.io.effects, ['schema'])

    def test_uncertain_schema_cannot_be_replayed_or_swapped(self):
        self.ready(); self.io.schema = 'uncertain'
        with self.assertRaisesRegex(ValueError, 'EFFECT_UNCONFIRMED'): self.run_step()
        self.assertEqual(self.io.effects, ['schema'])

    def test_failed_schema_preserves_candidate(self):
        self.ready(); self.io.schema = 'failed'
        with self.assertRaisesRegex(ValueError, 'RECOVERY_FAILED'): self.run_step()
        self.assertEqual(self.io.current, 'candidate')

    def test_old_readiness_failure_never_swaps(self):
        self.ready(); self.io.lose = 'guard'
        with self.assertRaisesRegex(ValueError, 'SCHEMA_NOT_READY'): self.run_step()
        self.assertEqual(self.io.current, 'candidate')
        self.assertNotIn('swap', self.io.effects)

    def test_lost_swap_response_uses_saved_guard_without_repeating_swap(self):
        self.ready(); self.io.lose = 'swap'
        with self.assertRaises(TimeoutError): self.run_step()
        self.io.lose = None
        self.assertEqual(self.run_step()['state'], 'api_pending')
        self.assertEqual(self.io.effects.count('guard'), 1)
        self.assertEqual(self.io.effects.count('swap'), 1)

    def test_lost_api_start_observes_healthy_api_even_with_new_chat(self):
        self.ready(); self.io.lose = 'start'
        with self.assertRaises(TimeoutError): self.run_step()
        self.io.chats = 1
        self.assertEqual(self.run_step()['state'], 'completed')
        self.assertEqual(self.io.effects.count('start'), 1)

    def test_failed_api_start_is_not_restarted(self):
        self.ready(); self.run_step(); self.io.api = 'failed'
        with self.assertRaisesRegex(ValueError, 'START_UNCONFIRMED'): self.run_step()
        self.assertEqual(self.io.effects.count('start'), 1)

    def test_live_api_requires_external_confirmed_stop(self):
        self.io.api = 'active'
        with self.assertRaisesRegex(ValueError, 'API_STOP_REQUIRED'): self.run_step()
        self.assertEqual(self.io.effects, [])

    def test_uncertain_api_does_not_receive_any_signal(self):
        self.io.api = 'uncertain'
        with self.assertRaisesRegex(ValueError, 'API_STOP_REQUIRED'): self.run_step()
        self.assertEqual(self.io.effects, [])

    def test_pending_chat_blocks_schema(self):
        self.io.chats = 1
        with self.assertRaisesRegex(ValueError, 'CHAT_STOP_UNCONFIRMED'): self.run_step()
        self.assertEqual(self.io.effects, [])

    def test_changed_scope_after_first_probe_holds_original_owner(self):
        self.ready(); self.io.scope_changed = True
        with self.assertRaisesRegex(ValueError, 'NEW_SCOPE_EFFECT'): self.run_step()
        self.assertEqual(self.io.effects, ['schema'])

    def test_another_operation_cannot_take_the_existing_recovery(self):
        self.ready()
        other = {**self.io.request, 'operationId': str(uuid.uuid4())}
        with self.assertRaisesRegex(ValueError, 'OWNER_CHANGED'): g.reconcile(self.io, other)

    def test_foreign_pointer_cannot_be_returned_to_base(self):
        self.ready(); self.io.current = 'foreign'
        with self.assertRaisesRegex(ValueError, 'POINTER_CHANGED'): self.run_step()

    def test_base_pointer_without_saved_swap_is_not_success(self):
        self.ready(); self.io.current = 'base'
        with self.assertRaisesRegex(ValueError, 'SWAP_UNPROVEN'): self.run_step()

    def test_changed_intent_cannot_authorize_schema(self):
        self.ready(); self.io.files['intent']['binding']['base'] = 'foreign'
        with self.assertRaisesRegex(ValueError, 'INTENT_CHANGED'): self.run_step()

    def test_changed_saved_proof_rejects_post_swap_replay(self):
        self.complete(); self.io.files['readiness']['proof']['ok'] = False
        with self.assertRaisesRegex(ValueError, 'COMPLETION_RECEIPT_UNBOUND'): self.run_step()

    def test_changed_completion_identity_does_not_become_read_only_success(self):
        self.complete(); self.io.files['complete']['binding']['base'] = 'foreign'
        with self.assertRaisesRegex(ValueError, 'COMPLETION_RECEIPT_UNBOUND'): self.run_step()

    def test_completed_replay_is_read_only(self):
        self.complete(); effects = list(self.io.effects)
        self.assertTrue(self.run_step()['readOnlyReplay'])
        self.assertEqual(self.io.effects, effects)

    def test_changed_schema_intent_rejects_before_observation(self):
        self.ready(); self.io.files['schema-intent']['binding']['base'] = 'foreign'
        with self.assertRaisesRegex(ValueError, 'SCHEMA_INTENT_CHANGED'): self.run_step()


class ReceiptBindingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.directory = pathlib.Path(self.temp.name)
        self.root, self.state, self.control = (self.directory / x for x in ('root', 'state', 'control'))
        for path in (self.root, self.state, self.control): path.mkdir()
        self.base = self.root / 'releases' / ('managed-' + str(uuid.uuid4())); self.base.mkdir(parents=True)
        self.candidate = self.base.with_name('managed-' + str(uuid.uuid4())); self.candidate.mkdir()
        (self.base / 'source').write_text('base'); (self.candidate / 'source').write_text('candidate')
        self.environment = self.directory / 'server.env'; self.environment.write_text('fixture only')
        self.request = request()
        self.publisher = {'path': str(self.control / 'agentos-release-action.py'), 'sha256': g.EXECUTOR_SHA,
                          'sourceCommit': 'a' * 40, 'registration': {'fixture': True}}
        self.record = {'candidate': str(self.candidate), 'digest': publisher.digest(self.candidate),
                       'base': str(self.base), 'candidateOrigin': {'published': {'releaseDigest': publisher.digest(self.base)}}}
        self.record_path = self.state / (self.request['occurrenceId'] + '.json')
        self.record_path.write_bytes(g.canonical(self.record))
        self.request['validationReceiptSha256'] = g.sha(self.record_path.read_bytes())
        self.blocked = {'version': 1, **{k: self.record[k] for k in ('candidate', 'digest', 'base')},
                        'validationReceiptSha256': self.request['validationReceiptSha256'], 'publisher': self.publisher,
                        'state': 'blocked', 'code': 'ROLLBACK_READINESS_UNPROVEN'}
        self.blocked_path = self.state / (self.request['occurrenceId'] + '.rollback.json')
        self.blocked_path.write_bytes(g.canonical(self.blocked))
        self.request['blockedReceiptSha256'] = g.sha(self.blocked_path.read_bytes())
        self.registration = {'_sha256': 'd' * 64, 'toolSha256': 'e' * 64,
                             'transition': {'occurrenceId': self.request['occurrenceId'],
                                            'validationReceiptSha256': self.request['validationReceiptSha256'],
                                            **{k: self.record[k] for k in ('candidate', 'digest', 'base')},
                                            'baseDigest': publisher.digest(self.base)}}
        self.guard = types.SimpleNamespace(digest=publisher.digest, _RECOVERY_PUBLISHER=self.publisher)
        for name, value in [('ROOT', self.root), ('STATE', self.state), ('CONTROL', self.control), ('ENV', self.environment)]:
            p = patch.object(g, name, value); p.start(); self.addCleanup(p.stop)
        # Synthetic temp root is world-traversable. Production protection is
        # separately exercised, and the real receipt/source code runs below.
        p = patch.object(g, 'protected', side_effect=lambda path, **_: pathlib.Path(path)); p.start(); self.addCleanup(p.stop)
        self.io = g.Runtime(self.registration, self.guard, self.request)

    def bind(self): return self.io.bind(self.request)

    def rewrite_blocked(self):
        self.blocked_path.write_bytes(g.canonical(self.blocked))
        self.request['blockedReceiptSha256'] = g.sha(self.blocked_path.read_bytes())

    def test_actual_receipts_bind_candidate_base_and_owner(self):
        self.assertEqual(self.bind()['base'], str(self.base))
        self.assertEqual(self.bind()['candidate'], str(self.candidate))

    def test_missing_blocked_observation_rejects_unrecorded_crash(self):
        self.blocked_path.unlink()
        with self.assertRaisesRegex(ValueError, 'PHASE_UNPROVEN'): self.bind()

    def test_changed_validation_receipt_is_rejected(self):
        self.record_path.write_text('{}')
        with self.assertRaisesRegex(ValueError, 'RECEIPT_CHANGED'): self.bind()

    def test_changed_blocked_observation_is_rejected(self):
        self.blocked_path.write_text('{}')
        with self.assertRaisesRegex(ValueError, 'RECEIPT_CHANGED'): self.bind()

    def test_foreign_publisher_registration_is_rejected(self):
        self.blocked['publisher'] = {**self.publisher, 'registration': {'foreign': True}}; self.rewrite_blocked()
        with self.assertRaisesRegex(ValueError, 'PUBLISHER_UNBOUND'): self.bind()

    def test_changed_base_source_is_rejected(self):
        (self.base / 'source').write_text('drift')
        with self.assertRaisesRegex(ValueError, 'SOURCE_CHANGED'): self.bind()

    def test_changed_candidate_source_is_rejected(self):
        (self.candidate / 'source').write_text('drift')
        with self.assertRaisesRegex(ValueError, 'SOURCE_CHANGED'): self.bind()

    def test_pending_worker_adoption_is_rejected(self):
        (self.state / 'pending-workers.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'ADOPTION_PENDING'): self.bind()

    def test_another_transition_registration_is_rejected(self):
        self.registration['transition']['baseDigest'] = 'f' * 64
        with self.assertRaisesRegex(ValueError, 'TRANSITION_UNBOUND'): self.bind()

    def test_nonblocked_observation_is_outside_scope(self):
        self.blocked['state'] = 'ready'; self.rewrite_blocked()
        with self.assertRaisesRegex(ValueError, 'PHASE_UNPROVEN'): self.bind()

    def test_original_receipts_are_not_modified_by_binding(self):
        before = (self.record_path.read_bytes(), self.blocked_path.read_bytes())
        self.bind()
        self.assertEqual(before, (self.record_path.read_bytes(), self.blocked_path.read_bytes()))

    def test_existing_owner_rejects_different_operation(self):
        self.bind(); other = {**self.request, 'operationId': str(uuid.uuid4())}
        io = g.Runtime(self.registration, self.guard, other)
        with self.assertRaisesRegex(ValueError, 'OWNER_CHANGED'): io.bind(other)


class InputTests(unittest.TestCase):
    def test_rejects_arbitrary_command_field(self):
        with self.assertRaisesRegex(ValueError, 'RECOVERY_INPUT'): g.parse_request({**request(), 'command': 'anything'})

    def test_rejects_noncanonical_uuid(self):
        value = request(); value['operationId'] = value['operationId'].upper()
        with self.assertRaisesRegex(ValueError, 'RECOVERY_IDENTITY'): g.parse_request(value)

    def test_rejects_nonhex_receipt_identity(self):
        value = request(); value['validationReceiptSha256'] = 'z' * 64
        with self.assertRaisesRegex(ValueError, 'RECOVERY_DIGEST'): g.parse_request(value)


if __name__ == '__main__': unittest.main()
