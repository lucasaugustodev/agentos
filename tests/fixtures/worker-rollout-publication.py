"""Exercise actual publication code with only temporary files and fake I/O.

No systemd, network, database or /opt paths are accessed by the compiled main.
"""
import ast, contextlib, fcntl, hashlib, importlib.util, io, json, multiprocessing, os
import pathlib, shutil, sys, tempfile, time, uuid
from unittest.mock import patch


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


origin = module('rollout_fixture_origin', pathlib.Path('scripts/candidate_origin.py'))
manager = module('rollout_fixture_manager', pathlib.Path('bridge/workspace-manager.py'))


def publisher(root, state, occurrence, ready, complete, result, layout, invalid=False):
    source = pathlib.Path('scripts/agentos-release-action.py').read_text()
    tree = ast.parse(source)
    selected = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in ['main', 'digest']], type_ignores=[])

    class Response:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *_args): pass

    class FakeRequest:
        @staticmethod
        def urlopen(*_args, **_kwargs): return Response()

    class FakeUrllib:
        request = FakeRequest

    class IsolatedOrigin:
        @staticmethod
        def require_publish_origin(record):
            # Exercise the real origin check, confined to this fixture's layout.
            # Loading the protected production bundle is a separate boundary test.
            proof = origin.require_publish_origin(record, layout)
            result['originChecked'] = True
            return proof

    def fake_source(_candidate, _record):
        # At this point the swap has happened, but pending is not written yet.
        other = open(state / 'publication.lock', 'a')
        try:
            try:
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
                result['lockedThroughPending'] = False
            except BlockingIOError:
                result['lockedThroughPending'] = True
        finally:
            other.close()

    namespace = {'ROOT': root, 'STATE': state, 'sys': sys, 'json': json, 'uuid': uuid,
                 'pathlib': pathlib, 'os': os, 'time': time, 'fcntl': fcntl,
                 'hashlib': hashlib, 'shutil': shutil, 'urllib': FakeUrllib,
                 'run': lambda *_args, **_kwargs: '', 'publish_source': fake_source,
                 'trusted_origin_module': lambda: IsolatedOrigin}
    namespace['publication_executor'] = lambda _origin: {'fixture': 'registered publisher'}
    exec(compile(selected, 'sandboxed-release-action.py', 'exec'), namespace)
    sys.argv = ['sandboxed-release-action.py', 'publish']
    sys.stdin = io.StringIO(json.dumps({'occurrenceId': occurrence}))
    ready.set()
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            namespace['main']()
    except ValueError as error:
        if not invalid or str(error) != 'CANDIDATE_ORIGIN_RECEIPT_UNBOUND':
            raise
        result['invalidOriginRejected'] = True
    else:
        if invalid:
            raise AssertionError('Publication accepted a mismatched origin receipt')
    complete.set()


if __name__ == '__main__':
    # Linux fork keeps all test processes inside this one isolated invocation.
    ctx = multiprocessing.get_context('fork')
    with tempfile.TemporaryDirectory(prefix='agentos-rollout-publication-') as temp, contextlib.ExitStack() as stack:
        temp = pathlib.Path(temp)
        layout = origin.Layout(temp)
        for target, name, value in [(origin, 'OWNER_UID', os.getuid()),
                                    (origin, 'PROTECTION_ROOT', temp),
                                    (manager, 'ROOT', layout.workspaces),
                                    (manager, 'DATA', layout.data)]:
            stack.enter_context(patch.object(target, name, value))
        stack.enter_context(patch.object(manager.storage_capacity, 'reserve', side_effect=lambda *a: contextlib.nullcontext()))
        root, state = layout.active.parent, layout.receipts
        for path in [layout.releases, state, layout.workspaces / 'agentos', layout.data]:
            path.mkdir(parents=True)
        before = layout.releases / ('managed-' + str(uuid.uuid4()))
        after = layout.releases / ('managed-' + str(uuid.uuid4()))
        before.mkdir(); after.mkdir()
        (before / 'source.txt').write_text('base')
        (after / 'source.txt').write_text('reviewed')
        layout.active.symlink_to(before)
        prior = {'candidate': str(before), 'base': str(before), 'previous': str(before),
                 'digest': origin.release_digest(before), 'validatedAt': time.time() - 2,
                 'publishedAt': time.time() - 1}
        (state / (str(uuid.uuid4()) + '.json')).write_text(json.dumps(prior))
        source = 'agentos/src' + prior['digest'][:16]
        shutil.copytree(before, layout.workspaces / source)
        workspace = 'agentos/ocfixture'
        manager.prepare({'workspace': workspace, 'sourceWorkspace': source})
        (manager.workspace(workspace) / 'source.txt').write_text('reviewed')
        candidate_digest = manager.source_hash(manager.sources(manager.workspace(workspace)))
        proof = origin.capture_origin(workspace, candidate_digest, layout)
        assert proof['publishEligible'], proof
        occurrence = str(uuid.uuid4())
        checksum = hashlib.sha256(b'source.txtreviewed').hexdigest()
        record = {'base': str(before), 'candidate': str(after), 'digest': checksum,
                  'sourceCandidateDigest': candidate_digest, 'candidateOrigin': proof}
        receipt = state / (occurrence + '.json')
        receipt.write_text(json.dumps(record | {'sourceCandidateDigest': '0' * 64}))
        ready, complete = ctx.Event(), ctx.Event()
        with ctx.Manager() as manager:
            result = manager.dict()
            rejected = ctx.Process(target=publisher, args=(root, state, occurrence, ready, complete, result, layout, True))
            rejected.start(); rejected.join(5)
            if rejected.is_alive():
                rejected.terminate(); rejected.join(2)
                raise RuntimeError('Origin rejection did not finish')
            assert rejected.exitcode == 0 and result.get('invalidOriginRejected') is True
            assert layout.active.resolve() == before and not (state / 'pending-workers.json').exists()
            assert 'lockedThroughPending' not in result and 'originChecked' not in result
            receipt.write_text(json.dumps(record))
            ready.clear(); complete.clear()
            with open(state / 'publication.lock', 'a') as holder:
                fcntl.flock(holder, fcntl.LOCK_EX)
                child = ctx.Process(target=publisher, args=(root, state, occurrence, ready, complete, result, layout))
                child.start()
                try:
                    if not ready.wait(3): raise RuntimeError('Publisher did not start')
                    result['lockedBeforeSwap'] = not complete.wait(0.15) and (root / 'current').resolve() == before and not (state / 'pending-workers.json').exists()
                    fcntl.flock(holder, fcntl.LOCK_UN)
                    if not complete.wait(5): raise RuntimeError('Publisher did not finish after lock release')
                    child.join(2)
                    if child.exitcode != 0: raise RuntimeError('Sandbox publication failed')
                    result['publishedAfterRelease'] = (root / 'current').resolve() == after and json.loads((state / 'pending-workers.json').read_text())['candidate'] == str(after)
                    assert all(result.get(key) is True for key in ['lockedBeforeSwap', 'lockedThroughPending', 'publishedAfterRelease']), 'Publication lock contract changed'
                    print(json.dumps(dict(result)))
                finally:
                    if child.is_alive(): child.terminate(); child.join(2)
