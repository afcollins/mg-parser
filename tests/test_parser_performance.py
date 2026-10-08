"""Tests for parser batching, caching, and kubelet progress reporting."""

import gzip
import io
import sqlite3
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path


# These tests do not parse YAML. Keep the parser unit tests runnable when the
# optional runtime dependency has not been installed in the test environment.
try:
    import yaml  # noqa: F401
except ModuleNotFoundError:
    sys.modules['yaml'] = types.ModuleType('yaml')

from pod_lifecycle_tracker import MustGatherParser, PodLifecycleDB, PodLifecycleQuery


class RecordingDB:
    """Minimal database double used to test parser behavior."""

    def __init__(self):
        self.ensure_calls = []
        self.log_events = []
        self.commit_calls = 0

    def ensure_pod_exists_by_name(self, namespace, name, node):
        self.ensure_calls.append((namespace, name, node))
        return False

    def insert_log_event(self, *event):
        self.log_events.append(event)

    def commit(self):
        self.commit_calls += 1


class ParserPerformanceTests(unittest.TestCase):
    def test_log_writes_commit_at_configured_interval(self):
        """Writes are committed in batches instead of once per log event."""
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'lifecycle.db'
            db = PodLifecycleDB(str(db_path))
            db.set_commit_interval(3)

            for index in range(2):
                db.insert_log_event(
                    '2026-01-01T00:00:00Z', None, f'pod-{index}', 'ns',
                    'node', 'kubelet', 'INFO', 'message', 'source'
                )

            self.assertEqual(db.pending_writes, 2)

            db.insert_log_event(
                '2026-01-01T00:00:00Z', None, 'pod-2', 'ns', 'node',
                'kubelet', 'INFO', 'message', 'source'
            )
            self.assertEqual(db.pending_writes, 0)
            db.close()

            with sqlite3.connect(db_path) as conn:
                count = conn.execute('SELECT COUNT(*) FROM log_events').fetchone()[0]
            self.assertEqual(count, 3)

    def test_repeated_kubelet_pod_uses_one_database_lookup(self):
        """Repeated events for one pod do not repeat the pod lookup."""
        db = RecordingDB()
        parser = MustGatherParser('.', db)
        line = 'Apr 30 01:04:14.977055 node kubelet[123]: SyncLoop ADD "ns/pod"'

        parser._parse_kubelet_line(line, 'node', 'source')
        parser._parse_kubelet_line(line, 'node', 'source')

        self.assertEqual(db.ensure_calls, [('ns', 'pod', 'node')])
        self.assertEqual(len(db.log_events), 2)

    def test_optimizer_indexes_both_pod_log_lookup_paths(self):
        """Pod lifecycle lookup returns UID and namespace/name log records."""
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'lifecycle.db'
            db = PodLifecycleDB(str(db_path))
            db.insert_pod('uid-1', 'ns', 'pod', 'node')
            db.insert_log_event(
                '2026-01-01T00:00:01Z', 'uid-1', 'pod', 'ns', 'node',
                'kubelet', 'INFO', 'uid event', 'source'
            )
            db.insert_log_event(
                '2026-01-01T00:00:02Z', None, 'pod', 'ns', 'node',
                'kubelet', 'INFO', 'name event', 'source'
            )

            with redirect_stdout(io.StringIO()):
                db.optimize_query_indexes()

            indexes = {
                row['name'] for row in db.conn.execute('PRAGMA index_list(log_events)')
            }
            self.assertIn('idx_log_events_pod_uid_time', indexes)
            self.assertIn('idx_log_events_namespace_pod_name_time', indexes)

            lifecycle = PodLifecycleQuery(db).get_pod_lifecycle(pod_uid='uid-1')
            self.assertEqual(
                [event['message'] for event in lifecycle['log_events']],
                ['uid event', 'name event']
            )
            db.close()

    def test_hostname_kubelet_archive_reports_progress(self):
        """Hostname-named gzip archives are discovered and produce progress."""
        with tempfile.TemporaryDirectory() as directory:
            base_path = Path(directory)
            node_name = 'w015.cluster.domain.company.io'
            log_path = base_path / 'nodes' / node_name / f'{node_name}_logs_kubelet.gz'
            log_path.parent.mkdir(parents=True)
            with gzip.open(log_path, 'wt') as log_file:
                log_file.write(
                    'Apr 30 01:04:14.977055 w015 kubelet[123]: '
                    'SyncLoop ADD "team-a/pod-a"\n'
                )
                log_file.write('unparseable line\n')

            db = RecordingDB()
            parser = MustGatherParser(
                str(base_path), db, verbose=True, progress_interval=1
            )
            output = io.StringIO()
            with redirect_stdout(output):
                parser.parse_kubelet_logs()

            progress = output.getvalue()
            self.assertIn('Found 1 kubelet log archive(s).', progress)
            self.assertIn(f'Parsing nodes/{node_name}/{node_name}_logs_kubelet.gz', progress)
            self.assertIn('2 lines scanned; 1 pod event(s) recorded.', progress)
            self.assertEqual(len(db.log_events), 1)
            self.assertEqual(db.commit_calls, 1)


if __name__ == '__main__':
    unittest.main()
