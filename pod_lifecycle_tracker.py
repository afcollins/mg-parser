#!/usr/bin/env python3
"""
Pod Lifecycle Tracker for OpenShift must-gather analysis.

Parses logs from must-gather to track pod scheduling lifecycle:
- K8s events (scheduling, pulling, created, started)
- Kubelet logs (pod admission, sync)
- CRI-O logs (container creation)
- Pod metadata

Detects anomalies like double-scheduling.
"""

import sqlite3
import csv
import yaml
import gzip
import json
import io
import re
import os
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from collections import defaultdict
import argparse


class PodLifecycleDB:
    """SQLite database for pod lifecycle events."""

    def __init__(self, db_path: str = "pod_lifecycle.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_schema()

    def create_schema(self):
        """Create database schema."""
        cursor = self.conn.cursor()

        # Pods table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pods (
                uid TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                node TEXT,
                created_at TEXT,
                yaml_path TEXT
            )
        """)

        # Events table - K8s events from events.yaml
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pod_uid TEXT,
                namespace TEXT,
                pod_name TEXT,
                event_time TEXT NOT NULL,
                reason TEXT,
                message TEXT,
                source TEXT,
                source_component TEXT,
                node TEXT,
                event_type TEXT,
                source_file TEXT,
                FOREIGN KEY (pod_uid) REFERENCES pods(uid)
            )
        """)

        # Log events - from kubelet, crio logs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS log_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                pod_uid TEXT,
                pod_name TEXT,
                namespace TEXT,
                node TEXT,
                log_source TEXT,
                log_level TEXT,
                message TEXT,
                source_file TEXT
            )
        """)

        # Containers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS containers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pod_uid TEXT,
                container_name TEXT,
                image TEXT,
                FOREIGN KEY (pod_uid) REFERENCES pods(uid)
            )
        """)

        # Indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_pod_uid ON events(pod_uid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON events(event_time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_reason ON events(reason)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_events_pod_uid ON log_events(pod_uid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_events_time ON log_events(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pods_namespace_name ON pods(namespace, name)")

        self.conn.commit()

    def insert_pod(self, uid: str, namespace: str, name: str, node: str = None,
                   created_at: str = None, yaml_path: str = None):
        """Insert or update pod record."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO pods (uid, namespace, name, node, created_at, yaml_path)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET
                namespace = excluded.namespace,
                name = excluded.name,
                node = COALESCE(excluded.node, node),
                created_at = COALESCE(excluded.created_at, created_at),
                yaml_path = COALESCE(excluded.yaml_path, yaml_path)
        """, (uid, namespace, name, node, created_at, yaml_path))
        self.conn.commit()

    def ensure_pod_exists(self, uid: str, namespace: str, name: str, node: str = None) -> bool:
        """Ensure pod record exists, creating minimal record if needed.
        Returns True if new record was created."""
        if uid and namespace and name:
            cursor = self.conn.cursor()
            cursor.execute("SELECT uid FROM pods WHERE uid = ?", (uid,))
            exists = cursor.fetchone() is not None
            if not exists:
                self.insert_pod(uid, namespace, name, node)
            return not exists
        return False

    def ensure_pod_exists_by_name(self, namespace: str, name: str, node: str = None) -> bool:
        """Ensure pod exists by namespace/name (no UID known).
        Generates synthetic UID from hash of namespace/name.
        Returns True if new record was created."""
        if namespace and name:
            cursor = self.conn.cursor()
            # Check if pod already exists by namespace/name
            cursor.execute("SELECT uid FROM pods WHERE namespace = ? AND name = ?", (namespace, name))
            existing = cursor.fetchone()
            if existing:
                return False

            # Generate synthetic UID from namespace/name
            synthetic_uid = f"log-{hashlib.sha256(f'{namespace}/{name}'.encode()).hexdigest()[:32]}"
            cursor.execute("SELECT uid FROM pods WHERE uid = ?", (synthetic_uid,))
            if cursor.fetchone():
                return False  # Already exists with synthetic UID

            self.insert_pod(synthetic_uid, namespace, name, node)
            return True
        return False

    def insert_event(self, pod_uid: Optional[str], namespace: str, pod_name: str,
                     event_time: str, reason: str, message: str, source: str = None,
                     source_component: str = None, node: str = None,
                     event_type: str = None, source_file: str = None):
        """Insert K8s event."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO events (pod_uid, namespace, pod_name, event_time, reason,
                              message, source, source_component, node, event_type, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (pod_uid, namespace, pod_name, event_time, reason, message,
              source, source_component, node, event_type, source_file))
        self.conn.commit()

    def insert_log_event(self, timestamp: str, pod_uid: Optional[str], pod_name: str,
                         namespace: str, node: str, log_source: str, log_level: str,
                         message: str, source_file: str):
        """Insert log event from kubelet/crio."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO log_events (timestamp, pod_uid, pod_name, namespace, node,
                                   log_source, log_level, message, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (timestamp, pod_uid, pod_name, namespace, node, log_source,
              log_level, message, source_file))
        self.conn.commit()

    def insert_container(self, pod_uid: str, container_name: str, image: str):
        """Insert container record."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO containers (pod_uid, container_name, image)
            VALUES (?, ?, ?)
        """, (pod_uid, container_name, image))
        self.conn.commit()

    def close(self):
        """Close database connection."""
        self.conn.close()


class MustGatherParser:
    """Parser for OpenShift must-gather archives."""

    def __init__(self, must_gather_path: str, db: PodLifecycleDB):
        self.base_path = Path(must_gather_path)
        self.db = db
        self.stats = {
            'pods_from_yaml': 0,
            'pods_from_events': 0,
            'pods_from_logs': 0,
            'events': 0,
            'log_events': 0,
            'errors': 0
        }

    def parse_all(self):
        """Parse all components of must-gather."""
        print("Parsing must-gather archive...")

        print("  Parsing pod metadata...")
        self.parse_pod_metadata()

        print("  Parsing K8s events...")
        self.parse_events()

        print("  Parsing scheduler logs...")
        self.parse_scheduler_logs()

        print("  Parsing kubelet logs...")
        self.parse_kubelet_logs()

        print("  Parsing crio logs...")
        self.parse_crio_logs()

        # Get total pod count from DB
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM pods")
        total_pods = cursor.fetchone()['count']

        print(f"\nParsing complete:")
        print(f"  Pods: {total_pods} ({self.stats['pods_from_yaml']} from YAML, {self.stats['pods_from_events']} from events, {self.stats['pods_from_logs']} from logs)")
        print(f"  Events: {self.stats['events']}")
        print(f"  Log events: {self.stats['log_events']}")
        print(f"  Errors: {self.stats['errors']}")

    def parse_pod_metadata(self):
        """Parse pod YAML files to extract metadata."""
        pod_dirs = self.base_path.glob("namespaces/*/pods/*")

        for pod_dir in pod_dirs:
            if not pod_dir.is_dir():
                continue

            # Find pod YAML file
            yaml_files = list(pod_dir.glob("*.yaml"))
            if not yaml_files:
                continue

            yaml_file = yaml_files[0]
            try:
                with open(yaml_file, 'r') as f:
                    pod_data = yaml.safe_load(f)

                if not pod_data or pod_data.get('kind') != 'Pod':
                    continue

                metadata = pod_data.get('metadata', {})
                spec = pod_data.get('spec', {})
                status = pod_data.get('status', {})

                uid = metadata.get('uid')
                namespace = metadata.get('namespace')
                name = metadata.get('name')
                node = spec.get('nodeName')
                created_at = metadata.get('creationTimestamp')

                if uid and namespace and name:
                    self.db.insert_pod(uid, namespace, name, node, created_at,
                                      str(yaml_file.relative_to(self.base_path)))
                    self.stats['pods_from_yaml'] += 1

                    # Extract container info
                    for container in spec.get('containers', []):
                        self.db.insert_container(
                            uid,
                            container.get('name'),
                            container.get('image')
                        )

            except Exception as e:
                print(f"    Error parsing {yaml_file}: {e}")
                self.stats['errors'] += 1

    def parse_events(self):
        """Parse K8s events.yaml files."""
        event_files = self.base_path.glob("namespaces/*/core/events.yaml")

        for event_file in event_files:
            try:
                with open(event_file, 'r') as f:
                    content = f.read()

                # Parse YAML documents (multiple events in one file)
                docs = yaml.safe_load_all(content)

                for doc in docs:
                    if not doc or doc.get('kind') != 'EventList':
                        continue

                    for item in doc.get('items', []):
                        self._parse_event_item(item, str(event_file.relative_to(self.base_path)))

            except Exception as e:
                print(f"    Error parsing {event_file}: {e}")
                self.stats['errors'] += 1

    def _parse_event_item(self, event: Dict, source_file: str):
        """Parse individual event item."""
        try:
            metadata = event.get('metadata', {})
            involved_obj = event.get('involvedObject', {})

            # Skip if not pod-related
            if involved_obj.get('kind') != 'Pod':
                return

            pod_uid = involved_obj.get('uid')
            namespace = involved_obj.get('namespace', metadata.get('namespace'))
            pod_name = involved_obj.get('name')

            # Get event time - try multiple fields
            event_time = (event.get('eventTime') or
                         event.get('lastTimestamp') or
                         event.get('firstTimestamp') or
                         metadata.get('creationTimestamp'))

            reason = event.get('reason')
            message = event.get('message')
            event_type = event.get('type')

            source = event.get('source', {})
            source_component = source.get('component')

            reporting_component = event.get('reportingComponent')
            if reporting_component:
                source_component = reporting_component

            # Extract node from message if present
            node = None
            if message and 'assigned' in message.lower():
                node_match = re.search(r'to (ip-[\w-]+\.[\w-]+\.compute\.internal)', message)
                if node_match:
                    node = node_match.group(1)

            # Ensure pod exists (auto-discover from events)
            if self.db.ensure_pod_exists(pod_uid, namespace, pod_name, node):
                self.stats['pods_from_events'] += 1

            self.db.insert_event(
                pod_uid, namespace, pod_name, event_time, reason, message,
                source.get('host'), source_component, node, event_type, source_file
            )
            self.stats['events'] += 1

        except Exception as e:
            print(f"    Error parsing event: {e}")
            self.stats['errors'] += 1

    def parse_scheduler_logs(self):
        """Parse kube-scheduler logs for scheduling decisions."""
        scheduler_logs = self.base_path.glob("namespaces/openshift-kube-scheduler/pods/*/kube-scheduler/kube-scheduler/logs/*.log")

        for log_file in scheduler_logs:
            try:
                with open(log_file, 'r') as f:
                    for line in f:
                        self._parse_scheduler_line(line, str(log_file.relative_to(self.base_path)))

            except Exception as e:
                print(f"    Error parsing {log_file}: {e}")
                self.stats['errors'] += 1

    def _parse_scheduler_line(self, line: str, source_file: str):
        """Parse individual scheduler log line for scheduling decisions."""
        # Format: 2026-09-18T21:10:49.848779631Z I0918 21:10:49.848732       1 schedule_one.go:314] "Successfully bound pod to node" pod="namespace/podname" node="nodename" evaluatedNodes=19 feasibleNodes=15

        # Look for schedule_one.go messages
        if 'schedule_one.go' not in line or 'Successfully bound pod to node' not in line:
            return

        # Extract timestamp
        ts_match = re.match(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)', line)
        if not ts_match:
            return
        timestamp = ts_match.group(1)

        # Extract pod (namespace/name format)
        pod_match = re.search(r'pod="([^/]+)/([^"]+)"', line)
        if not pod_match:
            return
        namespace, pod_name = pod_match.groups()

        # Extract node
        node_match = re.search(r'node="([^"]+)"', line)
        node = node_match.group(1) if node_match else None

        # Extract metrics
        eval_nodes_match = re.search(r'evaluatedNodes=(\d+)', line)
        feasible_nodes_match = re.search(r'feasibleNodes=(\d+)', line)

        metrics = []
        if eval_nodes_match:
            metrics.append(f"evaluated={eval_nodes_match.group(1)}")
        if feasible_nodes_match:
            metrics.append(f"feasible={feasible_nodes_match.group(1)}")

        message = f"Successfully bound pod to node {node}"
        if metrics:
            message += f" ({', '.join(metrics)})"

        # Ensure pod exists (auto-discover from scheduler logs)
        if self.db.ensure_pod_exists_by_name(namespace, pod_name, node):
            self.stats['pods_from_logs'] += 1

        # Record as log event
        self.db.insert_log_event(
            timestamp, None, pod_name, namespace, node or '',
            'scheduler', 'INFO', message, source_file
        )
        self.stats['log_events'] += 1

    def parse_kubelet_logs(self):
        """Parse kubelet journal logs."""
        kubelet_logs = self.base_path.glob("nodes/*/ip-*_logs_kubelet.gz")

        for log_file in kubelet_logs:
            node_name = log_file.parent.name

            try:
                with gzip.open(log_file, 'rt') as f:
                    for line in f:
                        self._parse_kubelet_line(line, node_name,
                                                str(log_file.relative_to(self.base_path)))

            except Exception as e:
                print(f"    Error parsing {log_file}: {e}")
                self.stats['errors'] += 1

    def _parse_kubelet_line(self, line: str, node: str, source_file: str):
        """Parse individual kubelet log line."""
        # Format: Apr 30 01:04:14.977055 ip-10-0-25-249 kubenswrapper[2501]: message
        match = re.match(r'(\w+ \d+ \d+:\d+:\d+\.\d+)\s+(\S+)\s+\S+\[(\d+)\]:\s+(.*)', line)
        if not match:
            return

        timestamp_str, hostname, pid, message = match.groups()

        # Look for pod-related messages
        pod_patterns = [
            (r'Pod "([^/]+)/([^"]+)"', 'pod_sync'),
            (r'pod="([^/]+)/([^"]+)"', 'pod_ref'),
            (r'SyncLoop.*ADD.*"([^/]+)/([^"]+)"', 'pod_add'),
            (r'Creating pod.*"([^/]+)/([^"]+)"', 'pod_create'),
        ]

        for pattern, event_type in pod_patterns:
            pod_match = re.search(pattern, message)
            if pod_match:
                namespace, pod_name = pod_match.groups()

                # Parse timestamp - must-gather format doesn't include year
                try:
                    # Assume current year from must-gather timestamp file
                    ts = datetime.strptime(timestamp_str, '%b %d %H:%M:%S.%f')
                    # Use 2026 based on the event times we saw
                    timestamp = f"2026-{ts.month:02d}-{ts.day:02d}T{ts.hour:02d}:{ts.minute:02d}:{ts.second:02d}.{ts.microsecond:06d}Z"
                except:
                    timestamp = timestamp_str

                # Extract log level
                log_level = 'INFO'
                if ' W' in message[:10]:
                    log_level = 'WARN'
                elif ' E' in message[:10]:
                    log_level = 'ERROR'

                # Ensure pod exists (auto-discover from logs)
                if self.db.ensure_pod_exists_by_name(namespace, pod_name, node):
                    self.stats['pods_from_logs'] += 1

                self.db.insert_log_event(
                    timestamp, None, pod_name, namespace, node,
                    'kubelet', log_level, message, source_file
                )
                self.stats['log_events'] += 1
                break

    def parse_crio_logs(self):
        """Parse CRI-O service logs."""
        crio_logs = self.base_path.glob("host_service_logs/*/crio_service.log")

        for log_file in crio_logs:
            try:
                with open(log_file, 'r') as f:
                    for line in f:
                        self._parse_crio_line(line, str(log_file.relative_to(self.base_path)))

            except Exception as e:
                print(f"    Error parsing {log_file}: {e}")
                self.stats['errors'] += 1

    def _parse_crio_line(self, line: str, source_file: str):
        """Parse individual CRI-O log line."""
        # Format: Apr 30 00:45:43.153110 ip-10-0-49-64 systemd[1]: message
        # Or: Apr 30 00:45:43.574251 ip-10-0-49-64 crio[2450]: time="..." level=info msg="..."

        match = re.match(r'(\w+ \d+ \d+:\d+:\d+\.\d+)\s+(\S+)\s+\S+\[(\d+)\]:\s+(.*)', line)
        if not match:
            return

        timestamp_str, hostname, pid, message = match.groups()

        # Parse structured log if present
        time_match = re.search(r'time="([^"]+)"', message)
        level_match = re.search(r'level=(\w+)', message)
        msg_match = re.search(r'msg="([^"]+)"', message)

        if time_match:
            timestamp = time_match.group(1)
        else:
            try:
                ts = datetime.strptime(timestamp_str, '%b %d %H:%M:%S.%f')
                timestamp = f"2026-{ts.month:02d}-{ts.day:02d}T{ts.hour:02d}:{ts.minute:02d}:{ts.second:02d}.{ts.microsecond:06d}Z"
            except:
                timestamp = timestamp_str

        log_level = level_match.group(1).upper() if level_match else 'INFO'
        log_msg = msg_match.group(1) if msg_match else message

        # Look for pod/container related messages
        if any(keyword in log_msg.lower() for keyword in ['container', 'pod', 'sandbox']):
            # Extract pod info if available
            pod_match = re.search(r'pod[_\s]+(\S+)', log_msg, re.IGNORECASE)
            pod_name = pod_match.group(1) if pod_match else None

            self.db.insert_log_event(
                timestamp, None, pod_name, '', hostname,
                'crio', log_level, log_msg, source_file
            )
            self.stats['log_events'] += 1


class PodLifecycleQuery:
    """Query interface for pod lifecycle data."""

    def __init__(self, db: PodLifecycleDB):
        self.db = db

    def list_pods(self, time_start: str = None, time_end: str = None,
                  namespace: str = None) -> List[Dict]:
        """List all pods, optionally filtered by time window and namespace."""
        cursor = self.db.conn.cursor()

        query = "SELECT * FROM pods WHERE 1=1"
        params = []

        if namespace:
            query += " AND namespace = ?"
            params.append(namespace)

        if time_start or time_end:
            # Join with events to filter by time
            query = """
                SELECT DISTINCT p.* FROM pods p
                JOIN events e ON p.uid = e.pod_uid
                WHERE 1=1
            """
            if namespace:
                query += " AND p.namespace = ?"
                params.append(namespace)
            if time_start:
                query += " AND e.event_time >= ?"
                params.append(time_start)
            if time_end:
                query += " AND e.event_time <= ?"
                params.append(time_end)

        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def get_pod_lifecycle(self, pod_uid: str = None, namespace: str = None,
                         pod_name: str = None) -> Dict:
        """Get complete lifecycle for a specific pod."""
        cursor = self.db.conn.cursor()

        # Get pod info
        if pod_uid:
            cursor.execute("SELECT * FROM pods WHERE uid = ?", (pod_uid,))
        elif namespace and pod_name:
            cursor.execute("SELECT * FROM pods WHERE namespace = ? AND name = ?",
                         (namespace, pod_name))
        else:
            return None

        pod = cursor.execute("SELECT * FROM pods WHERE uid = ? OR (namespace = ? AND name = ?)",
                           (pod_uid or '', namespace or '', pod_name or '')).fetchone()
        if not pod:
            return None

        pod_uid = pod['uid']

        # Get all events
        cursor.execute("""
            SELECT * FROM events
            WHERE pod_uid = ?
            ORDER BY event_time
        """, (pod_uid,))
        events = [dict(row) for row in cursor.fetchall()]

        # Get log events
        cursor.execute("""
            SELECT * FROM log_events
            WHERE pod_uid = ? OR (namespace = ? AND pod_name = ?)
            ORDER BY timestamp
        """, (pod_uid, pod['namespace'], pod['name']))
        log_events = [dict(row) for row in cursor.fetchall()]

        # Get containers
        cursor.execute("""
            SELECT * FROM containers WHERE pod_uid = ?
        """, (pod_uid,))
        containers = [dict(row) for row in cursor.fetchall()]

        return {
            'pod': dict(pod),
            'events': events,
            'log_events': log_events,
            'containers': containers
        }

    def detect_double_scheduling(self) -> List[Dict]:
        """Detect pods that may have been scheduled multiple times."""
        cursor = self.db.conn.cursor()

        # Find pods with multiple Scheduled events
        cursor.execute("""
            SELECT pod_uid, pod_name, namespace, COUNT(*) as schedule_count,
                   GROUP_CONCAT(event_time) as schedule_times,
                   GROUP_CONCAT(node) as nodes
            FROM events
            WHERE reason = 'Scheduled'
            GROUP BY pod_uid
            HAVING COUNT(*) > 1
        """)

        return [dict(row) for row in cursor.fetchall()]

    def get_scheduling_stats(self) -> Dict:
        """Get summary statistics on pod scheduling."""
        cursor = self.db.conn.cursor()

        stats = {}

        # Total pods
        cursor.execute("SELECT COUNT(*) as count FROM pods")
        stats['total_pods'] = cursor.fetchone()['count']

        # Pods by namespace
        cursor.execute("""
            SELECT namespace, COUNT(*) as count
            FROM pods
            GROUP BY namespace
            ORDER BY count DESC
        """)
        stats['pods_by_namespace'] = [dict(row) for row in cursor.fetchall()]

        # Event counts by reason
        cursor.execute("""
            SELECT reason, COUNT(*) as count
            FROM events
            GROUP BY reason
            ORDER BY count DESC
        """)
        stats['events_by_reason'] = [dict(row) for row in cursor.fetchall()]

        # Double-scheduled pods
        stats['double_scheduled'] = len(self.detect_double_scheduling())

        return stats

    def search_pods(self, time_start: str, time_end: str) -> List[Dict]:
        """Quick search for pods scheduled within time window."""
        cursor = self.db.conn.cursor()

        cursor.execute("""
            SELECT DISTINCT p.uid, p.namespace, p.name,
                   MIN(e.event_time) as first_event,
                   MAX(e.event_time) as last_event
            FROM pods p
            JOIN events e ON p.uid = e.pod_uid
            WHERE e.event_time >= ? AND e.event_time <= ?
            GROUP BY p.uid
            ORDER BY first_event
        """, (time_start, time_end))

        return [dict(row) for row in cursor.fetchall()]


class OutputFormatter:
    """Format query results for display."""

    @staticmethod
    def format_search_csv(pods: List[Dict]) -> str:
        """Format search results as CSV."""
        fieldnames = ['uid', 'namespace', 'name', 'first_event', 'last_event']
        output = io.StringIO(newline='')
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(pods)
        return output.getvalue()

    @staticmethod
    def format_pod_list_csv(pods: List[Dict]) -> str:
        """Format pod list results as CSV."""
        fieldnames = ['uid', 'namespace', 'name', 'node', 'created_at']
        output = io.StringIO(newline='')
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(pods)
        return output.getvalue()

    @staticmethod
    def format_timeline(lifecycle: Dict) -> str:
        """Format pod lifecycle as timeline."""
        output = []
        output.append(f"\n{'='*80}")
        output.append(f"Pod Lifecycle Timeline")
        output.append(f"{'='*80}")

        pod = lifecycle['pod']
        output.append(f"Pod: {pod['namespace']}/{pod['name']}")
        output.append(f"UID: {pod['uid']}")
        output.append(f"Node: {pod['node']}")
        output.append(f"Created: {pod['created_at']}")
        output.append(f"\n{'-'*80}")

        # Merge and sort all events
        all_events = []

        for event in lifecycle['events']:
            all_events.append({
                'time': event['event_time'],
                'source': 'K8s Event',
                'type': event['reason'],
                'message': event['message']
            })

        for event in lifecycle['log_events']:
            all_events.append({
                'time': event['timestamp'],
                'source': event['log_source'],
                'type': event['log_level'],
                'message': event['message'][:100]
            })

        all_events.sort(key=lambda x: x['time'] if x['time'] else '')

        output.append(f"{'Time':<28} {'Source':<12} {'Type':<15} {'Message':<50}")
        output.append(f"{'-'*80}")

        for event in all_events:
            output.append(f"{event['time']:<28} {event['source']:<12} {event['type']:<15} {event['message'][:45]}")

        if lifecycle['containers']:
            output.append(f"\n{'-'*80}")
            output.append("Containers:")
            for container in lifecycle['containers']:
                output.append(f"  - {container['container_name']}: {container['image']}")

        output.append(f"{'='*80}\n")
        return '\n'.join(output)

    @staticmethod
    def format_pod_list(pods: List[Dict]) -> str:
        """Format list of pods."""
        output = []
        output.append(f"\n{'Namespace':<40} {'Name':<50} {'Node':<35} {'Created':<28}")
        output.append(f"{'-'*155}")

        for pod in pods:
            node = pod.get('node') or 'N/A'
            created_at = pod.get('created_at') or 'N/A'
            output.append(f"{pod['namespace']:<40} {pod['name']:<50} {node:<35} {created_at:<28}")

        output.append(f"\nTotal: {len(pods)} pods\n")
        return '\n'.join(output)

    @staticmethod
    def format_stats(stats: Dict) -> str:
        """Format summary statistics."""
        output = []
        output.append(f"\n{'='*80}")
        output.append("Pod Scheduling Statistics")
        output.append(f"{'='*80}")

        output.append(f"\nTotal Pods: {stats['total_pods']}")
        output.append(f"Double-Scheduled Pods: {stats['double_scheduled']}")

        output.append(f"\nTop Namespaces:")
        for item in stats['pods_by_namespace'][:10]:
            output.append(f"  {item['namespace']:<50} {item['count']:>5} pods")

        output.append(f"\nEvent Breakdown:")
        for item in stats['events_by_reason'][:15]:
            output.append(f"  {item['reason']:<30} {item['count']:>8} events")

        output.append(f"{'='*80}\n")
        return '\n'.join(output)

    @staticmethod
    def format_double_scheduled(anomalies: List[Dict]) -> str:
        """Format double-scheduling anomalies."""
        output = []
        output.append(f"\n{'='*80}")
        output.append("Double-Scheduled Pods")
        output.append(f"{'='*80}\n")

        if not anomalies:
            output.append("No double-scheduled pods detected.\n")
        else:
            for item in anomalies:
                output.append(f"Pod: {item['namespace']}/{item['pod_name']}")
                output.append(f"  UID: {item['pod_uid']}")
                output.append(f"  Schedule Count: {item['schedule_count']}")
                output.append(f"  Times: {item['schedule_times']}")
                output.append(f"  Nodes: {item['nodes']}")
                output.append("")

        output.append(f"{'='*80}\n")
        return '\n'.join(output)


def main():
    parser = argparse.ArgumentParser(description='OpenShift Pod Lifecycle Tracker')
    parser.add_argument('must_gather_path', nargs='?', default='.',
                       help='Path to must-gather directory')
    parser.add_argument('--db', default='pod_lifecycle.db',
                       help='SQLite database file path')
    parser.add_argument('--parse', action='store_true',
                       help='Parse must-gather and populate database')
    parser.add_argument('--list', action='store_true',
                       help='List all pods')
    parser.add_argument('--stats', action='store_true',
                       help='Show summary statistics')
    parser.add_argument('--double-scheduled', action='store_true',
                       help='Show double-scheduled pods')
    parser.add_argument('--pod', help='Show lifecycle for specific pod (UID or namespace/name)')
    parser.add_argument('--namespace', help='Filter by namespace')
    parser.add_argument('--time-start', help='Start time filter (ISO format)')
    parser.add_argument('--time-end', help='End time filter (ISO format)')
    parser.add_argument('--search', action='store_true',
                       help='Quick search for pods in time window (requires --time-start and --time-end)')
    parser.add_argument('--csv', action='store_true',
                       help='Output --search or --list results as CSV')

    args = parser.parse_args()

    # Initialize database
    db = PodLifecycleDB(args.db)

    if args.parse:
        # Parse must-gather
        parser_obj = MustGatherParser(args.must_gather_path, db)
        parser_obj.parse_all()

    # Query interface
    query = PodLifecycleQuery(db)

    if args.search:
        if not args.time_start or not args.time_end:
            print("Error: --search requires --time-start and --time-end")
            return

        pods = query.search_pods(args.time_start, args.time_end)
        if args.csv:
            print(OutputFormatter.format_search_csv(pods), end='')
        else:
            print(f"\nFound {len(pods)} pods scheduled between {args.time_start} and {args.time_end}:")
            print(f"\n{'UID':<38} {'Namespace':<30} {'Name':<40} {'First Event':<28} {'Last Event':<28}")
            print(f"{'-'*165}")
            for pod in pods:
                print(f"{pod['uid']:<38} {pod['namespace']:<30} {pod['name']:<40} {pod['first_event']:<28} {pod['last_event']:<28}")
            print()

    elif args.list:
        pods = query.list_pods(args.time_start, args.time_end, args.namespace)
        if args.csv:
            print(OutputFormatter.format_pod_list_csv(pods), end='')
        else:
            print(OutputFormatter.format_pod_list(pods))

    elif args.stats:
        stats = query.get_scheduling_stats()
        print(OutputFormatter.format_stats(stats))

    elif args.double_scheduled:
        anomalies = query.detect_double_scheduling()
        print(OutputFormatter.format_double_scheduled(anomalies))

    elif args.pod:
        # Parse pod identifier (UID or namespace/name)
        if '/' in args.pod:
            namespace, name = args.pod.split('/', 1)
            lifecycle = query.get_pod_lifecycle(namespace=namespace, pod_name=name)
        else:
            lifecycle = query.get_pod_lifecycle(pod_uid=args.pod)

        if lifecycle:
            print(OutputFormatter.format_timeline(lifecycle))
        else:
            print(f"Pod not found: {args.pod}")

    else:
        parser.print_help()

    db.close()


if __name__ == '__main__':
    main()
