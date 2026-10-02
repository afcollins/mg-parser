# OpenShift Pod Lifecycle Tracker

Parses must-gather logs to track pod scheduling lifecycle across K8s events, kubelet logs, and CRI-O logs.

## Features

- Parse pod metadata, events, kubelet logs, crio logs into SQLite database
- Track full pod lifecycle: Scheduled → Pulling → Pulled → Created → Started → Ready
- Detect anomalies like double-scheduling
- Query pods by time window, namespace, or specific pod
- Multiple output formats: timeline, per-pod report, summary stats

## Setup

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### 1. Parse must-gather

```bash
python3 pod_lifecycle_tracker.py . --parse
```

This creates `pod_lifecycle.db` with all parsed data.

### Rebuild after parser changes or a repeated parse

Parsing adds events, log events, and container records to the database; it does
not replace previously parsed records. Before parsing the same must-gather again
or after updating this tool's parsers, remove the old database and rebuild it:

```bash
rm -f pod_lifecycle.db
python3 pod_lifecycle_tracker.py . --parse
```

If you use `--db`, remove that database file instead. This is required for the
kubelet filename-discovery change so the new files are included without retaining
the incomplete prior result set.

### 2. Query patterns

**Quick search - find pods scheduled in time window:**
```bash
python3 pod_lifecycle_tracker.py --search \
  --time-start "2026-04-30T01:30:00Z" \
  --time-end "2026-04-30T02:00:00Z"
```

For machine-readable results, add `--csv`:

```bash
python3 pod_lifecycle_tracker.py --search --csv \
  --time-start "2026-04-30T01:30:00Z" \
  --time-end "2026-04-30T02:00:00Z" > search-results.csv
```

**Get full lifecycle for specific pod:**
```bash
# By namespace/name
python3 pod_lifecycle_tracker.py --pod "openshift-ingress/router-default-6c9764f4c4-4xbxg"

# By UID
python3 pod_lifecycle_tracker.py --pod "abc123-uid-456"
```

**List all pods:**
```bash
# All pods
python3 pod_lifecycle_tracker.py --list

# Export all pods as CSV
python3 pod_lifecycle_tracker.py --list --csv > pods.csv

# Filter by namespace
python3 pod_lifecycle_tracker.py --list --namespace openshift-ingress

# Filter by time window
python3 pod_lifecycle_tracker.py --list \
  --time-start "2026-04-30T01:00:00Z" \
  --time-end "2026-04-30T02:00:00Z"
```

**Summary statistics:**
```bash
python3 pod_lifecycle_tracker.py --stats
```

**Detect double-scheduling:**
```bash
python3 pod_lifecycle_tracker.py --double-scheduled
```

## Workflow

1. **Quick time window search** - identify pods scheduled in specific time range
2. **Select pod** - pick specific pod from search results
3. **Full lifecycle** - view complete timeline with all events/logs
4. **Analyze anomalies** - check for double-scheduling or other issues

## Database Schema

- `pods` - Pod metadata (UID, namespace, name, node, created_at)
- `events` - K8s events (Scheduled, Pulling, Pulled, Created, Started)
- `log_events` - Kubelet/CRI-O log entries
- `containers` - Container info (name, image)

All queryable via SQLite:
```bash
sqlite3 pod_lifecycle.db "SELECT * FROM pods LIMIT 5"
```

## Example Output

### Timeline View
```
================================================================================
Pod Lifecycle Timeline
================================================================================
Pod: openshift-ingress/router-default-6c9764f4c4-4xbxg
UID: abc123-456-def
Node: ip-10-0-12-32.us-west-2.compute.internal
Created: 2026-04-30T01:34:23Z

--------------------------------------------------------------------------------
Time                         Source       Type            Message
--------------------------------------------------------------------------------
2026-04-30T01:34:23.000000Z K8s Event    Scheduled       Successfully assigned...
2026-04-30T01:34:24.000000Z K8s Event    Pulling         Pulling image "quay..."
2026-04-30T01:34:25.123456Z kubelet      INFO            SyncLoop ADD "openshift-ingress/router-d...
2026-04-30T01:34:28.000000Z K8s Event    Pulled          Successfully pulled image
2026-04-30T01:34:28.500000Z K8s Event    Created         Created container router
2026-04-30T01:34:29.000000Z K8s Event    Started         Started container router
================================================================================
```

### Double-Scheduling Detection
```
================================================================================
Double-Scheduled Pods
================================================================================

Pod: openshift-monitoring/prometheus-k8s-0
  UID: xyz789-abc-123
  Schedule Count: 2
  Times: 2026-04-30T01:30:00Z,2026-04-30T01:32:15Z
  Nodes: ip-10-0-25-249.us-west-2.compute.internal,ip-10-0-49-64.us-west-2.compute.internal

================================================================================
```
