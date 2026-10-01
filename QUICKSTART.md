# Quick Start Guide

## Setup (one time)

```bash
# Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Parse the must-gather archive
python3 pod_lifecycle_tracker.py . --parse
```

Database created: `pod_lifecycle.db` (372 pods, 5624 events, 127k log entries)

## Common Workflows

### 1. Find pods in specific time window

**Scenario**: Know when issue happened, want pods scheduled during that window.

```bash
# Quick search
python3 pod_lifecycle_tracker.py --search \
  --time-start "2026-04-30T01:34:00Z" \
  --time-end "2026-04-30T01:35:00Z"

# Output: table with UID, namespace, name, first/last event times
```

To export the matching pods as CSV:

```bash
python3 pod_lifecycle_tracker.py --search --csv \
  --time-start "2026-04-30T01:34:00Z" \
  --time-end "2026-04-30T01:35:00Z" > search-results.csv
```

From current data: **8 pods** scheduled between 01:34:00 and 01:35:00.

### 2. Trace specific pod lifecycle

**Scenario**: Suspect specific pod double-scheduled or slow startup.

```bash
# By namespace/name (easiest)
python3 pod_lifecycle_tracker.py --pod "openshift-ingress/router-default-6c9764f4c4-4xbxg"

# By UID (from search results)
python3 pod_lifecycle_tracker.py --pod "0eb2183a-30f2-4e8e-8d55-f2520b1f26a6"
```

Shows:
- Pod metadata (UID, node, created time)
- **Merged timeline** of K8s events + kubelet logs (sorted by timestamp)
- Container images
- Full flow: Scheduled → AddedInterface → Pulling → Pulled → Created → Started

### 3. Check for double-scheduling

**Scenario**: Suspect scheduler rescheduled pods.

```bash
python3 pod_lifecycle_tracker.py --double-scheduled
```

Finds pods with multiple `Scheduled` events (none in current data).

### 4. Overview statistics

```bash
python3 pod_lifecycle_tracker.py --stats
```

Shows:
- Total pods: **372**
- Pods per namespace (top 10)
- Event breakdown (Pulled: 1132, Created: 1107, Started: 1095, etc.)
- FailedScheduling count: **244 events**

### 5. Filter by namespace

```bash
# List all pods in namespace
python3 pod_lifecycle_tracker.py --list --namespace openshift-monitoring

# With time filter
python3 pod_lifecycle_tracker.py --list \
  --namespace openshift-monitoring \
  --time-start "2026-04-30T01:30:00Z" \
  --time-end "2026-04-30T02:00:00Z"
```

## Direct SQL Queries

Database schema allows custom queries:

```bash
sqlite3 pod_lifecycle.db

# Pods that failed scheduling
SELECT namespace, pod_name, COUNT(*) FROM events 
WHERE reason = 'FailedScheduling' 
GROUP BY pod_name 
ORDER BY COUNT(*) DESC;

# Scheduling latency (Scheduled → Started)
SELECT 
  e1.pod_name,
  e1.event_time as scheduled,
  e2.event_time as started,
  julianday(e2.event_time) - julianday(e1.event_time) as latency_days
FROM events e1
JOIN events e2 ON e1.pod_uid = e2.pod_uid
WHERE e1.reason = 'Scheduled' AND e2.reason = 'Started'
LIMIT 10;

# Image pull time
SELECT 
  e1.pod_name,
  e1.event_time as pull_start,
  e2.event_time as pull_end,
  (julianday(e2.event_time) - julianday(e1.event_time)) * 86400 as pull_seconds
FROM events e1
JOIN events e2 ON e1.pod_uid = e2.pod_uid
WHERE e1.reason = 'Pulling' AND e2.reason = 'Pulled'
ORDER BY pull_seconds DESC
LIMIT 10;
```

## Example: Full Investigation

```bash
# 1. Find when issues occurred (from stats)
python3 pod_lifecycle_tracker.py --stats
# Note: 244 FailedScheduling events

# 2. Find pods in problem window
python3 pod_lifecycle_tracker.py --search \
  --time-start "2026-04-30T01:30:00Z" \
  --time-end "2026-04-30T01:40:00Z"

# 3. Trace specific pod
python3 pod_lifecycle_tracker.py --pod "openshift-ingress/router-default-6c9764f4c4-4xbxg"

# 4. Check for double-scheduling
python3 pod_lifecycle_tracker.py --double-scheduled
```

## Parsers Included

✅ K8s events YAML (`namespaces/*/core/events.yaml`)  
✅ Pod metadata YAML (`namespaces/*/pods/*/`)  
✅ Kubelet logs (`nodes/*/ip-*_logs_kubelet.gz`)  
✅ CRI-O logs (`host_service_logs/*/crio_service.log`)

All correlated by pod UID/namespace/name.
