# Session Handoff Document

**Date:** 2026-09-30
**Context:** OpenShift must-gather pod lifecycle analysis tool

## What Was Built

**`pod_lifecycle_tracker.py`** - Complete pod lifecycle analysis tool for OpenShift must-gather archives.

### Core Features

1. **Multi-source pod discovery:**
   - YAML files (namespaces/*/pods/*/)
   - K8s events (events.yaml) - auto-discovers pods with UIDs
   - Kubelet logs - auto-discovers from log references (synthetic UIDs)
   - **Scheduler logs** - parses `schedule_one.go` messages for scheduling decisions

2. **SQLite database schema:**
   - `pods` - metadata (UID, namespace, name, node, created_at, yaml_path)
   - `events` - K8s events (Scheduled, Pulling, Pulled, Created, Started, etc.)
   - `log_events` - kubelet/crio/scheduler log entries
   - `containers` - container names and images

3. **Query capabilities:**
   - Time window search (quick pod list)
   - Full pod lifecycle (merged timeline across all sources)
   - Double-scheduling detection
   - Summary statistics
   - Namespace filtering

4. **Output formats:**
   - Timeline view (timestamp-sorted events + logs)
   - Pod list tables
   - Statistics reports
   - Anomaly detection

## Key Discoveries

### Test Data (Current Must-Gather)

Parsed from: `/Users/ancollin/Downloads/prow/pr-logs/pull/openshift_ovn-kubernetes/3166/...`

**Results:**
- **3,201 total pods** discovered:
  - 372 from YAML files
  - 163 auto-discovered from events (have UID but no YAML)
  - 2,666 auto-discovered from logs (workload pods, no YAML captured)
- 5,624 K8s events
- 130,270 log entries
- 0 parse errors

**Key insight:** Log-based discovery found **7x more pods** than YAML alone. Critical for workload pod analysis.

### Pod Discovery Methods

1. **YAML (372 pods):** Complete metadata, containers, status
2. **Events (163 pods):** Have real UIDs, partial metadata from events
3. **Logs (2,666 pods):** Synthetic UIDs (hash of namespace/name), discovered from:
   - Kubelet logs: `Pod "namespace/name"` references
   - **Scheduler logs:** `schedule_one.go` "Successfully bound pod to node" messages

### Scheduler Log Format

```
2026-09-18T21:10:49.848779631Z I0918 21:10:49.848732 1 schedule_one.go:314] "Successfully bound pod to node" pod="namespace/podname" node="nodename" evaluatedNodes=19 feasibleNodes=15
```

Parser extracts:
- Pod (namespace/name)
- Node assigned
- Metrics (evaluatedNodes, feasibleNodes)

### Node Naming Patterns

**Generic regex now handles:**
- AWS: `ip-10-0-25-249.us-west-2.compute.internal`
- Generic: `w000.subdomain.domain.cluster.io`
- Any: `[\w.-]+(?:\.[\w.-]+)*`

## Current State

### Repository

**Location:** `~/go/src/github.com/afcollins/mg-parser/`

**Files:**
- `pod_lifecycle_tracker.py` - Main tool (updated with scheduler parsing + generic node regex)
- `requirements.txt` - PyYAML>=6.0
- `README.md` - Full documentation
- `QUICKSTART.md` - Common workflows
- `HANDOFF.md` - This file
- `pod_lifecycle_tracker.py.bak` - Backup before scheduler log additions

**Git status:** Files staged, ready to commit

### Test Database

**Location:** Must-gather dir (not in repo)
**File:** `pod_lifecycle.db` (SQLite)
**Size:** Contains parsed data from test must-gather

## Usage Examples

### Setup
```bash
cd ~/go/src/github.com/afcollins/mg-parser
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Parse Must-Gather
```bash
python3 pod_lifecycle_tracker.py /path/to/must-gather --parse
```

### Query Patterns

**Find pods in time window:**
```bash
python3 pod_lifecycle_tracker.py --search \
  --time-start "2026-04-30T01:34:00Z" \
  --time-end "2026-04-30T01:35:00Z"
```

**Full pod lifecycle:**
```bash
# By namespace/name
python3 pod_lifecycle_tracker.py --pod "openshift-ingress/router-default-6c9764f4c4-4xbxg"

# By UID
python3 pod_lifecycle_tracker.py --pod "0eb2183a-30f2-4e8e-8d55-f2520b1f26a6"
```

**Statistics:**
```bash
python3 pod_lifecycle_tracker.py --stats
```

**Check anomalies:**
```bash
python3 pod_lifecycle_tracker.py --double-scheduled
```

## Architecture Decisions

### Synthetic UIDs for Log-Discovered Pods

**Problem:** Logs have namespace/name but no UID. Need unique identifier.

**Solution:** Generate synthetic UID from hash:
```python
synthetic_uid = f"log-{hashlib.sha256(f'{namespace}/{name}'.encode()).hexdigest()[:32]}"
```

**Rationale:**
- Deterministic (same pod always gets same UID)
- Collision-resistant (SHA256)
- Distinguishable (prefix `log-`)
- Compatible with real UIDs (same length, format)

### Three-Tier Discovery

1. **YAML first:** Most complete data, real UIDs
2. **Events second:** Real UIDs, partial metadata, fills gaps
3. **Logs last:** Synthetic UIDs, minimal metadata, catches workload pods

Each tier calls `ensure_pod_exists()` or `ensure_pod_exists_by_name()` - idempotent, won't duplicate.

### Log Parser Priority

**Scheduler logs parsed BEFORE kubelet logs:**
- Scheduler has cleaner format (structured pod/node binding)
- Includes metrics (evaluatedNodes, feasibleNodes)
- Runs less frequently (only on scheduling), faster parse

## Known Limitations

1. **Scheduler logs optional:** Not all clusters have `schedule_one.go` logging enabled
2. **Synthetic UIDs:** Can't join with real K8s events (different UID)
3. **Log timestamp parsing:** Kubelet logs lack year, assumes 2026 (hardcoded)
4. **No API server logs:** Could add more pod discovery sources
5. **No container lifecycle:** Tracks pod-level, not individual container restarts

## Next Steps

### Immediate (Ready to Commit)

1. **Update repo files:**
   ```bash
   cd ~/go/src/github.com/afcollins/mg-parser
   git add pod_lifecycle_tracker.py HANDOFF.md
   git status  # Verify: tracker, requirements, README, QUICKSTART, HANDOFF
   ```

2. **Create initial commit:**
   ```bash
   git commit -m "Initial commit: OpenShift pod lifecycle tracker

   - Parse must-gather: YAML, events, kubelet, crio, scheduler logs
   - SQLite backend with pods, events, log_events, containers tables
   - Auto-discover pods from events (real UIDs) and logs (synthetic UIDs)
   - Query: time window search, full lifecycle, double-scheduling detection
   - Scheduler log parsing: schedule_one.go 'Successfully bound' messages
   - Generic node name regex (AWS + non-AWS formats)

   Test results: 3201 pods discovered (372 YAML + 163 events + 2666 logs)"
   ```

### Enhancements

1. **API server logs:** Parse for pod creation requests
2. **Container-level tracking:** Track individual container restarts/failures
3. **Latency metrics:** Calculate scheduling→started, pulling→pulled times
4. **Export formats:** JSON, CSV output
5. **Web UI:** Flask/Django dashboard for visualization
6. **Diff mode:** Compare two must-gathers (before/after)

### Testing

1. **Other must-gather formats:** Test on different cluster versions
2. **Large datasets:** Profile performance on >10k pod clusters
3. **Edge cases:** Pods with no events, failed scheduling, preemption

## Database Schema

```sql
-- Pods table
CREATE TABLE pods (
    uid TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    node TEXT,
    created_at TEXT,
    yaml_path TEXT
);

-- Events table
CREATE TABLE events (
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
);

-- Log events table
CREATE TABLE log_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    pod_uid TEXT,
    pod_name TEXT,
    namespace TEXT,
    node TEXT,
    log_source TEXT,  -- 'kubelet', 'crio', 'scheduler'
    log_level TEXT,
    message TEXT,
    source_file TEXT
);

-- Containers table
CREATE TABLE containers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pod_uid TEXT,
    container_name TEXT,
    image TEXT,
    FOREIGN KEY (pod_uid) REFERENCES pods(uid)
);
```

## Direct SQL Examples

```sql
-- Pods missing from YAML but in events
SELECT * FROM pods WHERE yaml_path IS NULL AND uid NOT LIKE 'log-%';

-- Pods only discovered from logs
SELECT * FROM pods WHERE uid LIKE 'log-%';

-- Scheduling latency
SELECT
  e1.pod_name,
  julianday(e2.event_time) - julianday(e1.event_time) AS latency_days
FROM events e1
JOIN events e2 ON e1.pod_uid = e2.pod_uid
WHERE e1.reason = 'Scheduled' AND e2.reason = 'Started'
ORDER BY latency_days DESC;

-- Scheduler decisions
SELECT * FROM log_events WHERE log_source = 'scheduler';

-- Failed scheduling attempts
SELECT namespace, pod_name, COUNT(*)
FROM events
WHERE reason = 'FailedScheduling'
GROUP BY pod_name
ORDER BY COUNT(*) DESC;
```

## Questions for Next Session

1. **Output format preferences?** JSON export? CSV? Prometheus metrics?
2. **Performance requirements?** Need streaming parser for huge archives?
3. **Integration targets?** CI/CD pipeline? Monitoring dashboard?
4. **Additional log sources?** API server audit logs? etcd logs?
5. **Time window heuristics?** Auto-detect "interesting" time periods?

## Context for New Session

**Primary use case:** Trace pod scheduling flow across must-gather logs to debug double-scheduling, slow starts, failed scheduling.

**Key constraint:** Workload pods often not captured in YAML - must reverse-engineer from logs.

**Success metric:** Can trace any pod (even ephemeral) through full lifecycle from first log mention to termination.

**Test command:**
```bash
cd ~/go/src/github.com/afcollins/mg-parser
source venv/bin/activate
python3 pod_lifecycle_tracker.py /path/to/must-gather --parse
python3 pod_lifecycle_tracker.py --stats
python3 pod_lifecycle_tracker.py --pod "namespace/pod-name"
```

## Files to Commit

```
~/go/src/github.com/afcollins/mg-parser/
├── pod_lifecycle_tracker.py    # Main tool (staged)
├── requirements.txt             # PyYAML (staged)
├── README.md                    # Documentation (staged)
├── QUICKSTART.md               # Workflows (staged)
└── HANDOFF.md                  # This file (needs staging)
```

**Excluded from git:**
- `venv/` (virtual environment)
- `*.db` (SQLite databases)
- `*.pyc` (Python bytecode)
- `__pycache__/` (cache)

**Recommend `.gitignore`:**
```
venv/
*.db
*.pyc
__pycache__/
.DS_Store
*.bak
```
