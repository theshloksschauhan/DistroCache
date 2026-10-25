# Consistency checker — recorded runs

Machine: Windows 11, Python 3.12.4, 3-node localhost cluster (N=3), run 2026-10-25.
Reproduce with `python -m tests.integration.checker.runner` (see flags below). See
[docs/design/0008-consistency-checker.md](../design/0008-consistency-checker.md) for the checks.

## R=2/W=2 (R+W>N) — expected CLEAN

Three 120-second runs under the nemesis (kills/restarts, ≤ N−W = 1 node down at a time). Every
check passes in every run.

```
python -m tests.integration.checker.runner --duration 120 --read-quorum 2 --write-quorum 2 --seed <s>
```

| Run (seed) | Operations | Nemesis actions | Statuses (200/404/503) | Convergence | Report |
|------------|-----------:|----------------:|------------------------|-------------|--------|
| 1 (seed 1) | 10,776 | 62 | 9452 / 1068 / 256 | after 1 pass | **CLEAN** |
| 2 (seed 2) | 12,142 | 66 | 10690 / 1212 / 240 | after 1 pass | **CLEAN** |
| 3 (seed 3) |  9,055 | 71 | 7771 / 868 / 416 | after 1 pass | **CLEAN** |

All five checks PASS in all three runs: no acked write lost, no fabricated read, read-your-writes,
no resurrected delete, replicas converge. (503s are clean below-quorum refusals during the brief
window after a kill, not errors; there were zero non-503 5xx.)

## R=1/W=1 (R+W≤N) — expected VIOLATIONS (proves the checker can fail)

With R+W ≤ N the quorum-overlap guarantee does not hold, so the checker is expected to catch
violations. This is the end-to-end proof that the checker can fail (the deterministic proof is the
hand-built violating histories in `tests/test_checker.py`).

```
python -m tests.integration.checker.runner --duration 90 --read-quorum 1 --write-quorum 1 --seed 301
```

Example run (seed 301, 90s): 8000 operations, 49 nemesis actions.

```
  [PASS] no acked write lost  (examined 5)
  [PASS] no fabricated read  (examined 3113)
  [FAIL (11)] read-your-writes  (examined 3932)
        - key-2: read returned (...011769, 2), older than acked event (...022168, 0)
                 that committed before the read started
        - key-1: read returned (...012641, 0), older than acked event (...029400, 0) ...
        ... 9 more
  [FAIL (11)] no resurrected delete  (examined 3104)
        - key-2: read returned value at (...011769, 2) <= acked delete (...021042, 0)
                 that completed before the read -- the delete was resurrected
        ... 10 more
  [PASS] replicas converge  (examined 5)

REPORT: VIOLATIONS FOUND
```

This is probabilistic: DistroCache's background replication is fast, so a stale read only appears
when a read happens to hit a not-yet-replicated replica. About four runs in five report violations
(measured over 5 seeds: 4 reported read-your-writes / resurrection / lost-write violations, 1 was
clean). The stale-read and resurrected-delete violations are exactly the read-your-writes guarantee
breaking without quorum overlap.
