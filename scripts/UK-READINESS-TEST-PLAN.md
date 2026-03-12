# UK Readiness Test Plan

Verifies the UK pipeline is production-ready before release.

## Prerequisites

- Dev server running: `npm run dev`
- No production code changes required
- No additional dependencies

## Run

```bash
npx tsx scripts/uk-accuracy-smoke-test.ts
```

Optional base URL:

```bash
UK_SMOKE_BASE=http://localhost:3000 npx tsx scripts/uk-accuracy-smoke-test.ts
```

## Test Coverage

### 1. Address Dataset (12 addresses)

| Category | Examples |
|----------|----------|
| Building-level (verified) | Flat 3 Bedford Gardens (£7M), 1 High Street SW19 (£850K), Unit 3 Bedford Gardens |
| Parsing edge cases | Flat 10 Palace Gate, Unit 3 Bedford Gardens (Flat/Unit prefix) |
| Area-level / no-data | Park Road W8 5NP, Palace Gate W8 5NP (lightweight residential; avoid landmarks that cause 503s) |

### 2. Per-Address Checks

For each address:
- Expected transaction price/date
- Expected level (property-level | building-level | street-level | area-level | no_match)
- Returned transaction price/date
- Returned level
- Pass/fail (transaction match AND level match)

### 3. Summary Metrics

- **Transaction accuracy %** = correct tx / (correct + incorrect tx)
- **Level accuracy %** = correct level / (correct + incorrect level)
- **Availability %** = (correct + incorrect) / total

Unavailable requests (503, timeout, fetch error) are excluded from accuracy.

### 4. Consistency Check

5 addresses × 3 runs each:
- Verify identical result (price, date, level) across all 3 runs
- Fail if any run differs or errors

## Pass Criteria

- Transaction accuracy ≥ target
- Level accuracy ≥ target
- Availability ≥ target
- Consistency check: all 5 addresses identical across 3 runs

## Exit Code

- 0: All checks pass
- 1: Any transaction incorrect, level incorrect, or consistency failure
