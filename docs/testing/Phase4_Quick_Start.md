# Phase 4: Testing Infrastructure - Quick Start Guide

Get started with Phase 4 testing infrastructure in 5 minutes.

## Prerequisites

```bash
# Install Python 3.11+
python --version  # Should be 3.11 or higher

# Install development dependencies
pip install -e ".[dev]"
```

## Quick Test Commands

### 1. Property-Based Tests (2 minutes)

```bash
# Run with default settings (fast)
pytest tests/property/ -v

# Expected output:
# tests/property/test_reconciliation_properties.py::test_row_count_comparison_properties PASSED
# tests/property/test_reconciliation_properties.py::test_checksum_deterministic PASSED
# ...
# Hypothesis: 20 examples generated per test
```

### 2. Mutation Tests (10-15 minutes)

```bash
# Run mutation testing
make mutation-test

# Expected output:
# Running mutation tests...
# Progress: [████████████████████] 100%
# Results: 45 killed, 3 survived, 0 timeout
# Mutation Score: 93.8%
```

### 3. Load Tests (2 minutes)

```bash
# Option A: Headless mode (automated)
make test-load

# Option B: Web UI mode (interactive)
make load-test-ui
# Then open http://localhost:8089
```

## What Each Test Does

### Property-Based Tests
✓ Validates mathematical properties (e.g., symmetry, associativity)
✓ Tests edge cases automatically
✓ Verifies SQL injection protection
✓ Checks checksum consistency

**Example property:**
```python
# Property: Same data should produce same checksum
checksum1 = hash(data)
checksum2 = hash(data)
assert checksum1 == checksum2  # ✓ Always true
```

### Mutation Tests
✓ Validates test suite effectiveness
✓ Finds gaps in test coverage
✓ Introduces bugs to verify tests catch them

**Example mutation:**
```python
# Original code:
if count > 0:
    return True

# Mutation:
if count >= 0:  # Changed > to >=
    return True

# If tests still pass, mutation "survived" → need better tests
```

### Load Tests
✓ Measures performance under load
✓ Identifies bottlenecks
✓ Tests scalability limits

**Test scenarios:**
- 50 concurrent API requests
- Large table reconciliation
- Connection pool stress testing

## Understanding Results

### Property Test Output

```
Hypothesis: Generated 20 examples
✓ test_row_count_comparison_properties PASSED

This means:
- Hypothesis tried 20 different inputs
- All 20 satisfied the property
- Test passed ✓
```

If a property fails:
```
Falsifying example: test_foo(x=0, y=-1)

This means:
- Hypothesis found x=0, y=-1 breaks the property
- Add this as explicit test case
```

### Mutation Test Results

```
Mutation Score: 93.8%
- Killed: 45    ✓ Tests caught the bug
- Survived: 3   ⚠️ Need more tests
- Timeout: 0

Good score! (Target: >80%)
```

To see survived mutations:
```bash
make mutation-survived
```

### Load Test Metrics

```
Type    Name                    # Reqs  Fails  Avg(ms)  P95(ms)
GET     /connectors             5234    0      45       89
GET     /connectors/{id}/status 3145    2      67       134

This means:
- 5234 requests to /connectors
- 0 failures (100% success rate) ✓
- Average response: 45ms
- 95th percentile: 89ms (fast!)
```

## Common Issues & Fixes

### Issue: Import errors

```bash
# Fix: Install dependencies
pip install -e ".[dev]"
```

### Issue: Mutation tests too slow

```bash
# Fix: Test specific module only
mutmut run --paths-to-mutate=src/reconciliation/compare.py
```

### Issue: Load tests failing

```bash
# Fix: Ensure services are running
make start
make verify-services

# Then retry
make test-load
```

### Issue: Property tests timing out

```bash
# Fix: Use faster profile
pytest tests/property/ --hypothesis-profile=dev
```

## Integration with CI/CD

Tests automatically run on:
- Pull requests
- Pushes to main
- Nightly builds

See results in GitHub Actions tab.

## Next Steps

1. ✅ Run all three test types
2. Review mutation survivors and add tests
3. Establish performance baselines
4. Add to CI/CD pipeline
5. Monitor metrics over time

## Full Documentation

For detailed information, see:
- [Complete Guide](./Phase4_Testing_Infrastructure_Guide.md)
- [Tests README](../../tests/README.md)
- [Implementation Plan](../opportunities_Implementation_plan.md)

## Help & Support

```bash
# List all test commands
make help | grep test

# View test markers
pytest --markers

# Run specific test
pytest tests/property/test_reconciliation_properties.py::test_specific -v
```

---

**Time Investment:** 5 minutes setup, 15 minutes first run
**Value:** Catch bugs before production, validate test quality, ensure performance
**Status:** ✅ Ready to use
