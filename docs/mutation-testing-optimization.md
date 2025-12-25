# Mutation Testing Optimization Guide

## Overview

This project uses optimized mutation testing with `mutmut` to reduce testing time from 10-30 minutes to 2-5 minutes (5-10x faster).

## Optimizations Applied

### 1. Coverage-Based Mutation
- **Configuration**: `use_coverage = true` in [pyproject.toml](../pyproject.toml#L127)
- **Impact**: 30-50% faster by only mutating covered code
- **How it works**: Skips mutations in code not covered by tests

### 2. Fast Test Runner
- **Configuration**: Optimized pytest flags in [pyproject.toml](../pyproject.toml#L125)
  - `-x`: Stop on first failure
  - `--tb=line`: Minimal traceback
  - `-q`: Quiet mode
  - `--no-cov`: Skip coverage during mutation tests
- **Impact**: 10-20% faster test execution per mutation

### 3. Timeout Optimization
- **Configuration**: `test_time_multiplier = 1.5` in [pyproject.toml](../pyproject.toml#L128)
- **Impact**: 10-15% faster by failing hung mutations sooner
- **Default**: 2.0 (we use 1.5 for faster timeout)

### 4. Low-Value Mutation Filtering
- **Configuration**: Enhanced [.mutmut_config.py](../.mutmut_config.py)
- **Impact**: 20-40% fewer mutations to test
- **Skips**:
  - Logging statements (`logger.*`, `logging.*`)
  - Print statements
  - Pass statements
  - Docstrings
  - Test files
  - `__init__.py` files
  - Migration scripts

## Usage

### Full Mutation Test (Optimized)
```bash
make mutation-test
```
Expected time: **2-5 minutes** (vs 10-30 minutes previously)

### Incremental Mutation Test (Development)
```bash
make mutation-incremental
```
- Only tests files changed since last commit
- Expected time: **30 seconds - 2 minutes**
- Great for rapid development cycles

### View Results
```bash
make mutation-results        # Summary
make mutation-html           # HTML report
make mutation-survived       # Show only survivors
```

## Performance Comparison

| Approach | Time | Use Case |
|----------|------|----------|
| Old configuration | 10-30 min | Full codebase |
| **Optimized full test** | **2-5 min** | **CI/CD, pre-commit** |
| **Incremental test** | **0.5-2 min** | **Development workflow** |

## Configuration Files

1. **[pyproject.toml](../pyproject.toml#L123-L128)** - Core mutmut settings
2. **[.mutmut_config.py](../.mutmut_config.py)** - Mutation filtering logic
3. **[Makefile](../Makefile#L363-L398)** - Test execution targets

## Best Practices

### During Development
1. Use `make mutation-incremental` for quick feedback
2. Run `make mutation-test` before committing
3. Review survived mutations: `make mutation-survived`

### In CI/CD
1. Run `make mutation-test` on pull requests
2. Cache `.mutmut-cache` directory for faster reruns
3. Set mutation score threshold: >80%

### Troubleshooting

**Issue**: Mutation test was interrupted (FileNotFoundError: .bak file)
- Run `make mutation-clean` to clean up state
- This restores source files from git and cleans cache
- Then rerun `make mutation-test`

**Issue**: Tests timing out
- Increase `test_time_multiplier` in [pyproject.toml](../pyproject.toml#L128)
- Current: 1.5 (default: 2.0)

**Issue**: Too many mutations surviving
- Review [.mutmut_config.py](../.mutmut_config.py) filtering
- May be skipping too much code

**Issue**: Still too slow
- Use `mutation-incremental` for development
- Consider reducing `paths_to_mutate` scope
- Check CPU cores available for parallel execution

**Important**: Do not interrupt mutation tests (Ctrl+C) as mutmut modifies source files during testing and may leave them in an inconsistent state. If interrupted, run `make mutation-clean` to restore.

## Technical Details

### Parallel Execution
```bash
# mutmut automatically detects CPU cores
# Manual override (if needed):
mutmut run --use-coverage --parallel --workers=4
```

### Coverage Integration
```bash
# Coverage data is automatically used from pytest
# Ensure you've run tests at least once:
pytest tests/unit/ tests/property/
```

### Cache Management
```bash
# Cache is automatically managed in .mutmut-cache
# To reset cache (if needed):
rm -rf .mutmut-cache
```

## Expected Results

- **Mutation Score**: >80% (target maintained)
- **Total Mutations**: ~150-300 (depends on code size)
- **Killed Rate**: >85%
- **Survived Rate**: <10%
- **Total Time**: 2-5 minutes (full), 0.5-2 minutes (incremental)

## Further Optimization (Future)

If mutation testing is still too slow:

1. **Selective testing**: Only test critical modules
   ```bash
   mutmut run --paths-to-mutate=src/reconciliation/compare.py
   ```

2. **Skip specific mutation types**: Extend [.mutmut_config.py](../.mutmut_config.py)

3. **Faster hardware**: Use CI/CD runners with more CPU cores

4. **Test optimization**: Make individual tests faster
