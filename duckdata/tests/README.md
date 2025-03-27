# DuckData Test Suite

This directory contains a comprehensive test suite for DuckData, covering various components and cross-language functionality.

## Test Structure

The test suite is organized into the following directories:

- **unit_tests/** - Tests for individual components
- **integration_tests/** - Tests for component interactions
- **language_tests/** - Language-specific tests
  - **python/** - Tests for Python integration
  - **r/** - Tests for R integration
  - **julia/** - Tests for Julia integration
- **pipeline_tests/** - Tests for complete pipelines
- **test_data/** - Test data generation scripts and data files
- **fixtures/** - Test fixtures and utilities

## Running Tests

### Prerequisites

Make sure you have installed DuckData and its dependencies:

```bash
# Navigate to the root directory
cd duckdata
./install.sh
```

### Running the Test Suite

To run the entire test suite:

```bash
cd duckdata/tests
python run_tests.py --all
```

### Running Specific Tests

You can run specific types of tests using these flags:

```bash
# Run only unit tests
python run_tests.py --unit

# Run only language-specific tests
python run_tests.py --language

# Run pipeline tests
python run_tests.py --pipeline

# Run only Python tests
python run_tests.py --language --python

# Run only R tests
python run_tests.py --language --r

# Run only Julia tests
python run_tests.py --language --julia
```

### Verbose Output

For more detailed output, add the `-v` or `--verbose` flag:

```bash
python run_tests.py --all --verbose
```

## Language Availability

The test runner will automatically detect which languages are available on your system and skip tests for unavailable languages. The following languages are used in the test suite:

- **Python**: Always available (used to run the tests)
- **R**: Optional, used for testing R integration
- **Julia**: Optional, used for testing Julia integration

## Test Data Generation

The `test_data/` directory contains scripts for generating test datasets used in the tests. You can generate fresh test data by running:

```bash
python test_data/generate_test_data.py
```

## Adding New Tests

When adding new tests:

1. Place them in the appropriate directory based on the type of test
2. Follow the naming convention: `test_*.py`
3. Use the provided fixtures from `conftest.py`
4. Make sure tests can handle missing languages gracefully

## Testing Multi-language Pipelines

The pipeline tests demonstrate how DuckData enables seamless data sharing between languages. Each language performs different operations on the data:

- **Python**: Data generation and final reporting
- **R**: Data transformation and feature engineering
- **Julia**: Statistical analysis and modeling

If a language is not available, the tests will use Python fallbacks to simulate the missing functionality. 