#!/usr/bin/env python3
"""
Main test runner for DuckData tests.
"""
import os
import sys
import argparse
import subprocess
import pytest

def parse_args():
    parser = argparse.ArgumentParser(description="Run DuckData tests")
    parser.add_argument("--unit", action="store_true", help="Run unit tests")
    parser.add_argument("--integration", action="store_true", help="Run integration tests")
    parser.add_argument("--language", action="store_true", help="Run language-specific tests")
    parser.add_argument("--pipeline", action="store_true", help="Run pipeline tests")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    parser.add_argument("--python", action="store_true", help="Run Python tests only")
    parser.add_argument("--r", action="store_true", help="Run R tests only")
    parser.add_argument("--julia", action="store_true", help="Run Julia tests only")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    return parser.parse_args()

def check_language_available(language):
    """Check if a language is available."""
    if language == "python":
        return True
    
    if language == "r":
        try:
            result = subprocess.run(["Rscript", "--version"], 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE)
            return result.returncode == 0
        except:
            return False
            
    if language == "julia":
        try:
            result = subprocess.run(["julia", "--version"], 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE)
            return result.returncode == 0
        except:
            return False
            
    return False

def run_tests(args):
    """Run the specified tests."""
    # Set up pytest arguments
    pytest_args = ["-xvs"] if args.verbose else ["-xs"]
    
    # Determine which tests to run
    if args.all:
        print("Running all tests...")
        return pytest.main(pytest_args + ["unit_tests", "integration_tests", "language_tests", "pipeline_tests"])
    
    test_paths = []
    
    if args.unit:
        test_paths.append("unit_tests")
    
    if args.integration:
        test_paths.append("integration_tests")
    
    if args.language:
        language_paths = []
        
        if args.python or not (args.r or args.julia):
            language_paths.append("language_tests/python")
            
        if args.r and check_language_available("r"):
            language_paths.append("language_tests/r")
        elif args.r:
            print("WARNING: R not found. Skipping R tests.")
            
        if args.julia and check_language_available("julia"):
            language_paths.append("language_tests/julia")
        elif args.julia:
            print("WARNING: Julia not found. Skipping Julia tests.")
            
        test_paths.extend(language_paths)
    
    if args.pipeline:
        test_paths.append("pipeline_tests")
    
    # If no specific test type was selected, run all tests
    if not test_paths:
        print("No test type specified. Running all tests...")
        return pytest.main(pytest_args + ["unit_tests", "integration_tests", "language_tests", "pipeline_tests"])
    
    print(f"Running tests: {', '.join(test_paths)}")
    return pytest.main(pytest_args + test_paths)

if __name__ == "__main__":
    args = parse_args()
    sys.exit(run_tests(args)) 