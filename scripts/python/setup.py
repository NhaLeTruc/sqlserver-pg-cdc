#!/usr/bin/env python3
"""
Setup script for CDC reconciliation tool

This script installs the reconciliation tool and its dependencies.

Usage:
    python setup.py install
    python setup.py develop  # For development mode
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README if it exists
readme_file = Path(__file__).parent.parent.parent / "README.md"
long_description = ""
if readme_file.exists():
    long_description = readme_file.read_text()

setup(
    name="cdc-reconciliation",
    version="1.0.0",
    description="Reconciliation tool for SQL Server to PostgreSQL CDC pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="CDC Pipeline Team",
    author_email="cdc-team@example.com",
    url="https://github.com/example/sqlserver-pg-cdc",
    packages=find_packages(where="../../src"),
    package_dir={"": "../../src"},
    install_requires=[
        "pyodbc>=4.0.39",
        "psycopg2-binary>=2.9.9",
        "APScheduler>=3.10.4",
        "requests>=2.31.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "black>=23.12.0",
            "flake8>=6.1.0",
            "mypy>=1.7.1",
        ]
    },
    entry_points={
        "console_scripts": [
            "cdc-reconcile=scripts.python.reconcile:main",
        ],
    },
    python_requires=">=3.11",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
    ],
)
