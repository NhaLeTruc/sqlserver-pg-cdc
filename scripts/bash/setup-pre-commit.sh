#!/bin/bash
# Pre-commit hooks installation script
# Installs and configures pre-commit hooks for code quality checks

set -euo pipefail

# Colors for output
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Installing pre-commit hooks...${NC}"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed${NC}"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo -e "${BLUE}Using Python ${PYTHON_VERSION}${NC}"

# Install pre-commit if not already installed
if ! command -v pre-commit &> /dev/null; then
    echo -e "${YELLOW}pre-commit not found, installing...${NC}"
    pip3 install pre-commit
else
    echo -e "${GREEN}✓ pre-commit already installed${NC}"
fi

# Install pre-commit hooks
echo -e "${BLUE}Installing pre-commit git hooks...${NC}"
pre-commit install

# Also install commit-msg hooks
echo -e "${BLUE}Installing commit-msg hooks...${NC}"
pre-commit install --hook-type commit-msg || echo -e "${YELLOW}⚠ commit-msg hooks not configured${NC}"

# Install pre-push hooks (optional)
echo -e "${BLUE}Installing pre-push hooks...${NC}"
pre-commit install --hook-type pre-push || echo -e "${YELLOW}⚠ pre-push hooks not configured${NC}"

# Run hooks on all files to ensure they work
echo -e "${BLUE}Running pre-commit on all files (this may take a minute)...${NC}"
if pre-commit run --all-files; then
    echo -e "${GREEN}✓ All pre-commit hooks passed!${NC}"
else
    echo -e "${YELLOW}⚠ Some hooks failed or made changes. Review the changes and commit them.${NC}"
    echo -e "${YELLOW}  You can run 'pre-commit run --all-files' again to verify.${NC}"
fi

echo ""
echo -e "${GREEN}✓ Pre-commit hooks installed successfully!${NC}"
echo ""
echo -e "${BLUE}Hooks will now run automatically before each commit.${NC}"
echo -e "${BLUE}To run hooks manually: pre-commit run --all-files${NC}"
echo -e "${BLUE}To skip hooks (emergency only): git commit --no-verify${NC}"
echo ""
