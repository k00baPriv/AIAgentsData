#!/usr/bin/env bash
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

REPORT_DIR="${ROOT_DIR}/reports"
mkdir -p "$REPORT_DIR"
FAILURES=0
FAILED_STEPS=()

explain_step() {
  local text="$1"
  echo "  -> $text"
}

run_step() {
  local label="$1"
  shift
  echo "$label"
  if ! "$@"; then
    FAILURES=$((FAILURES + 1))
    FAILED_STEPS+=("$label")
    echo "Step failed: $label"
  fi
}

echo "[0/14] Improve formating"
explain_step "Applies canonical style so diffs stay clean and linting starts from a normalized codebase."
python -m ruff format .

echo "[1/14] Environment check"
explain_step "Prints the active Python interpreter version to confirm runtime compatibility."
python --version

echo "[2/14] Dependency check (dev tools)"
explain_step "Verifies all QA tools are installed in the current environment."
MISSING=0
for pkg in pytest coverage mypy ruff bandit radon xenon vulture pip-audit semgrep; do
  if ! python -m pip show "$pkg" >/dev/null 2>&1; then
    echo "Missing package: $pkg"
    MISSING=1
  fi
done
if [[ "$MISSING" -ne 0 ]]; then
  echo "Install dev dependencies with:"
  echo "  python -m pip install pytest coverage mypy ruff bandit radon xenon vulture pip-audit semgrep"
  exit 1
fi

echo "[3/14] Unit tests"
explain_step "Checks functional correctness of the code using the project test suite."
run_step "[3/14] Unit tests" python -m pytest tests -q

echo "[4/14] Coverage run"
explain_step "Measures how much production code is exercised by tests and exports machine-readable reports."
run_step "[4/14] Coverage run (test execution)" python -m coverage run -m pytest tests -q
run_step "[4/14] Coverage run (terminal report)" python -m coverage report -m
run_step "[4/14] Coverage run (xml report)" python -m coverage xml -o "$REPORT_DIR/coverage.xml"
run_step "[4/14] Coverage run (html report)" python -m coverage html -d "$REPORT_DIR/htmlcov"

echo "[5/14] Mypy"
explain_step "Performs static type analysis to catch interface and data-flow mismatches before runtime."
run_step "[5/14] Mypy" python -m mypy src tests

echo "[6/14] Ruff lint"
explain_step "Finds lint issues like import order problems, likely bugs, and style violations."
run_step "[6/14] Ruff lint" python -m ruff check src tests

echo "[7/14] Ruff format check"
explain_step "Verifies formatting is already compliant without mutating files."
run_step "[7/14] Ruff format check" python -m ruff format --check src tests

echo "[8/14] Bandit security scan"
explain_step "Scans source for insecure coding patterns (hardcoded secrets, unsafe calls, weak crypto usage)."
run_step "[8/14] Bandit security scan" python -m bandit -q -r src tests -f txt -o "$REPORT_DIR/bandit.txt"

echo "[9/14] Complexity check (radon cc)"
explain_step "Reports cyclomatic complexity per function/method to identify decision-heavy hotspots."
run_step "[9/14] Complexity check (radon cc)" python -m radon cc -s -a src tests

echo "[10/14] Maintainability check (radon mi)"
explain_step "Calculates maintainability index to estimate long-term readability and change cost."
run_step "[10/14] Maintainability check (radon mi)" python -m radon mi -s src tests

echo "[11/14] Complexity gate (xenon)"
explain_step "Fails QA if complexity thresholds are exceeded to prevent gradual architecture decay."
run_step "[11/14] Complexity gate (xenon)" python -m xenon --max-absolute B --max-modules B --max-average A src tests

echo "[12/14] Dead code check (vulture)"
explain_step "Detects likely unused functions, classes, and variables to reduce maintenance noise."
run_step "[12/14] Dead code check (vulture)" python -m vulture src tests --min-confidence 80

echo "[13/14] Dependency vulnerability audit (pip-audit)"
explain_step "Checks installed dependencies for known CVEs from advisory databases."
run_step "[13/14] Dependency vulnerability audit (pip-audit)" python -m pip_audit --ignore-vuln GHSA-4xh5-x5gv-qwph --ignore-vuln GHSA-6vgw-5pg2-w6jp

echo "[14/14] Policy scan (semgrep no-egress)"
explain_step "Enforces hard policy rules forbidding network/process/dynamic-exec patterns in application code."
if command -v semgrep >/dev/null 2>&1; then
  run_step "[14/14] Policy scan (semgrep no-egress)" semgrep --config semgrep-rules/no-egress.yml src tests
else
  run_step "[14/14] Policy scan (semgrep no-egress)" python -m semgrep --config semgrep-rules/no-egress.yml src tests
fi

if [[ "$FAILURES" -eq 0 ]]; then
  echo "QA completed successfully."
else
  echo "QA completed with ${FAILURES} failing step(s)."
  echo "Failed steps:"
  for step in "${FAILED_STEPS[@]}"; do
    echo "  - $step"
  done
  echo "Hint: for Ruff formatting issues run: python -m ruff format ."
  exit 1
fi
echo "Reports:"
echo "  - $REPORT_DIR/coverage.xml"
echo "  - $REPORT_DIR/htmlcov/index.html"
echo "  - $REPORT_DIR/bandit.txt"
