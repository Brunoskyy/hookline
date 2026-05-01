#!/usr/bin/env bash
# Builds dist/lambda.zip for arm64 Lambda (Python 3.13, Amazon Linux 2023, glibc 2.34).
# Run from the repository root.
set -euo pipefail
rm -rf build/lambda dist/lambda.zip
mkdir -p build/lambda dist
uv export --no-dev --no-hashes --format requirements-txt > build/requirements.txt
uv pip install --target build/lambda --python-platform aarch64-manylinux_2_28 \
  --python-version 3.13 --only-binary=:all: -r build/requirements.txt
cp -r src/hookline build/lambda/
(cd build/lambda && zip -qr ../../dist/lambda.zip .)
echo "dist/lambda.zip: $(du -h dist/lambda.zip | cut -f1)"
