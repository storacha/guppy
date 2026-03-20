#!/usr/bin/env bash
set -euo pipefail

# Bail if working tree is not clean
if [ -n "$(git status --porcelain)" ]; then
  echo "Error: Working tree is not clean. Please commit or stash your changes first."
  exit 1
fi

echo "Fetching from origin..."
git fetch origin

echo "Checking out origin/main..."
git checkout origin/main

echo "Running gorelease..."
output=$(go tool gorelease 2>&1) || true
echo "$output"

# Extract suggested version
version=$(echo "$output" | grep '^Suggested version:' | sed 's/Suggested version: //')

if [ -z "$version" ]; then
  echo "Error: gorelease did not suggest a version. You may need to choose one manually."
  exit 1
fi

echo ""

# Create release branch
echo "Creating branch release/$version..."
git checkout -b "release/$version"

# Update version.json
echo "Updating version.json..."
cat > version.json <<EOF
{
  "version": "$version"
}
EOF

# Commit the change
git add version.json
git commit -m "chore: Release $version"

echo ""
echo "Done! Branch release/$version is ready."
echo "Push it and open a PR when ready."
