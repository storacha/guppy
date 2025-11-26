#!/bin/bash

# Performs an upload and retrieval test using the guppy client CLI.
#
# Usage: test/doupload.sh <account-email>
#
# Be prepared to log in to the account by email link twice. The test will:
# 1. Create a new space
# 2. Upload some random data to the space
# 3. Retrieve the full data
# 4. Retrieve a subdirectory of the data
# 5. Reset the client and log in again
# 6. Retrieve the full data again
# 7. Verify that all retrieved data matches the original

set -e
set -o pipefail

# Change to the directory of this script
cd "$(dirname "$0")"

# Teeing to /dev/fd/3 will show output on stdout while still capturing it.
exec 3>&1

account="$1"
if [ -z "$account" ]; then
	echo "Usage: $0 <account-email>"
	exit 1
fi

sandbox="doupload"

rm -rf "$sandbox"
mkdir -p "$sandbox"

guppy="go run .."
guppy="$guppy --storacha-dir ./$sandbox/storacha"

dataDir="$sandbox/data"
outDir1="$sandbox/out1"
outDir2="$sandbox/out2"
outDir3="$sandbox/out3"


mkdir -p "$dataDir/subdir"
dd if=/dev/urandom bs=1 count=10240 status=none > "$dataDir/file1"
dd if=/dev/urandom bs=1 count=10240 status=none > "$dataDir/file2"
dd if=/dev/urandom bs=1 count=10240 status=none > "$dataDir/subdir/file1"
dd if=/dev/urandom bs=1 count=10240 status=none > "$dataDir/subdir/file2"

echo "🔐 Logging in as $account"
$guppy login "$account"
echo
echo "🎁 Generating new space"
output=$($guppy space generate | tee /dev/fd/3)
space=$(echo "$output" | grep 'Generated space:' | awk '{print $3}')

echo
echo "📜 Listing space info"
$guppy space info "$space"

echo
echo "📤 Uploading data from $dataDir to space $space"
$guppy upload sources add "$space" "$dataDir"
output=$($guppy upload "$space" | tee /dev/fd/3)
rootCID=$(echo "$output" | grep 'Root CID:' | awk '{print $3}')

echo
echo "📥 Retrieving data from space $space with root CID $rootCID to $outDir1"
$guppy retrieve "$space" "$rootCID" "$outDir1"

echo
echo "📥 Retrieving data from only /subdir with root CID $rootCID to $outDir2"
$guppy retrieve "$space" "$rootCID"/subdir "$outDir2"


echo
echo "🔄 Resetting client"
$guppy reset
echo "🔐 Logging in as $account again"
$guppy login "$account"

echo
echo "📥 Retrieving data from space $space with root CID $rootCID to $outDir3"
$guppy retrieve "$space" "$rootCID" "$outDir3"

echo "↔️ Verifying retrieved data matches original"
diff -r "$dataDir" "$outDir1"
diff -r "$dataDir/subdir" "$outDir2"
diff -r "$dataDir" "$outDir3"
echo "✅ Data verified!"