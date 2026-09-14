#!/usr/bin/env bash
# Cross-compile release archives into dist/.
#
# Usage: scripts/build-release.sh [version]
#
# The version defaults to the current git description and is stamped into both
# binaries, so `r1sd --version` and `r1s --version` report it.
set -euo pipefail

cd "$(dirname "$0")/.."

BINARIES=(r1sd r1s)
ARCHIVE=r1s
VERSION="${1:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
DIST=dist

PLATFORMS=(
	linux/amd64
	linux/arm64
	darwin/amd64
	darwin/arm64
	windows/amd64
	windows/arm64
)

rm -rf "$DIST"
mkdir -p "$DIST"

for platform in "${PLATFORMS[@]}"; do
	goos="${platform%/*}"
	goarch="${platform#*/}"
	stage="$DIST/${ARCHIVE}_${VERSION}_${goos}_${goarch}"

	exe=""
	if [ "$goos" = windows ]; then
		exe=.exe
	fi

	mkdir -p "$stage"
	for binary in "${BINARIES[@]}"; do
		echo "building $goos/$goarch $binary"
		CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build \
			-trimpath \
			-ldflags "-s -w -X main.version=${VERSION}" \
			-o "$stage/${binary}${exe}" \
			./cmd/"$binary"
	done

	cp README.md "$stage/"
	# Windows users expect a zip; everything else gets a tarball.
	if [ "$goos" = windows ]; then
		(cd "$DIST" && zip -qr "$(basename "$stage").zip" "$(basename "$stage")")
	else
		tar -czf "${stage}.tar.gz" -C "$DIST" "$(basename "$stage")"
	fi
	rm -rf "$stage"
done

cd "$DIST"
if command -v sha256sum >/dev/null 2>&1; then
	sha256sum ./*.tar.gz ./*.zip | sed 's| \./| |' > SHA256SUMS
else
	shasum -a 256 ./*.tar.gz ./*.zip | sed 's| \./| |' > SHA256SUMS
fi
echo
echo "artifacts in $DIST:"
ls -1
