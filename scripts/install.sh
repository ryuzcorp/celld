#!/bin/sh
# Install the Cello binary from GitHub Releases.
#
#   curl -fsSL https://cello.ryuz.eu/install.sh | sh
#
# Options:
#   --beta              install the newest prerelease instead of latest stable
#   -h, --help          print help
#
# Environment:
#   CELLO_VERSION       install this version, e.g. 0.1.0 or v0.1.0
#                       (default: resolve the newest release)
#   CELLO_CHANNEL=beta  same as --beta
#   CELLO_INSTALL_DIR   install here (default: /usr/local/bin when writable
#                       or run as root, else ~/.local/bin)
#   CELLO_DOWNLOAD_BASE override the download base URL (tests only).
#   CELLO_API_BASE      override the GitHub API base URL (tests only).
#
# Integrity vs provenance: the SHA256SUMS check below proves the tarball
# arrived intact, not who built it. For provenance, verify the SLSA
# attestation attached to the release instead:
#   gh attestation verify cello-<version>-linux-<arch>.tar.gz --repo ryuzcorp/cello
# and for the image:
#   gh attestation verify oci://ghcr.io/ryuzcorp/cello@<digest> --repo ryuzcorp/cello
#
# The script runs inside main() (invoked on the last line), so piping a
# truncated download into sh defines functions but installs nothing.
set -eu

REPO="ryuzcorp/cello"
APP="cello"

die() {
  echo "install: $*" >&2
  exit 1
}

print_help() {
  cat <<'HELP'
Usage: install.sh [--beta] [-h|--help]

Install the Cello binary from GitHub Releases.

  --beta              install the newest prerelease instead of latest stable
  -h, --help          print this help and exit

Environment:
  CELLO_VERSION       install this version, e.g. 0.1.0 or v0.1.0
  CELLO_CHANNEL=beta  same as --beta
  CELLO_INSTALL_DIR   install here (default: /usr/local/bin when writable
                      or run as root, else ~/.local/bin)
  CELLO_DOWNLOAD_BASE override the download base URL (tests only)
HELP
}

# Newest stable: follow the releases/latest redirect to its tag.
latest_stable_tag() {
  url="$(curl -fsSL --proto '=https' --proto-redir '=https' \
    -o /dev/null -w '%{url_effective}' \
    "https://github.com/$REPO/releases/latest")" \
    || die "could not resolve the latest release"
  tag="${url##*/}"
  [ -n "$tag" ] && [ "$tag" != "latest" ] || die "could not resolve the latest release"
  printf '%s' "$tag"
}

# Newest prerelease: first release in the API list flagged prerelease,
# paging through results until one is found or the pages run out.
latest_prerelease_tag() {
  api_base="${CELLO_API_BASE:-https://api.github.com}"
  page=1
  while :; do
    page_url="$api_base/repos/$REPO/releases?per_page=100&page=$page"
    assert_safe_url "$page_url"
    json="$(api_get "$page_url")" \
      || die "could not resolve the latest prerelease"
    if command -v jq >/dev/null 2>&1; then
      tag="$(printf '%s' "$json" | jq -r \
        '[.[] | select(.prerelease == true) | .tag_name] | first // empty' || true)"
      count="$(printf '%s' "$json" | jq -r \
        'if type == "array" then length else 0 end' || true)"
    else
      tag="$(printf '%s' "$json" | tr ',' '\n' | awk -F'"' '
        $2 == "tag_name" { tag = $4 }
        $2 == "prerelease" {
          line = $0; sub(/.*: */, "", line); sub(/[^a-z].*/, "", line)
          if (line == "true" && tag != "") { print tag; exit }
          tag = ""
        }')"
      count="$(printf '%s' "$json" | grep -o '"tag_name"' | wc -l)"
      count="$(printf '%s' "$count" | tr -d '[:space:]')"
    fi
    if [ -n "$tag" ]; then
      printf '%s' "$tag"
      return 0
    fi
    [ "${count:-0}" -gt 0 ] || break
    page=$((page + 1))
  done
  die "no prerelease found"
}

# Download a URL to a file. https-only, with --proto guards; plain http is
# accepted solely for anchored-localhost test overrides (enforced by
# assert_safe_url before fetch is called).
fetch() {
  out="$1"
  url="$2"
  case "$url" in
    https://*)
      curl -fsSL --proto '=https' --proto-redir '=https' -o "$out" "$url" ;;
    *)
      curl -fsSL -o "$out" "$url" ;;
  esac
}
# Fetch an API URL to stdout. https-only, with --proto guards; plain http
# is accepted solely for anchored-localhost test overrides (enforced by
# assert_safe_url before any call).
api_get() {
  case "$1" in
    https://*)
      curl -fsSL --proto '=https' --proto-redir '=https' "$1" ;;
    *)
      curl -fsSL "$1" ;;
  esac
}

# Exact loopback authority with an optional numeric port; userinfo
# (`user@host`) is never loopback.
is_loopback_authority() {
  case "$1" in
    *@*) return 1 ;;
  esac
  case "$1" in
    '['*']'*)
      host="${1%%]*}]"
      tail="${1#*]}"
      [ "$host" = "[::1]" ] || return 1
      case "$tail" in
        "") port="" ;;
        :*) port="${tail#:}" ;;
        *) return 1 ;;
      esac
      ;;
    *)
      case "$1" in
        *:*) host="${1%%:*}"; port="${1#*:}" ;;
        *) host="$1"; port="" ;;
      esac
      case "$host" in
        localhost | 127.0.0.1) ;;
        *) return 1 ;;
      esac
      ;;
  esac
  case "$port" in
    "") return 0 ;;
    *[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

# https only; plain http is accepted solely for anchored test overrides
# ($CELLO_DOWNLOAD_BASE / $CELLO_API_BASE, tests only) whose authority is
# exactly loopback with an optional numeric port.
assert_safe_url() {
  case "$1" in
    https://*) return 0 ;;
    http://*) ;;
    *) die "refusing to download over insecure http: $1" ;;
  esac
  rest="${1#http://}"
  authority="${rest%%/*}"
  authority="${authority%%\?*}"
  authority="${authority%%\#*}"
  is_loopback_authority "$authority" &&
    [ -n "${CELLO_DOWNLOAD_BASE:-}${CELLO_API_BASE:-}" ] ||
    die "refusing to download over insecure http: $1"
}

main() {
  beta=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --beta) beta="1"; shift ;;
      -h | --help) print_help; return 0 ;;
      *) die "unknown argument: $1 (see --help)" ;;
    esac
  done
  [ "${CELLO_CHANNEL:-stable}" = "beta" ] && beta="1"

  # --- platform -----------------------------------------------------------
  os="$(uname -s)"
  [ "$os" = "Linux" ] || die "refusing: $os is not supported (Linux only)"
  machine="$(uname -m)"
  case "$machine" in
    x86_64) arch="amd64" ;;
    aarch64 | arm64) arch="arm64" ;;
    *) die "refusing: $machine is not supported (amd64/arm64 only)" ;;
  esac

  command -v curl >/dev/null 2>&1 || die "refusing: curl is required"
  command -v tar >/dev/null 2>&1 || die "refusing: tar is required"
  command -v grep >/dev/null 2>&1 || die "refusing: grep is required"

  # --- version ------------------------------------------------------------
  if [ -n "${CELLO_VERSION:-}" ]; then
    tag="$CELLO_VERSION"
  elif [ -n "$beta" ]; then
    tag="$(latest_prerelease_tag)"
  else
    tag="$(latest_stable_tag)"
  fi
  case "$tag" in
    v*) ;;
    *) tag="v$tag" ;;
  esac
  version="${tag#v}"
  # The version shapes download URLs and file names, whether the user set
  # it or a resolver returned it: reject newlines, spaces and slashes
  # first, then enforce the semver charset and shape.
  nl=$(printf '\nX'); nl=${nl%X}
  case "$version" in
    *"$nl"*|*' '*|*/*)
      die "refusing: invalid version: $version" ;;
  esac
  printf '%s' "$version" | grep -Eq \
    '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$' \
    || die "refusing: invalid version: $version"

  # --- download -----------------------------------------------------------
  if [ -n "${CELLO_DOWNLOAD_BASE:-}" ]; then
    base="$CELLO_DOWNLOAD_BASE"
  else
    base="https://github.com/$REPO/releases/download/$tag"
  fi
  assert_safe_url "$base"

  tarball="$APP-$version-linux-$arch.tar.gz"

  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT INT TERM

  echo "install: downloading $tarball ($tag)" >&2
  fetch "$tmp/$tarball" "$base/$tarball" \
    || die "download failed: $base/$tarball"
  fetch "$tmp/SHA256SUMS" "$base/SHA256SUMS" \
    || die "download failed: $base/SHA256SUMS"

  # --- checksum -----------------------------------------------------------
  if command -v sha256sum >/dev/null 2>&1; then
    sum() { sha256sum "$1" | awk '{print $1}'; }
  elif command -v shasum >/dev/null 2>&1; then
    sum() { shasum -a 256 "$1" | awk '{print $1}'; }
  else
    die "refusing: sha256sum or shasum is required to verify the download"
  fi

  want="$(grep -F " $tarball" "$tmp/SHA256SUMS" | awk '{print $1}' || true)"
  [ -n "$want" ] || die "refusing: $tarball is missing from SHA256SUMS"
  got="$(sum "$tmp/$tarball")"
  [ "$want" = "$got" ] || die "refusing: checksum mismatch for $tarball (want $want, got $got)"

  # --- install ------------------------------------------------------------
  if [ -n "${CELLO_INSTALL_DIR:-}" ]; then
    dir="$CELLO_INSTALL_DIR"
  elif [ -w /usr/local/bin ] || [ "$(id -u)" -eq 0 ]; then
    dir="/usr/local/bin"
  else
    dir="$HOME/.local/bin"
  fi
  mkdir -p "$dir"
  tar -xzf "$tmp/$tarball" -C "$tmp"
  [ -f "$tmp/$APP" ] || die "refusing: $tarball has no $APP binary"
  if command -v install >/dev/null 2>&1; then
    install -m 755 "$tmp/$APP" "$dir/$APP"
  else
    cp "$tmp/$APP" "$dir/$APP"
    chmod 755 "$dir/$APP"
  fi

  "$dir/$APP" --version >&2
  echo "install: installed $dir/$APP" >&2
  case ":$PATH:" in
    *":$dir:"*) ;;
    *) echo "install: add $dir to PATH to run $APP from anywhere" >&2 ;;
  esac
  echo "install: note: bundled deploys need esbuild on PATH" >&2
}

main "$@"
