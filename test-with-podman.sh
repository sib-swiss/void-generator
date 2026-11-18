#!/usr/bin/env bash
set -euo pipefail

# $XDG_RUNTIME_DIR is often unset outside an interactive login session
# (e.g. in a plain shell/CI step); fall back to podman/systemd's own
# convention instead of building a broken socket path.
: "${XDG_RUNTIME_DIR:=/run/user/$(id -u)}"
socket="${XDG_RUNTIME_DIR}/podman/podman.sock"

podman system service --time=0 "unix://${socket}" &
pid=$!

cleanup() {
	# $pid is always set by this point (right after backgrounding, before
	# the trap can ever fire), so this never races an unset/empty kill target.
	if kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
		for _ in 1 2 3 4 5; do
			kill -0 "$pid" 2>/dev/null || break
			sleep 1
		done
		# still alive after a graceful SIGTERM wait -- force it.
		kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
	fi
	# reap the background job so the shell doesn't print its own
	# "Terminated" notice, and so this function doesn't exit non-zero
	# (which would otherwise override the script's real exit status).
	wait "$pid" 2>/dev/null || true
}
# Runs on normal exit, on error (set -e), and on Ctrl-C/SIGTERM -- the one
# thing the original script didn't do, so an interrupted or failing `mvn`
# run left the podman service running forever.
trap cleanup EXIT INT TERM

# Give the API socket a moment to actually appear before pointing
# Testcontainers at it, instead of racing podman's own startup.
for _ in $(seq 1 30); do
	[ -S "${socket}" ] && break
	sleep 0.5
done

export DOCKER_HOST="unix://${socket}"
export TESTCONTAINERS_RYUK_DISABLED=true

# Two separate mvn calls, not `-P virtuoso-it,qlever-it verify` in one: both
# profiles declare a default-id maven-failsafe-plugin execution with their
# own <includes>, and activating both at once would merge/override one
# profile's plugin configuration with the other's rather than running both.
mvn -Pvirtuoso-it verify
mvn -Pqlever-it verify
