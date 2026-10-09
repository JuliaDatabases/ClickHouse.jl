#!/bin/bash
set +e
julia --version > /work/julia-version.txt
julia --startup-file=no --check-bounds=yes --project=/work/env -e 'using Pkg; Pkg.develop(PackageSpec(path="/source")); Pkg.instantiate(); Pkg.precompile()' > /work/setup.log 2>&1
phase_status=$?
printf '%s\n' "$phase_status" > /work/setup.exit
if [ "$phase_status" -ne 0 ]; then exit "$phase_status"; fi
julia --startup-file=no --check-bounds=yes --project=/work/env /harness/root-clickhouse-insert-progress-probe.jl > /work/probe.log 2>&1
phase_status=$?
printf '%s\n' "$phase_status" > /work/probe.exit
julia --startup-file=no --check-bounds=yes --project=/work/env /harness/root-clickhouse-insert-progress-live.jl > /work/live.log 2>&1
phase_status=$?
printf '%s\n' "$phase_status" > /work/live.exit
julia --startup-file=no --check-bounds=yes --project=/work/docs -e 'using Pkg; Pkg.develop(PackageSpec(path="/source")); Pkg.instantiate()' > /work/docs-setup.log 2>&1
phase_status=$?
printf '%s\n' "$phase_status" > /work/docs-setup.exit
if [ "$phase_status" -ne 0 ]; then exit "$phase_status"; fi
julia --startup-file=no --check-bounds=yes --project=/work/docs /work/docs/make.jl > /work/docs-build.log 2>&1
phase_status=$?
printf '%s\n' "$phase_status" > /work/docs-build.exit
for process_path in /proc/[0-9]*/cmdline; do printf '%s ' "$process_path"; tr '\0' ' ' < "$process_path"; printf '\n'; done > /work/processes-final.txt
exit 0
