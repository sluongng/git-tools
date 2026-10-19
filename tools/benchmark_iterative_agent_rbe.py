#!/usr/bin/env python3
"""Matched baseline/candidate edit -> status -> real BLAKE3 RBE build acceptance.

Dry-run by default. Reuses the provisioned-toolchain warm-proxy startup harness;
server preparation and first build are separate from steady-state edit cycles.
Every cycle changes one semantic byte, fsyncs it, runs full tracked Git status,
and requires exactly three uncached remote C++ actions with matching outputs.
No retries, outlier filtering, global cache flushing, or shared writable clients.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import select
import shutil
import signal
import statistics
import subprocess
import sys
import time
import traceback

from benchmark_agent_rbe import credential_helper, grpc_log_summary, terminate_owned_bazel, workspace_log_summary
from benchmark_github_bazel import COMMIT, SOURCE, dump, sha256
from benchmark_proxy_agent_rbe import AgentRbeBenchmark, TARGET

ARMS = ("baseline", "candidate")
EDIT_PATH = "src/main/cpp/util/strings.cc"
EDIT_ANCHOR = b'static const char kSeparator[] = "'
# Distinct, single-byte C++ string characters: no escapes and no original space.
EDIT_BYTES = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
BUILD_TIMEOUT = 600
STATUS_TIMEOUT = 60
EDIT_TIMEOUT = 30


def sample_summary(values):
    ordered = sorted(values)
    if not ordered:
        return {"n": 0}
    return {"n": len(ordered), "p50": statistics.median(ordered),
            "p95": ordered[math.ceil(.95 * len(ordered)) - 1],
            "min": ordered[0], "max": ordered[-1], "samples": list(values)}


def validate_config(config):
    """Only accept this public workload/configuration, not arbitrary credentials."""
    if config.get("source") != SOURCE or config.get("commit") != COMMIT:
        raise ValueError("Use the pinned GitHub Bazel source/commit")
    if config.get("bazel_version") != "bazel 9.2.0":
        raise ValueError("This acceptance workload requires Bazel 9.2.0")
    if config.get("metadata_mode") != "production-warm-sizes":
        raise ValueError("Use production-warm-sizes, not a benchmark metadata adapter")
    if not config.get("toolchain_provisioning_manifest") or not config.get("pilot_dependencies_warm_verified"):
        raise ValueError("Require provisioned toolchain manifest and verified warm dependency pilot")
    if config.get("build_target") != TARGET or not config.get("build_output_paths"):
        raise ValueError("Require the pinned strings target and nonempty output manifest paths")
    extra = config.get("rbe_extra_flags", [])
    if not extra or any(not isinstance(flag, str) or not flag.startswith("--override_repository=") for flag in extra):
        raise ValueError("rbe_extra_flags may contain only the provisioned repository override")
    # These config files are copied to artifacts. Credentials belong ONLY in the
    # existing host-scoped helper, never config/argv, even for a diagnostic run.
    def inspect(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if re.search(r"credential|password|secret|token|header|api.?key", key, re.I):
                    raise ValueError("Credential-like config fields are forbidden; use --credential-rc")
                inspect(child)
        elif isinstance(value, list):
            for child in value:
                inspect(child)
        elif isinstance(value, str) and re.search(r"authorization|remote_header|api.?key[=:]|token[=:]|https?://[^/\s]+@", value, re.I):
            raise ValueError("Credential-like config values are forbidden; use --credential-rc")
    inspect(config)


class IterativeRbeBenchmark(AgentRbeBenchmark):
    def __init__(self, args):
        config_bytes = args.config.read_bytes()
        config = json.loads(config_bytes)
        validate_config(config)
        super().__init__(args)
        self.input_config_hash = hashlib.sha256(config_bytes).hexdigest()
        if sha256(args.config) != self.input_config_hash:
            raise ValueError("Configuration changed while being validated")
        self.config["vfs_binary"] = str(args.baseline_vfs.resolve())
        self.config["vfs_source_revision_verified"] = args.baseline_source
        # No historical local/checkout golden applies to each semantic edit.
        self.config.pop("golden_build_outputs", None)
        self.current_attribute = "user.blake3"
        self.current_arm = "baseline"
        self.current_cycle = 0
        self.frozen_vfs = {}
        self.result["workflow"] = "iterative-agent-rbe"
        self.result["scope"] = ("functional pilot only; not headline latency evidence; other compilation/validation may overlap"
                                if args.functional_pilot else "matched iterative RBE acceptance matrix")
        self.result["execution_options"].update(cycles=args.cycles, trials=args.trials,
            build_timeout_seconds=BUILD_TIMEOUT, status_timeout_seconds=STATUS_TIMEOUT,
            edit_timeout_seconds=EDIT_TIMEOUT)
        self.result["environment"].update({
            "ablation": "frozen baseline VFS versus frozen candidate VFS; both user.blake3, identical warm proxy, CAS, r2c2, prefetch and provisioned RBE toolchain",
            "timing": "startup clone-to-first-successful-build separate; each iteration starts at edit worker launch and stops at successful remote build process exit, including edit/status/build and handovers; hashes/log parsing/audits outside timer",
            "cycle_edit": "one fsynced same-size semantic separator byte in strings.cc, unique deterministic value per cycle, no restore between cycles",
            "percentiles": "p50 median; p95 nearest rank (small N can equal max); pooled cycles are correlated within cells; per-trial summaries also retained",
            "r2c2_boundary": "startup requires observed successful repository GetActionResult RPCs; warm Bazel iterations may issue none, which is reported as no observed lookup, never a hit",
            "cache_policy": "owned proxy mirror/packs/mappings and workspace CAS prewarmed before cells; full upstream mirror population and image provisioning excluded/disclosed; shared archive cache verified warm; private clone/overlay/output base/local r2c2 cache per cell, retained across its edit cycles; OS caches not dropped",
        })
        self.result["output_references_by_cycle"] = {}
        self.result["output_comparisons"] = []

    def freeze(self):
        if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
            raise RuntimeError("Iterative acceptance requires Linux pidfd ownership and signaling APIs")
        if sha256(self.args.config) != self.input_config_hash:
            raise ValueError("Configuration changed after validation")
        super().freeze()
        self.frozen_vfs["baseline"] = self.vfs
        candidate = self.root / "git-vfs-candidate-frozen"
        shutil.copy2(self.args.candidate_vfs, candidate)
        self.frozen_vfs["candidate"] = candidate
        self.run([candidate, "--version"], "candidate-vfs-version")
        self.result["frozen_vfs_arms"] = {
            arm: {"path": str(path), "sha256": sha256(path),
                  "source_revision_claim": getattr(self.args, arm + "_source"),
                  "input_path": str(getattr(self.args, arm + "_vfs").resolve())}
            for arm, path in self.frozen_vfs.items()}
        harness = self.root / "harness" / Path(__file__).name
        shutil.copy2(__file__, harness)
        self.result["harness_provenance"][harness.name] = sha256(harness)
        shutil.copy2(self.args.config, self.root / "input-config.json")
        self.result["input_config_sha256"] = sha256(self.root / "input-config.json")
        if self.result["input_config_sha256"] != self.input_config_hash:
            raise ValueError("Configuration changed while being frozen")
        self.save()

    def verify_build_outputs(self, outputs, name, phase, checkout):
        actual = self.canonical_outputs(outputs)
        key = str(self.current_cycle)
        references = self.result["output_references_by_cycle"]
        reference = references.get(key)
        if reference is None:
            if self.current_arm != "baseline":
                raise RuntimeError("A baseline cell must establish each cycle's output reference first")
            reference = references[key] = {"cell": name, "cycle": self.current_cycle, "outputs": actual}
        if actual != reference["outputs"]:
            raise RuntimeError("Output path/hash/size differs from the same edit cycle's frozen baseline reference")
        self.result["output_comparisons"].append({"cell": name, "cycle": self.current_cycle,
            "matches_reference": True, "reference_cell": reference["cell"], "outputs": actual})

    def build_logs(self, raw_workspace, raw_grpc, evidence):
        try:
            evidence["workspace_rule_events"] = workspace_log_summary(raw_workspace)
            evidence["r2c2_rpc"] = grpc_log_summary(raw_grpc)
            successful = [call for call in evidence["r2c2_rpc"]["calls"]
                if call["method"].endswith("/GetActionResult") and call["status_code"] == 0
                and call.get("action_id") and not re.fullmatch(r"[0-9a-f]{64}", call["action_id"])
                and re.fullmatch(r"[0-9a-f]{64}", call.get("action_digest", {}).get("hash", ""))]
            evidence["r2c2_repository_lookup_successes"] = len(successful)
            evidence["r2c2_repository_ids"] = sorted({call["action_id"] for call in successful})
            evidence["r2c2_observation"] = "observed successful repository lookup" if successful else "no successful repository lookup observed in this build"
        finally:
            # These binary logs can contain request headers/environments. Keep
            # only existing safe parsers' summaries; raw logs live outside artifacts.
            raw_workspace.unlink(missing_ok=True)
            raw_grpc.unlink(missing_ok=True)

    def execute_build(self, cwd, base, contents, name, evidence):
        raw_workspace = self.credential.parent / (name + "-workspace.bin")
        raw_grpc = self.credential.parent / (name + "-grpc.bin")
        command = self.remote_build_command(base, name, contents, raw_workspace, raw_grpc)
        try:
            evidence["phases"]["build"] = self.run(command, name + "-first_build", cwd=cwd,
                                                     timeout=BUILD_TIMEOUT, check=False)
        finally:
            # Finish/log summaries are outside the continuous build/cycle timer.
            self.critical = False
            self.build_logs(raw_workspace, raw_grpc, evidence)
        phase = evidence["phases"]["build"]
        if phase["returncode"] or phase["timed_out"]:
            raise RuntimeError("Remote build failed; retained command status/logs identify the cell")
        evidence["variant"] = self.current_arm
        self.verify_remote_build(cwd, name, evidence)
        return phase["wait_end_perf_counter"]

    def read_source(self, source, name):
        phase = self.run([sys.executable, self.root / "harness" / Path(__file__).name,
                          "--read-source", source], name, timeout=STATUS_TIMEOUT)
        phase["outside_startup_and_cycle_timers"] = True
        return (self.root / (name + ".stdout")).read_bytes(), phase

    def stop_bazel(self, base, cwd, name):
        result = self.run(self.bazel_args(base) + ["shutdown"], name + "-shutdown",
                          cwd=cwd, timeout=60, check=False)
        if result["returncode"]:
            if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
                raise RuntimeError("Bazel fallback refuses signaling without pidfd identity support")
            self.result.setdefault("bazel_cleanup_fallbacks", []).append(
                {"cell": name, "result": terminate_owned_bazel(base)})
            self.save()
            raise RuntimeError("Bazel shutdown failed; pidfd-only owned-server fallback attempted")

    def stop_daemon(self, state, name, mountpoint=None):
        process, log = state
        descriptor, forced, interrupted = None, False, None
        try:
            if process.returncode is None:
                # This owner has not reaped the leader. No numeric signal below
                # is issued after wait(), including the timeout escalation.
                try:
                    descriptor = os.pidfd_open(process.pid)
                    ready, _, _ = select.select([descriptor], [], [], 0)
                    if not ready:
                        try:
                            os.killpg(process.pid, signal.SIGINT)
                        except ProcessLookupError:
                            pass
                        ready, _, _ = select.select([descriptor], [], [], 30)
                    forced = not ready
                except BaseException as error:
                    interrupted, forced = error, True
                finally:
                    if forced:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    try:
                        process.wait()
                    except KeyboardInterrupt as error:
                        # Exit was ready or SIGKILL was already sent. Reap,
                        # without attempting any numeric post-wait signal.
                        interrupted = interrupted or error
                        process.wait()
        finally:
            if descriptor is not None:
                os.close(descriptor)
            log.close()
        mount_remaining = bool(mountpoint and os.path.ismount(mountpoint))
        if mount_remaining:
            subprocess.run(["fusermount3", "-uz", str(mountpoint)], env=self.env,
                           capture_output=True, timeout=5, check=False)
            mount_remaining = os.path.ismount(mountpoint)
            forced = True
        try:
            os.killpg(process.pid, 0)  # Observation only; never a signaling target after reap.
            residual_group = True
        except ProcessLookupError:
            residual_group = False
        record = {"name": name, "returncode": process.returncode, "forced": forced,
                  "residual_group_observed": residual_group, "mount_remaining": mount_remaining}
        self.result.setdefault("owned_daemon_cleanup", []).append(record)
        self.save()
        if interrupted is not None:
            raise interrupted
        if forced or residual_group or mount_remaining or process.returncode:
            raise RuntimeError("Daemon cleanup was forced, unsuccessful, or observed a residual group; no post-reap group signal sent")

    def stop_mount(self, mountpoint, state):
        if not mountpoint.resolve().is_relative_to(self.root):
            raise RuntimeError("Refuse cleanup of a mount outside the owned benchmark root")
        self.stop_daemon(state, mountpoint.name, mountpoint)

    def stop_proxy(self):
        if self.proxy_state:
            state, self.proxy_state = self.proxy_state, None
            self.stop_daemon(state, "proxy")

    def cell(self, trial, arm):
        self.current_arm, self.current_cycle = arm, 0
        self.vfs = self.frozen_vfs[arm]
        self.current_attribute = "user.blake3"
        name = f"trial-{trial}-{arm}"
        repo, overlay = self.root / (name + ".git"), self.root / (name + "-overlay")
        mountpoint, base = self.root / (name + "-mount"), self.root / (name + "-output")
        contents = self.root / (name + "-repo-contents-cache")
        row = {"trial": trial, "variant": arm, "phases": {}, "first_build": {"phases": {}},
               "cycles": [], "complete": False, "digest_function": "BLAKE3", "digest_attribute": "user.blake3",
               "private_paths": {"repo": str(repo), "overlay": str(overlay), "output_base": str(base),
                                 "repo_contents_cache": str(contents)}}
        self.result["active_cell"] = row
        self.save()
        state, started, failed = None, False, False
        cleanup_errors = []
        try:
            self.critical = True
            row["phases"]["clone"] = self.clone(repo, name, False)
            self.warm_mapping(repo, name, row["phases"])
            state, row["phases"]["mount"] = self.mount(repo, overlay, mountpoint, name)
            started = True
            first_end = self.execute_build(mountpoint, base, contents, name, row["first_build"])
            row["real_wall_clone_to_first_build_seconds"] = first_end - row["phases"]["clone"]["start_perf_counter"]
            if row["first_build"]["r2c2_repository_lookup_successes"] == 0:
                raise RuntimeError("First build did not establish actual r2c2 repository lookup evidence")
            if self.git(repo, ["rev-parse", "HEAD"]).decode().strip() != COMMIT:
                raise RuntimeError("Unexpected cloned revision")
            if self.git(repo, ["remote", "get-url", "origin"]).decode().strip() != self.clone_url:
                raise RuntimeError("Unexpected cloned origin")
            source = mountpoint / EDIT_PATH
            original, row["edit_source_materialization"] = self.read_source(source, name + "-materialize-edit-source")
            row["edit_source_materialization"]["note"] = "Read the public source fixture once before steady-state edit cycles; this can hydrate its base blob"
            if original.count(EDIT_ANCHOR) != 1:
                raise RuntimeError("Pinned semantic edit fixture anchor changed")
            offset = original.index(EDIT_ANCHOR) + len(EDIT_ANCHOR)
            if original[offset:offset + 1] != b" ":
                raise RuntimeError("Pinned semantic edit fixture value changed")
            original_sha = hashlib.sha256(original).hexdigest()
            previous = self.canonical_outputs(row["first_build"]["build_outputs"])
            row["source_fixture"] = {"path": EDIT_PATH, "size_bytes": len(original),
                "original_sha256": original_sha, "edit_offset": offset}
            row["background_prefetch_after_first_build"] = self.background_prefetch(name)
            self.save()
            for index, byte in enumerate(EDIT_BYTES[:self.args.cycles], 1):
                self.current_cycle = index
                cycle_name = name + f"-cycle-{index}"
                cycle = {"cycle": index, "phases": {}, "complete": False,
                         "expected_byte_hex": bytes([byte]).hex()}
                row["cycles"].append(cycle)
                self.save()
                self.critical = True
                cycle["phases"]["edit"] = self.run([sys.executable,
                    self.root / "harness" / Path(__file__).name, "--write-byte", source,
                    str(offset), bytes([byte]).hex()], cycle_name + "-edit", timeout=EDIT_TIMEOUT,
                    check=False)
                if cycle["phases"]["edit"]["returncode"] or cycle["phases"]["edit"]["timed_out"]:
                    raise RuntimeError("Semantic edit worker failed; retained phase/logs identify the cell")
                cycle["phases"]["git_status"] = self.run([self.config["git"], "-C", mountpoint,
                    "status", "--porcelain=v1", "--untracked-files=no"], cycle_name + "-status",
                    cwd=mountpoint, timeout=STATUS_TIMEOUT, check=False)
                if cycle["phases"]["git_status"]["returncode"] or cycle["phases"]["git_status"]["timed_out"]:
                    raise RuntimeError("Git status failed; retained phase/logs identify the cell")
                expected_status = (" M " + EDIT_PATH + "\n").encode()
                if (self.root / (cycle_name + "-status.stdout")).read_bytes() != expected_status:
                    raise RuntimeError("Git status did not report exactly the semantic tracked edit")
                end = self.execute_build(mountpoint, base, contents, cycle_name, cycle)
                cycle["continuous_edit_status_build_seconds"] = end - cycle["phases"]["edit"]["start_perf_counter"]
                cycle["phase_sum_seconds"] = sum(item["seconds"] for item in cycle["phases"].values())
                expected = original[:offset] + bytes([byte]) + original[offset + 1:]
                actual, cycle["source_verification"] = self.read_source(source, cycle_name + "-verify-source")
                if actual != expected:
                    raise RuntimeError("Acknowledged semantic edit bytes differ from expected contents")
                cycle["changed_source"] = {"path": EDIT_PATH, "size_bytes": len(actual),
                    "sha256": hashlib.sha256(actual).hexdigest(),
                    "changed_bytes_sha256": hashlib.sha256(bytes([byte])).hexdigest()}
                current = self.canonical_outputs(cycle["build_outputs"])
                if current == previous:
                    raise RuntimeError("Semantic edit failed to change compiled output manifest")
                cycle["outputs_changed_from_previous_cycle"] = True
                cycle["complete"] = True
                previous = current
                self.save()
            row["footprint_mounted"] = self.footprint(repo, overlay)
            self.stop_bazel(base, mountpoint, name)
            started = False
        except BaseException as error:
            failed = True
            row["failure"] = {"type": type(error).__name__, "cycle": self.current_cycle}
            if isinstance(error, RuntimeError):
                row["failure"]["reason"] = str(error)
            raise
        finally:
            self.critical = False
            if started:
                try:
                    self.stop_bazel(base, mountpoint, name + "-cleanup")
                except BaseException as error:
                    cleanup_errors.append({"component": "bazel", "type": type(error).__name__})
            if state:
                try:
                    self.stop_mount(mountpoint, state)
                    lines = (self.root / (name + "-mount.stderr")).read_text().splitlines()
                    counters = [json.loads(line.split(": ", 1)[1]) for line in lines
                                if line.startswith("BLAKE3 xattrs: ")]
                    if not counters:
                        raise RuntimeError("Missing VFS BLAKE3 shutdown counters")
                    row["bazel_blake3_xattrs"] = counters[-1]
                    usage = counters[-1]
                    if any(type(usage.get(key)) is not int or usage[key] < 0
                           for key in ("base_hits", "local_hashes", "misses")):
                        raise RuntimeError("Invalid VFS BLAKE3 consumption counters")
                    edited_consumption = usage["local_hashes"]
                    if "local_cache_hits" in usage:
                        if type(usage["local_cache_hits"]) is not int or usage["local_cache_hits"] < 0:
                            raise RuntimeError("Invalid VFS BLAKE3 local digest-cache counter")
                        if arm == "candidate":
                            edited_consumption += usage["local_cache_hits"]
                    if usage["base_hits"] == 0 or edited_consumption == 0:
                        raise RuntimeError("Require actual base and edited-content BLAKE3 xattr consumption")
                    row["digest_consumption_verified"] = True
                    row["digest_consumption_rule"] = (
                        "base_hits and local_hashes + explicitly reported local_cache_hits positive"
                        if arm == "candidate" and "local_cache_hits" in usage else
                        "base_hits and actual local_hashes positive")
                    row["digest_counter_scope"] = "first build and all cycles; source-byte verification also reads files, not xattrs"
                    row["background_prefetch_after_shutdown"] = self.background_prefetch(name)
                except BaseException as error:
                    cleanup_errors.append({"component": "mount/counters", "type": type(error).__name__})
            row["cleanup_errors"] = cleanup_errors
            self.save()
            if cleanup_errors and not failed:
                raise RuntimeError("Owned cleanup or digest-counter verification failed; see active cell")
        row["complete"] = True
        self.result["results"].append(row)
        self.result.pop("active_cell", None)
        self.save()
        print(json.dumps({"cell": name, "first_build_seconds": row["real_wall_clone_to_first_build_seconds"],
                          "completed_cycles": len(row["cycles"])}), flush=True)

    def summarize(self):
        summary = {"percentile_method": "median p50, nearest-rank p95; no excluded samples", "arms": {}}
        for arm in ARMS:
            rows = [row for row in self.result["results"] if row["variant"] == arm]
            cycles = [cycle for row in rows for cycle in row["cycles"]]
            summary["arms"][arm] = {
                "first_clone_to_build_seconds": sample_summary([row["real_wall_clone_to_first_build_seconds"] for row in rows]),
                "first_build_process_seconds": sample_summary([row["first_build"]["phases"]["build"]["seconds"] for row in rows]),
                "cycle_seconds": sample_summary([cycle["continuous_edit_status_build_seconds"] for cycle in cycles]),
                "cycle_phase_seconds": {phase: sample_summary([cycle["phases"][phase]["seconds"] for cycle in cycles])
                                        for phase in ("edit", "git_status", "build")},
                "per_trial_cycle_seconds": {str(row["trial"]): sample_summary([cycle["continuous_edit_status_build_seconds"] for cycle in row["cycles"]]) for row in rows}}
        self.result["summary"] = summary
        return summary

    def report(self):
        summary = self.summarize()
        lines = ["# Iterative VFS / BuildBuddy RBE acceptance", "",
            self.result["scope"], "",
            f"Complete: {self.result['complete']}. Retained completed cells: {len(self.result['results'])}; unfinished cell remains in results.json.", "",
            "Server mirror copy/refresh, mappings, packs and CAS prewarm are retained excluded setup costs. Original full mirror population and image provisioning are not measured. Fresh private agents share only that owned proxy, remote services and verified archive/toolchain caches. No query precedes first build. After first build, source-fixture materialization is measured separately before steady-state cycles; it may hydrate the edited base blob. The edit phase includes bounded Python-worker startup, one-byte pwrite and fsync.", "",
            "| Arm | First clone→build p50 / p95 (s) | Edit cycles N | Cycle p50 / p95 (s) |",
            "|---|---:|---:|---:|"]
        for arm, data in summary["arms"].items():
            startup, cycle = data["first_clone_to_build_seconds"], data["cycle_seconds"]
            if startup["n"]:
                lines.append(f"| {arm} | {startup['p50']:.6f} / {startup['p95']:.6f} | {cycle['n']} | " +
                    (f"{cycle['p50']:.6f} / {cycle['p95']:.6f} |" if cycle["n"] else "— |"))
        lines += ["", "p95 uses nearest rank, so small-N p95 can equal the maximum. Cycles within a cell are correlated; per-trial medians, all samples, phases, remote executor metrics and output/source hashes are retained. Action metrics overlap and are not additive wall-time phases. No failed/outlier trial is retried or filtered. A failed matrix is not an accepted comparison.", "",
            "Each accepted build requires exactly three uncached remote BLAKE3 CppCompile/CppArchive/CppLink actions. Startup requires observed successful repository r2c2 lookup RPCs. Iterations reuse a live Bazel server and may issue zero repository RPCs; that absence is not called an r2c2 hit. Output manifests must match baseline for the same edit cycle across every arm/trial, and change versus the preceding cycle."]
        (self.root / "REPORT.md").write_text("\n".join(lines) + "\n")


def write_byte_worker(arguments):
    if len(arguments) != 3:
        raise ValueError("Internal edit worker requires path, offset and one hex byte")
    path, offset, value = Path(arguments[0]), int(arguments[1]), bytes.fromhex(arguments[2])
    if offset < 0 or len(value) != 1:
        raise ValueError("Invalid semantic edit offset/value")
    with path.open("r+b", buffering=0) as file:
        if offset >= os.fstat(file.fileno()).st_size or os.pwrite(file.fileno(), value, offset) != 1:
            raise RuntimeError("Semantic edit did not write exactly one existing byte")
        os.fsync(file.fileno())


def main():
    if sys.argv[1:2] == ["--write-byte"]:
        write_byte_worker(sys.argv[2:])
        return
    if sys.argv[1:2] == ["--read-source"]:
        if len(sys.argv) != 3:
            raise ValueError("Internal source reader requires exactly one path")
        sys.stdout.buffer.write(Path(sys.argv[2]).read_bytes())
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mirror", type=Path, required=True)
    parser.add_argument("--credential-rc", type=Path, required=True, help="Selected credential rc; not read on dry-run or copied")
    parser.add_argument("--baseline-vfs", type=Path, required=True)
    parser.add_argument("--candidate-vfs", type=Path, required=True)
    parser.add_argument("--baseline-source", required=True, help="Independently verified source revision claim for frozen binary")
    parser.add_argument("--candidate-source", required=True, help="Independently verified source revision claim for frozen binary")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--cycles", type=int, default=8)
    parser.add_argument("--functional-pilot", action="store_true", help="Permit one trial; explicitly exclude headline latency claims")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    if args.trials < (1 if args.functional_pilot else 3) or not 2 <= args.cycles <= len(EDIT_BYTES):
        parser.error(f"Require at least three rotated trials (one with --functional-pilot) and 2–{len(EDIT_BYTES)} cycles")
    if any(not re.fullmatch(r"[0-9a-f]{40}", value) for value in (args.baseline_source, args.candidate_source)):
        parser.error("Source revision claims must be full lowercase 40-hex commit IDs")
    args.workflow, args.warm_queries, args.build_proof = "build-first", 0, False
    args.post_build_query, args.proxy_cas_sync = False, True
    bench = IterativeRbeBenchmark(args)
    if not args.run:
        print(json.dumps({"ready": True, "execution": False, "arms": ARMS,
            "trials": args.trials, "cycles_per_cell": args.cycles, "target": TARGET,
            "commit": COMMIT, "digest": "BLAKE3", "proxy_cas_sync": True,
            "baseline": str(args.baseline_vfs), "candidate": str(args.candidate_vfs),
            "output": str(bench.root), "budgets_seconds": {"edit": EDIT_TIMEOUT,
                "git_status": STATUS_TIMEOUT, "remote_build": BUILD_TIMEOUT},
            "note": "No binaries, credentials, network or FUSE accessed; execute only with --run"}, indent=2))
        return
    try:
        with credential_helper(args.credential_rc) as helper:
            bench.credential = helper
            bench.load_proxy_credential()
            bench.freeze()
            bench.prepare_proxy()
            orders = [list(ARMS if trial % 2 else ARMS[::-1]) for trial in range(1, args.trials + 1)]
            bench.result["environment"]["trial_order"] = orders
            for trial, order in enumerate(orders, 1):
                for arm in order:
                    bench.cell(trial, arm)
            expected = args.trials * len(ARMS) * (args.cycles + 1)
            if len(bench.result["output_comparisons"]) != expected:
                raise RuntimeError("Incomplete cross-arm/trial same-cycle output verification")
            bench.verify_toolchain("after")
            for tool in [*bench.result["tool_provenance"].values(), *bench.result["frozen_vfs_arms"].values(), bench.result["frozen_proxy"]]:
                if sha256(Path(tool["path"])) != tool["sha256"]:
                    raise RuntimeError("A frozen executable changed during the matrix")
            for name, digest in bench.result["harness_provenance"].items():
                if sha256(bench.root / "harness" / name) != digest:
                    raise RuntimeError("A frozen harness source changed during the matrix")
            bench.result["complete"] = True
    except BaseException as error:
        bench.result["failure"] = {"type": type(error).__name__,
            "frames": [{"file": Path(frame.filename).name, "line": frame.lineno, "function": frame.name}
                       for frame in traceback.extract_tb(error.__traceback__)]}
        if isinstance(error, RuntimeError):
            bench.result["failure"]["reason"] = str(error)
        raise
    finally:
        bench.critical = False
        try:
            bench.stop_proxy()
        except BaseException as error:
            bench.result["complete"] = False
            bench.result["proxy_cleanup_failure"] = {"type": type(error).__name__}
            raise
        finally:
            bench.cas_key = None
            bench.summarize()
            bench.save()
            if bench.writable_evidence:
                bench.report()


if __name__ == "__main__":
    main()
