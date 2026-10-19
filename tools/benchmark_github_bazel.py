#!/usr/bin/env python3
"""Serial real-FUSE GitHub/Bazel comparison. No matrix runs without --run.

REST import is a diagnostic baseline adapter, never a production metadata path.
All timed setup and first-command phases are contiguous; audits follow the timer.
"""
import argparse
import collections
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import subprocess
import time
import threading
import traceback

SOURCE = "https://github.com/bazelbuild/bazel.git"
COMMIT = "c9af12d1db12d1b2df2137feb9ea8d39e3a25c79"
LABELS_SHA = "56d7a2f6140674159f5252bccc16411b6ec59f5eb30bb46571381491a32883fa"
DEFAULT_PREFETCH_COMMAND = ["{vfs}", "--git", "{git}", "prefetch-bazel", "--repo", "{repo}"]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dump(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


class Benchmark:
    def __init__(self, args):
        self.args = args
        self.root = args.output.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.config = json.loads(args.config.read_text())
        self.prefetch_mode = self.config.get("prefetch_mode", "before-mount")
        self.mount_prefetch_args = self.prefetch_mount_args()
        self.commands = []
        self.result = {"complete": False, "results": [], "config": self.config,
                       "workflow": getattr(args, "workflow", "query"),
                       "execution_options": {key: getattr(args, key, None) for key in ("trials", "warm_queries", "warm_builds", "workflow")},
                       "environment": {"platform": platform.platform(), "cpus": os.cpu_count(),
                           "os_page_cache": "not dropped", "source": SOURCE,
                           "cache_policy": "shared archive/dependency cache explicitly warmed by parent pilot; fresh output base per cell; fresh source clone; no action/disk/remote cache added by harness",
                           "timing": "continuous clone launch through first successful " + ("build" if getattr(args, "workflow", "query") == "build-first" else "query") + " process exit; no object audits in interval",
                           "build_timing": ("continuous clone-to-first-build, no prior query; verified warm content-noop builds and golden query afterward" if getattr(args, "workflow", "query") == "build-first" else "optional first build AFTER cold + warm queries, NOT clone-to-first-build"),
                           "metadata_mode": self.config["metadata_mode"],
                           "prefetch_mode": self.prefetch_mode,
                           "prefetch_timing": ("mount-owned background work overlaps Bazel; completion diagnostics are excluded from phase sums" if self.prefetch_mode == "after-mount" else "standalone blocking phase before mount"),
                           "clock_reference": {"perf_counter": time.perf_counter(), "epoch": time.time()},
                           "artifact_adapter": self.config["metadata_mode"] == "artifact-rest-import"}}
        self.env = dict(os.environ, GIT_NO_LAZY_FETCH="1")
        for key in list(self.env):
            if key in {"GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE",
                       "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES"} or key.startswith("GIT_TRACE"):
                self.env.pop(key, None)
        # TRACE2 logs commands, not HTTP payloads/headers. Never enable GIT_TRACE_CURL.
        self.env.pop("GIT_CURL_VERBOSE", None)
        self.vfs = self.root / "git-vfs-frozen"
        self.helper = None
        self.critical = False
        self.writable_evidence = False

    def save(self):
        if self.writable_evidence and not self.critical:
            dump(self.root / "commands.json", self.commands)
            dump(self.root / "results.json", self.result)

    def traced_env(self, name):
        return dict(self.env, GIT_TRACE2_EVENT=str(self.root / (name + ".trace2.jsonl")),
                    GIT_TRACE_REDACT="1")

    def run(self, argv, name, cwd=None, timeout=600, check=True):
        argv = [str(value) for value in argv]
        cwd = cwd or self.root
        start = time.perf_counter()
        timed_out = False
        with (self.root / (name + ".stdout")).open("wb") as out, (self.root / (name + ".stderr")).open("wb") as err:
            process = subprocess.Popen(argv, cwd=cwd, env=self.traced_env(name), stdout=out,
                                       stderr=err, start_new_session=True)
            def expire():
                nonlocal timed_out
                timed_out = True
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            timer = threading.Timer(timeout, expire)
            timer.daemon = True
            timer.start()
            try:
                process.wait()  # Blocking wait avoids polling latency in short warm queries.
                ended = time.perf_counter()
            finally:
                timer.cancel()
        result = {"seconds": ended - start, "returncode": process.returncode,
                  "timed_out": timed_out, "start_perf_counter": start,
                  "wait_end_perf_counter": ended}
        self.commands.append({"name": name, "argv": argv, "cwd": str(cwd),
                              "trace2": name + ".trace2.jsonl", **result})
        self.save()
        if check and process.returncode:
            raise RuntimeError(f"{name} failed ({process.returncode}); see {name}.stderr")
        return result

    def git(self, repo, args, data=None):
        return subprocess.check_output([self.config["git"], "--no-replace-objects", "-C", str(repo), *args],
                                       env=self.env, input=data, stderr=subprocess.PIPE)

    def vfs_args(self, *args):
        return [self.vfs, "--git", self.config["git"], *args]

    def bazel_args(self, base):
        return [self.config["bazel"], "--nosystem_rc", "--nohome_rc",
                "--output_user_root=" + self.config["output_user_root"],
                "--output_base=" + str(base), "--digest_function=GITSHA1",
                "--unix_digest_hash_attribute_name=user.gitsha1"]

    def expand(self, command, repo):
        values = {"vfs": str(self.vfs), "git": self.config["git"], "repo": str(repo)}
        return [part.format(**values) for part in command]

    def prefetch_mount_args(self):
        if self.prefetch_mode not in {"before-mount", "after-mount"}:
            raise ValueError("prefetch_mode must be before-mount or after-mount")
        if self.prefetch_mode == "before-mount":
            return []
        command = self.config.get("prefetch_command", DEFAULT_PREFETCH_COMMAND)
        if not isinstance(command, list) or command[:len(DEFAULT_PREFETCH_COMMAND)] != DEFAULT_PREFETCH_COMMAND:
            raise ValueError("after-mount requires the canonical prefetch-bazel command prefix; custom commands cannot be translated safely")
        result = ["--prefetch-bazel"]
        suffix = iter(command[len(DEFAULT_PREFETCH_COMMAND):])
        for argument in suffix:
            option, separator, value = argument.partition("=")
            if option == "--no-discover" and not separator:
                result.append("--prefetch-no-discover")
            elif option in {"--path", "--include"}:
                if not separator:
                    value = next(suffix, None)
                if value is None or value.startswith("--"):
                    raise ValueError(f"Missing value for prefetch option {option}")
                result.extend(["--prefetch-" + option[2:], value])
            else:
                raise ValueError(f"Unsupported after-mount prefetch option: {argument}")
        return result

    def background_prefetch(self, name):
        evidence = {"overlaps_critical_phases": True, "included_in_phase_sum": False,
                    "log": name + "-mount.stderr", "status": "pending"}
        for line in (self.root / evidence["log"]).read_text(errors="replace").splitlines(keepends=True):
            if not line.endswith("\n"):
                continue  # The worker may still be writing when the first command exits.
            for prefix, status in (("Background prefetch: ", "complete"),
                                   ("Background prefetch failed: ", "failed")):
                if line.startswith(prefix):
                    completion = json.loads(line[len(prefix):])
                    evidence.update({"status": "cancelled" if completion.get("cancelled") else status,
                                     "completion": completion})
        return evidence

    def freeze(self):
        if (self.root / "results.json").exists():
            raise RuntimeError("Output contains results.json; use a fresh output directory, never overwrite evidence")
        self.writable_evidence = True
        if self.config["source"] != SOURCE or self.config["commit"] != COMMIT:
            raise ValueError("This workload is pinned to the specified GitHub Bazel snapshot")
        if self.config["golden_label_count"] != 4213 or self.config["golden_labels_sha256"] != LABELS_SHA:
            raise ValueError("Unexpected golden query output")
        if self.config["metadata_mode"] not in {"artifact-rest-import", "production-warm-sizes"}:
            raise ValueError("Unknown metadata_mode")
        if not self.config.get("pilot_dependencies_warm_verified"):
            raise ValueError("Parent pilot must verify dependency/archive warmth before starting")
        shutil.copy2(self.config["vfs_binary"], self.vfs)
        self.result["frozen_binary"] = {"path": str(self.vfs), "sha256": sha256(self.vfs),
                                        "input_path": self.config["vfs_binary"],
                                        "source_revision_claim": self.config.get("vfs_source_revision_verified", None)}
        if self.config["metadata_mode"] == "artifact-rest-import":
            self.helper = self.root / "github-tree-size-import-frozen"
            shutil.copy2(self.config["bootstrap_helper"], self.helper)
            self.result["artifact_helper"] = {"path": str(self.helper), "sha256": sha256(self.helper),
                                              "production": False}
        for name, command in [("git-version", [self.config["git"], "--version"]),
                              ("bazel-version", [self.config["bazel"], "--version"]),
                              ("vfs-version", self.vfs_args("--version"))]:
            self.run(command, name)
        actual = (self.root / "bazel-version.stdout").read_text().strip()
        if actual != self.config["bazel_version"]:
            raise RuntimeError(f"Bazel version mismatch: {actual}")
        if not Path("/dev/fuse").exists():
            raise RuntimeError("Real /dev/fuse required")
        dump(self.root / "frozen-config.json", self.config)
        self.save()

    def clone(self, repo, name, checkout):
        command = [self.config["git"], "-c", "protocol.version=2", "clone", "--depth=1",
                   "--revision=" + self.config["commit"]]
        if not checkout:
            command += ["--bare", "--filter=blob:none"]
        return self.run(command + [self.config["source"], repo], name + "-clone")

    def presence(self, repo, name, phase, overlay=None):
        """Audit fixed-snapshot availability in base AND private admin, outside timing.

        Existing present_* fields retain their historical base-only meaning.
        private_available_* includes alternates; it is not private-store disk usage.
        """
        entries = []
        for record in self.git(repo, ["ls-tree", "-r", "-z", self.config["commit"]]).split(b"\0"):
            if not record:
                continue
            header, path = record.split(b"\t", 1)
            mode, kind, oid = header.split()
            if kind == b"blob":
                entries.append({"path": os.fsdecode(path), "path_hex": path.hex(),
                                "mode": mode.decode(), "oid": oid.decode()})
        ids = sorted({entry["oid"] for entry in entries})
        data = ("\n".join(ids) + "\n").encode()

        def available(repository):
            raw = self.git(repository, ["cat-file", "--batch-check=%(objectname) %(objectsize)"], data)
            found = {}
            for line in raw.splitlines():
                fields = line.split()
                if fields[-1] != b"missing":
                    found[fields[0].decode()] = int(fields[1])
            return found

        present = available(repo)
        hydrated = [dict(entry, size_bytes=present[entry["oid"]],
                          justification=self.justification(entry["path"], entry["mode"]))
                    for entry in entries if entry["oid"] in present]
        counts = {"snapshot_unique_blobs": len(ids), "snapshot_blob_paths": len(entries),
                  "present_blobs": len(present), "hydrated_paths": len(hydrated),
                  "present_logical_unique_bytes": sum(present.values()), "present_oids": sorted(present)}
        payload = {"phase": phase, "audit_outside_timing": True,
                   "snapshot_commit": self.config["commit"], "tree_source": str(repo),
                   "git_env": {"GIT_NO_LAZY_FETCH": "1"}, "hydrated_entries": hydrated,
                   "legacy_fields_scope": "present_* and hydrated_entries describe BASE ONLY; use union_* for complete mounted-snapshot availability",
                   "byte_semantics": "uncompressed unique Git blob bytes, NOT network bytes or physical private-store bytes"}
        if overlay is not None:
            private = Path(overlay) / "git"
            if not private.is_dir():
                raise RuntimeError(f"Private Git admin missing: {private}; refusing incomplete VFS audit")
            private_available = available(private)
            for oid in present.keys() & private_available.keys():
                if present[oid] != private_available[oid]:
                    raise RuntimeError(f"Base/private object size conflict for {oid}")
            union = present | private_available
            private_only = private_available.keys() - present.keys()
            counts.update({"audit_scope": "base_and_private_admin_with_alternates",
                           "base_present_blobs": len(present),
                           "base_present_logical_unique_bytes": sum(present.values()),
                           "private_available_blobs": len(private_available),
                           "private_available_logical_unique_bytes": sum(private_available.values()),
                           "private_available_oids": sorted(private_available),
                           "private_only_blobs": len(private_only),
                           "private_only_logical_unique_bytes": sum(private_available[oid] for oid in private_only),
                           "private_only_oids": sorted(private_only),
                           "union_present_blobs": len(union),
                           "union_present_logical_unique_bytes": sum(union.values()),
                           "union_present_oids": sorted(union)})
            payload["private_admin"] = str(private)
            payload["private_available_semantics"] = "Git object availability through private admin, INCLUDING base alternates"
            payload["private_only_entries"] = [dict(entry, size_bytes=private_available[entry["oid"]],
                                                       justification=self.justification(entry["path"], entry["mode"]))
                                               for entry in entries if entry["oid"] in private_only]
        else:
            counts["audit_scope"] = "base_only_no_private_admin_requested"
        filename = name + "-presence-" + phase + ".json"
        payload["counts"] = counts
        dump(self.root / filename, payload)
        return dict(counts, manifest=filename)

    @staticmethod
    def justification(path, mode):
        leaf = Path(path).name
        if mode == "120000":
            return "symlink target"
        if leaf in {"BUILD", "BUILD.bazel", "WORKSPACE", "WORKSPACE.bazel", "MODULE.bazel", "REPO.bazel"} or path.endswith(".bzl"):
            return "Bazel/Starlark metadata"
        if leaf.startswith(".bazel") or path.endswith(".bazelrc"):
            return "Bazel startup/configuration"
        return "other repository-loading/build input; inspect query log/trace before assigning causal justification"

    def footprint(self, repo, overlay):
        files = []
        for source, base in [("source", repo), ("overlay", overlay)]:
            if base is None or not base.exists():
                continue
            for directory, dirs, names in os.walk(base, followlinks=False):
                links = [leaf for leaf in dirs if (Path(directory) / leaf).is_symlink()]
                for leaf in names + links:
                    path = Path(directory) / leaf
                    try:
                        stat = path.lstat()
                    except FileNotFoundError:
                        continue
                    files.append({"path": str(path.relative_to(self.root)), "source": source,
                                  "apparent_bytes": stat.st_size, "allocated_bytes": stat.st_blocks * 512})
        return {"apparent_bytes": sum(f["apparent_bytes"] for f in files),
                "allocated_bytes": sum(f["allocated_bytes"] for f in files), "files": files,
                "scope": "source + overlay files/symlinks; excludes output bases, shared caches and directory inode overhead"}

    def mount(self, repo, overlay, mountpoint, name):
        mountpoint.mkdir()
        command = [str(value) for value in self.vfs_args("mount", "--repo", repo, "--overlay", overlay,
                                                       *self.expand(self.mount_prefetch_args, repo), mountpoint)]
        log = (self.root / (name + "-mount.stderr")).open("wb")
        start = time.perf_counter()
        process = subprocess.Popen(command, cwd=self.root, env=self.traced_env(name + "-mount"),
                                   stdout=subprocess.DEVNULL, stderr=log, start_new_session=True)
        state = (process, log)
        try:
            while not os.path.ismount(mountpoint):
                if process.poll() is not None or time.perf_counter() - start > 60:
                    raise RuntimeError(name + " mount failed; inspect mount stderr")
                time.sleep(.005)
            seconds = time.perf_counter() - start
            self.commands.append({"name": name + "-mount", "argv": command, "cwd": str(self.root),
                                  "seconds": seconds, "pid": process.pid,
                                  "trace2": name + "-mount.trace2.jsonl"})
            return state, {"seconds": seconds, "returncode": 0}
        except BaseException:
            self.stop_mount(mountpoint, state)
            raise

    def stop_mount(self, mountpoint, state):
        process, log = state
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            subprocess.run(["fusermount3", "-u", str(mountpoint)], capture_output=True)
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        log.close()
        if os.path.ismount(mountpoint):
            raise RuntimeError(f"Cleanup failed: {mountpoint} still mounted")

    def stop_bazel(self, base, cwd, name):
        result = self.run(self.bazel_args(base) + ["shutdown"], name + "-shutdown", cwd=cwd,
                          timeout=60, check=False)
        if result["returncode"]:
            pid_file = base / "server/server.pid.txt"
            if pid_file.exists():
                pid = int(pid_file.read_text())
                cmdline = Path("/proc") / str(pid) / "cmdline"
                if cmdline.exists() and str(base).encode() in cmdline.read_bytes():
                    os.kill(pid, signal.SIGTERM)
            raise RuntimeError(f"{name} Bazel shutdown failed; fallback SIGTERM attempted")

    def warm_sizes(self, repo, name, phases):
        if self.config["metadata_mode"] == "production-warm-sizes":
            command = self.config.get("warm_sizes_command", ["{vfs}", "--git", "{git}", "warm-sizes", "--repo", "{repo}"])
            phases["warm_sizes"] = self.run(self.expand(command, repo), name + "-warm-sizes")
        else:
            phases["resolve_root_tree"] = self.run([self.config["git"], "--git-dir", repo,
                                                       "rev-parse", "HEAD^{tree}"], name + "-root-tree")
            tree = (self.root / (name + "-root-tree.stdout")).read_text().strip()
            if len(tree) != 40 or any(c not in "0123456789abcdef" for c in tree):
                raise RuntimeError("Invalid root tree ID")
            url = self.config["github_tree_api_prefix"] + tree + "?recursive=1"
            tree_json = self.root / (name + "-github-tree.json")
            phases["github_tree_download"] = self.run(["curl", "--fail", "--silent", "--show-error",
                "--location", "--header", "Accept: application/vnd.github+json", "--output", tree_json,
                "--write-out", "%{http_code} %{size_download} %{time_total}\n", url], name + "-github-tree", timeout=180)
            phases["metadata_import"] = self.run([self.helper, self.config["git"], repo, tree_json], name + "-metadata-import")

    def verify_labels(self, name, phase):
        labels = sorted((self.root / (name + "-" + phase + ".stdout")).read_bytes().splitlines())
        digest = hashlib.sha256(b"\n".join(labels)).hexdigest()
        if len(labels) != self.config["golden_label_count"] or digest != self.config["golden_labels_sha256"]:
            raise RuntimeError(f"{name}/{phase} query labels differ: {len(labels)} / {digest}")
        return {"label_count": len(labels), "labels_sha256": digest}

    def build_command(self, base, target, name, artifact="build"):
        flags = ["--repository_cache=" + self.config["repository_cache"], "--watchfs",
                 "--lockfile_mode=off", "--noshow_progress", "--color=no", "--curses=no",
                 *self.config.get("build_flags", [])]
        return self.bazel_args(base) + ["build", target, *flags,
            "--profile=" + str(self.root / (name + "-" + artifact + ".profile.gz")),
            "--build_event_json_file=" + str(self.root / (name + "-" + artifact + ".bep.jsonl"))]

    @staticmethod
    def canonical_outputs(outputs):
        if not isinstance(outputs, list) or not outputs:
            raise RuntimeError("Build output manifest must be a nonempty list")
        canonical = []
        seen = set()
        for item in outputs:
            if not isinstance(item, dict) or set(item) != {"path", "sha256", "size_bytes"}:
                raise RuntimeError("Invalid build output manifest entry")
            path, digest, size = item["path"], item["sha256"], item["size_bytes"]
            if (not isinstance(path, str) or not path or Path(path).is_absolute()
                    or ".." in Path(path).parts or path in seen
                    or not isinstance(digest, str) or len(digest) != 64
                    or any(c not in "0123456789abcdef" for c in digest)
                    or type(size) is not int or size < 0):
                raise RuntimeError("Invalid path/hash/size in build output manifest")
            seen.add(path)
            canonical.append(dict(item))
        return sorted(canonical, key=lambda item: item["path"])

    def verify_build_outputs(self, outputs, name, phase, checkout):
        actual = self.canonical_outputs(outputs)
        reference = self.result.get("build_output_reference")
        if reference is None:
            golden = self.config.get("golden_build_outputs")
            if golden is not None:
                reference = {"source": "config.golden_build_outputs", "outputs": self.canonical_outputs(golden)}
            elif checkout:
                reference = {"source": name + "/" + phase + " (first verified checkout)", "outputs": actual}
            else:
                raise RuntimeError("No frozen build golden or verified checkout reference available")
            self.result["build_output_reference"] = reference
        if actual != reference["outputs"]:
            raise RuntimeError(f"{name}/{phase}: build path/hash/size manifest differs from matrix reference")
        self.result.setdefault("build_output_comparisons", []).append(
            {"cell": name, "phase": phase, "matches_reference": True,
             "reference_source": reference["source"], "outputs": actual})

    def completed_build_consistency(self):
        rows = [row for row in self.result["results"] if row.get("build_target")]
        if not rows:
            return
        reference = self.result.get("build_output_reference")
        if reference is None:
            raise RuntimeError("Matrix has builds but no verified output reference")
        for row in rows:
            if self.canonical_outputs(row["build_outputs"]) != reference["outputs"]:
                raise RuntimeError("Completed cell build differs from matrix reference")
        self.result["build_output_consistency"] = {
            "verified": True, "cells": len(rows),
            "build_commands": len(self.result["build_output_comparisons"]),
            "reference_source": reference["source"], "outputs": reference["outputs"]}

    def build_evidence(self, cwd, name, artifact="build"):
        outputs = []
        for relative in self.config.get("build_output_paths", []):
            path = cwd / relative
            outputs.append({"path": relative, "sha256": sha256(path), "size_bytes": path.stat().st_size})
        metrics = None
        for line in (self.root / (name + "-" + artifact + ".bep.jsonl")).read_text().splitlines():
            event = json.loads(line)
            build_metrics = event.get("buildMetrics")
            if isinstance(build_metrics, dict) and "actionSummary" in build_metrics:
                candidate = build_metrics["actionSummary"]
                recognized = {"actionsCreated", "actionsExecuted", "actionsCreatedNotIncludingAspects", "actionData"}
                if isinstance(candidate, dict) and candidate and set(candidate) & recognized:
                    metrics = candidate
        evidence = {"outputs": outputs, "action_summary": metrics, "valid_action_summary": False}
        if metrics is None:
            return evidence

        def count(value):
            if type(value) is int and value >= 0:
                return value
            if isinstance(value, str) and value.isascii() and value.isdecimal():
                return int(value)
            raise ValueError("Invalid BEP action count")

        try:
            for key in ("actionsCreated", "actionsExecuted", "actionsCreatedNotIncludingAspects"):
                if key in metrics:
                    count(metrics[key])
            total = count(metrics.get("actionsExecuted", 0))
            data = metrics.get("actionData", [])
            if not isinstance(data, list):
                raise ValueError("Invalid BEP actionData")
            known, content = 0, 0
            for item in data:
                if not isinstance(item, dict) or not isinstance(item.get("mnemonic"), str) or not item["mnemonic"]:
                    raise ValueError("Invalid BEP action mnemonic")
                executed = count(item.get("actionsExecuted", 0))
                known += executed
                if item["mnemonic"] != "BazelWorkspaceStatusAction":
                    content += executed
            if known != total:
                raise ValueError("BEP action counts are incomplete or inconsistent")
            evidence.update({"valid_action_summary": True, "actions_executed": total,
                             "content_actions_executed": content})
        except ValueError as error:
            evidence["action_summary_error"] = str(error)
        return evidence

    def cell(self, trial, variant):
        name = f"trial-{trial}-{variant}"
        checkout = variant == "checkout"
        workflow = self.args.workflow
        repo = self.root / (name + ("-checkout" if checkout else ".git"))
        overlay = None if checkout else self.root / (name + "-overlay")
        mountpoint = self.root / (name + "-mount")
        base = self.root / (name + "-output")
        row = {"trial": trial, "variant": variant, "workflow": workflow,
               "prefetch_mode": self.prefetch_mode if not checkout else "not-applicable", "phases": {}, "complete": False}
        target = self.args.build_target or self.config.get("build_target")
        self.result["active_cell"] = row
        self.save()
        state, bazel_started, cwd = None, False, repo
        self.critical = True
        try:
            row["phases"]["clone"] = self.clone(repo, name, checkout)
            if not checkout:
                self.warm_sizes(repo, name, row["phases"])
                if self.prefetch_mode == "before-mount":
                    command = self.config.get("prefetch_command", DEFAULT_PREFETCH_COMMAND)
                    row["phases"]["prefetch"] = self.run(self.expand(command, repo), name + "-prefetch")
                state, row["phases"]["mount"] = self.mount(repo, overlay, mountpoint, name)
                cwd = mountpoint
            flags = list(dict.fromkeys(["--output=label", "--lockfile_mode=off",
                "--repository_cache=" + self.config["repository_cache"], "--noshow_progress", "--color=no",
                "--curses=no", "--watchfs", *self.config.get("query_flags", [])]))
            query = self.bazel_args(base) + ["query", self.config["query_expression"], *flags]
            bazel_started = True
            if workflow == "query":
                first = "cold_query"
                command = query + ["--profile=" + str(self.root / (name + "-cold_query.profile.gz"))]
            else:
                first = "first_build"
                command = self.build_command(base, target, name, first)
            row["phases"][first] = self.run(command, name + "-" + first, cwd=cwd)
            wall = row["phases"][first]["wait_end_perf_counter"] - row["phases"]["clone"]["start_perf_counter"]
            metric = "first_query" if workflow == "query" else "first_build"
            row["real_wall_clone_to_" + metric + "_seconds"] = wall
            row["setup_plus_" + metric + "_phase_sum_seconds"] = sum(p["seconds"] for p in row["phases"].values())
            self.critical = False
            if not checkout and self.prefetch_mode == "after-mount":
                row["background_prefetch_at_first_command_exit"] = self.background_prefetch(name)
            if self.git(repo, ["rev-parse", "HEAD"]).decode().strip() != self.config["commit"]:
                raise RuntimeError("Unexpected HEAD")
            if self.git(repo, ["remote", "get-url", "origin"]).decode().strip() != SOURCE:
                raise RuntimeError("Unexpected origin")
            gitdir = repo / ".git" if checkout else repo
            if (gitdir / "objects/info/alternates").exists():
                raise RuntimeError("Unexpected shared object store")
            if workflow == "query":
                for index in range(self.args.warm_queries + 1):
                    phase = "cold_query" if index == 0 else f"warm_query_{index}"
                    if index:
                        row["phases"][phase] = self.run(query + ["--profile=" + str(self.root / (name + "-" + phase + ".profile.gz"))], name + "-" + phase, cwd=cwd)
                    row.update(self.verify_labels(name, phase))
                    row.setdefault("object_phases", {})["after_" + phase] = self.presence(repo, name, "after_" + phase, overlay=overlay)
                if target:
                    phase = "incremental_first_build_after_queries"
                    row["phases"][phase] = self.run(self.build_command(base, target, name), name + "-" + phase, cwd=cwd)
                    evidence = self.build_evidence(cwd, name)
                    self.verify_build_outputs(evidence["outputs"], name, phase, checkout)
                    row["build_outputs"] = evidence["outputs"]
                    row["build_phases"] = {phase: evidence}
                    row["build_target"] = target
                    row["object_phases"]["after_build"] = self.presence(repo, name, "after_build", overlay=overlay)
            else:
                evidence = self.build_evidence(cwd, name, first)
                self.verify_build_outputs(evidence["outputs"], name, first, checkout)
                row["build_outputs"] = evidence["outputs"]
                row["build_phases"] = {first: evidence}
                row["build_target"] = target
                row["object_phases"] = {"after_first_build": self.presence(repo, name, "after_first_build", overlay=overlay)}
                for index in range(1, self.args.warm_builds + 1):
                    phase = f"warm_build_{index}"
                    row["phases"][phase] = self.run(self.build_command(base, target, name, phase), name + "-" + phase, cwd=cwd)
                    warm = self.build_evidence(cwd, name, phase)
                    if warm["outputs"] != evidence["outputs"]:
                        raise RuntimeError(name + ": warm build changed output hashes")
                    self.verify_build_outputs(warm["outputs"], name, phase, checkout)
                    if not warm["valid_action_summary"] or warm.get("content_actions_executed") != 0:
                        raise RuntimeError(name + ": warm build was not a verified content no-op")
                    warm["verified_content_noop"] = True
                    row["build_phases"][phase] = warm
                    row["object_phases"]["after_" + phase] = self.presence(repo, name, "after_" + phase, overlay=overlay)
                row["verified_noop_builds"] = self.args.warm_builds
                phase = "post_build_query"
                row["phases"][phase] = self.run(query + ["--profile=" + str(self.root / (name + "-" + phase + ".profile.gz"))], name + "-" + phase, cwd=cwd)
                row.update(self.verify_labels(name, phase))
                row["object_phases"]["after_" + phase] = self.presence(repo, name, "after_" + phase, overlay=overlay)
            row["footprint_mounted"] = self.footprint(repo, overlay)
            self.stop_bazel(base, cwd, name)
            bazel_started = False
        finally:
            self.critical = False
            if bazel_started:
                try:
                    self.stop_bazel(base, cwd, name + "-failure")
                except BaseException as error:
                    row["bazel_cleanup_error"] = repr(error)
            if state:
                self.stop_mount(mountpoint, state)
                if self.prefetch_mode == "after-mount":
                    row["background_prefetch"] = self.background_prefetch(name)
            self.save()
        row["footprint_after_close"] = self.footprint(repo, overlay)
        row["complete"] = True
        self.result["results"].append(row)
        self.result.pop("active_cell", None)
        self.save()
        print(json.dumps({"name": name, "workflow": workflow, ("clone_to_query" if workflow == "query" else "clone_to_first_build"): wall,
                          first: row["phases"][first]["seconds"]}), flush=True)

    def native_probe(self):
        repo = self.root / "native-size-probe.git"
        self.clone(repo, "native-size-probe", False)
        before = self.presence(repo, "native-size-probe", "before_warm")
        command = self.config.get("native_warm_sizes_command")
        if command is None:
            command = ["{vfs}", "--git", "{git}", "warm-sizes", "--repo", "{repo}"]
            if self.config["metadata_mode"] == "production-warm-sizes":
                command += ["--size-source", "git"]
        status = self.run(self.expand(command, repo), "native-size-probe-warm", check=False)
        after = self.presence(repo, "native-size-probe", "after_warm")
        dump(self.root / "native-size-probe.json", {"diagnostic_only": True, "before": before,
             "after": after, "warm_sizes": status, "failure_retained": status["returncode"] != 0})

    def trace_summary(self):
        reference = self.result["environment"].get("clock_reference")
        if reference is None and (self.root / "clock-calibration.json").exists():
            reference = json.loads((self.root / "clock-calibration.json").read_text())
        if reference is None:
            return
        offset = reference["epoch"] - reference["perf_counter"]
        summary = []
        commands = {"cat-file", "fetch", "ls-tree", "rev-parse", "config", "status", "diff-index", "init", "index-pack", "maintenance", "remote-https"}
        for row in self.result["results"]:
            if row["variant"] != "vfs":
                continue
            path = self.root / f"trial-{row['trial']}-vfs-mount.trace2.jsonl"
            if not path.exists():
                continue
            starts = {}
            phases = collections.defaultdict(lambda: collections.defaultdict(lambda: {"processes": 0, "summed_process_wall_seconds": 0.0}))
            for line in path.read_text().splitlines():
                event = json.loads(line)
                if event.get("event") == "start":
                    argv = event.get("argv", [])
                    command = next((arg for arg in argv[1:] if arg in commands), "other")
                    epoch = datetime.datetime.fromisoformat(event["time"].replace("Z", "+00:00")).timestamp()
                    phase = next((name for name, data in row["phases"].items()
                                  if "start_perf_counter" in data and data["start_perf_counter"] + offset <= epoch <= data["wait_end_perf_counter"] + offset), "mount_or_handover")
                    starts[event["sid"]] = (phase, command)
                    phases[phase][command]["processes"] += 1
                elif event.get("event") == "exit" and event["sid"] in starts:
                    phase, command = starts[event["sid"]]
                    phases[phase][command]["summed_process_wall_seconds"] += event.get("t_abs", 0.0)
            summary.append({"trial": row["trial"], "phases": phases})
        dump(self.root / "native-process-attribution.json", {"clock_reference": reference,
             "semantics": "Mount-child native Git process starts grouped by command and start phase. Wall sums include nested/overlapping processes and MUST NOT be summed as critical path latency.", "results": summary})

    def report(self):
        build_first = self.result.get("workflow", "query") == "build-first"
        first_label = "first build" if build_first else "query"
        wall_key = "real_wall_clone_to_first_build_seconds" if build_first else "real_wall_clone_to_first_query_seconds"
        phase_key = "first_build" if build_first else "cold_query"
        warm_prefix = "warm_build_" if build_first else "warm_query_"
        lines = ["# GitHub/Bazel real-FUSE benchmark", "", f"Matrix complete: `{self.result['complete']}`; error: `{self.result.get('error', 'none')}`.",
                 "", f"Workflow: `{self.result.get('workflow', 'query')}`; metadata mode: `{self.config['metadata_mode']}`; prefetch mode: `{self.prefetch_mode}`.",
                 "", "Shared archive/dependency caches warm; fresh source clones and independent output bases. OS caches not dropped.",
                 "", f"| Arm | Samples | Clone → {first_label} median / range (s) | First command median (s) | Warm command median (s) |",
                 "|---|---:|---:|---:|---:|"]
        for variant in ("checkout", "vfs"):
            rows = [r for r in self.result["results"] if r["variant"] == variant]
            if not rows:
                continue
            wall = [r[wall_key] for r in rows]
            cold = [r["phases"][phase_key]["seconds"] for r in rows]
            warm = [p["seconds"] for r in rows for name, p in r["phases"].items() if name.startswith(warm_prefix)]
            lines.append(f"| {variant} | {len(rows)} | {statistics.median(wall):.6f} / {min(wall):.6f}–{max(wall):.6f} | {statistics.median(cold):.6f} | {statistics.median(warm) if warm else float('nan'):.6f} |")
        lines += ["", "Raw evidence: `results.json`, `commands.json`, per-phase stdout/stderr and TRACE2 JSONL, object-presence/path manifests, optional build profile/BEP.",
                  "", "Historical present_* fields count BASE ONLY. Mounted VFS audits additionally cover overlay/git (including alternates); union_present_* and private_only_* reveal private-admin hydration that base-only counts conceal.",
                  "", "Logical blob bytes are not transport bytes. TRACE2 permits native fetch/process attribution but does not measure encrypted Git network bytes. No HTTP header tracing is enabled."]
        if self.prefetch_mode == "after-mount":
            lines += ["", "Size warming remains blocking. Mount-owned prefetch runs concurrently with Bazel; its stderr completion duration/report is diagnostic overlapping work, not an additional critical-path phase. Continuous clone-to-first-success timing is unchanged. Completion status is captured after the first command and again after mount shutdown; no prefetch wait or object audit interrupts the timer."]
        consistency = self.result.get("build_output_consistency")
        if consistency:
            lines += ["", f"Cross-arm/trial build output consistency: **verified** for {consistency['cells']} cells / {consistency['build_commands']} build commands against `{consistency['reference_source']}`. Exact path/hash/size reference and individual checks are retained in results.json."]
        if build_first:
            count = sum(r.get("verified_noop_builds", 0) for r in self.result["results"])
            lines += ["", f"The first Bazel command is build, not query. Continuous cold wall stops at successful build exit, before hashes/audits. Verified content-noop warm builds: **{count}**; each retains exact output hashes and executes zero content actions. Unconditional BazelWorkspaceStatusAction bookkeeping may still execute and is recorded separately. A golden-label query follows all builds, outside first-build timing."]
        else:
            lines += ["", "Optional build is measured after all queries; it is not clone-to-first-build. Build output hashes require pilot-specified `build_output_paths`."]
        (self.root / "REPORT.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trials", type=int, default=5, help="rotated paired trials (default 5)")
    parser.add_argument("--warm-queries", type=int, default=3)
    parser.add_argument("--workflow", choices=["query", "build-first"], default="query")
    parser.add_argument("--warm-builds", type=int, default=3, help="verified content-noop repeats in build-first workflow only")
    parser.add_argument("--build-target", help="build target; required via CLI/config for build-first")
    parser.add_argument("--native-probe", action="store_true", help="retain standalone native GitHub size-capability result")
    parser.add_argument("--run", action="store_true", help="explicitly authorize execution; otherwise validate configuration only")
    args = parser.parse_args()
    if args.trials < 1 or args.warm_queries < 0 or args.warm_builds < 0:
        parser.error("trials must be positive and warm repeat counts nonnegative")
    bench = Benchmark(args)
    if args.workflow == "build-first" and not (args.build_target or bench.config.get("build_target")):
        parser.error("build-first requires a build_target")
    if (args.build_target or bench.config.get("build_target")) and not bench.config.get("build_output_paths"):
        parser.error("Configured builds require nonempty build_output_paths for cross-arm correctness")
    if not args.run:
        print(json.dumps({"ready": True, "execution": False, "config": bench.config,
                          "output": str(bench.root), "trials": args.trials}, indent=2))
        return
    try:
        bench.freeze()
        if args.native_probe:
            bench.native_probe()
        orders = [["checkout", "vfs"] if trial % 2 else ["vfs", "checkout"]
                  for trial in range(1, args.trials + 1)]
        bench.result["environment"]["trial_order"] = orders
        for trial, order in enumerate(orders, 1):
            for variant in order:
                bench.cell(trial, variant)
        bench.completed_build_consistency()
        bench.result["complete"] = True
    except BaseException as error:
        bench.critical = False
        bench.result["error"] = repr(error)
        bench.result["traceback"] = traceback.format_exc()
        raise
    finally:
        bench.save()
        if bench.writable_evidence:
            bench.report()
            bench.trace_summary()


if __name__ == "__main__":
    main()
