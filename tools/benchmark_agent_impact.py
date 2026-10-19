#!/usr/bin/env python3
"""Agent source-access journeys and restart-cache evidence (Linux, dry-run by default).

This is NOT an RBE/build benchmark or a network-byte measurement. Each cell owns
a copied warm mirror, loopback proxy and fresh independent client workspaces.
Use the existing benchmark_proxy_agent_rbe.py for the complementary build test.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path
import platform
import select
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.request

from benchmark_github_bazel import COMMIT, dump, sha256


def read_manifest(root, manifest):
    """Run in a child so blocked FUSE reads have the same fixed timeout as Git."""
    total = 0
    for item in json.loads(Path(manifest).read_text()):
        data = (Path(root) / item["path"]).read_bytes()
        oid = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        if oid != item["oid"]:
            raise ValueError("content mismatch: " + item["path"])
        total += len(data)
    print(json.dumps({"verified": True, "files": len(json.loads(Path(manifest).read_text())),
                      "logical_bytes_read": total}))


def disk_usage(path):
    """Metadata-only audit, after timing; avoid descending through mountpoints."""
    logical = allocated = files = 0
    for root, dirs, names in os.walk(path, followlinks=False):
        dirs[:] = [d for d in dirs if not os.path.ismount(Path(root) / d)]
        for name in names:
            stat = (Path(root) / name).lstat()
            logical += stat.st_size
            allocated += stat.st_blocks * 512
            files += 1
    return {"logical_file_bytes": logical, "allocated_bytes": allocated, "files": files}


def filesystem_identity(path):
    try:
        value = subprocess.check_output(["stat", "-f", "--format=%T:%i", str(path)],
            stderr=subprocess.DEVNULL, timeout=5).decode().strip()
        kind, identity = value.split(":", 1)
        return {"type": kind, "id": identity}
    except (OSError, ValueError, subprocess.SubprocessError):
        return {"type": "unknown", "id": None}


def delta(before, after):
    result = {}
    for key, value in after.items():
        if isinstance(value, dict):
            result[key] = delta(before.get(key, {}), value)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            result[key] = value - before.get(key, 0)
    return result


def trace_summary(output, prefix):
    """Count observed native-Git fetch launches, not bytes or network requests."""
    summary = {"files": [], "native_fetch_processes": 0, "fetch_child_launches": 0, "fetch_commands": [], "malformed_lines": 0}
    for path in sorted(output.glob(prefix + "*.trace2.jsonl")):
        summary["files"].append(path.name)
        for line in path.read_text(errors="replace").splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                summary["malformed_lines"] += 1
                continue
            argv = event.get("argv", [])
            if event.get("event") == "start" and "fetch" in argv:
                summary["native_fetch_processes"] += 1
                summary["fetch_commands"].append(argv)
            if event.get("event") == "child_start" and "fetch" in argv:
                summary["fetch_child_launches"] += 1
    return summary


def process_tree_rss(pid, limit=1024):
    """Best-effort instantaneous RSS, including children of every process thread.

    Processes can exit during the walk. Summing RSS double-counts shared pages;
    this is a snapshot after reads, not a lifetime peak or unique-memory count.
    """
    pending, visited = [pid], set()
    rss = root_rss = unreadable = child_lists_unreadable = 0
    truncated = False
    while pending and len(visited) < limit:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        proc = Path("/proc") / str(current)
        try:
            value = int((proc / "statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
            rss += value
            if current == pid:
                root_rss = value
        except (OSError, ValueError, IndexError):
            unreadable += 1
        try:
            tasks = list((proc / "task").iterdir())
        except OSError:
            child_lists_unreadable += 1
            continue
        truncated = truncated or len(tasks) > limit
        for task in tasks[:limit]:
            try:
                children = (task / "children").read_text().split()
                # Bound queued work too; count truncation below.
                for child in children:
                    if len(pending) + len(visited) >= limit:
                        truncated = True
                        break
                    pending.append(int(child))
            except (OSError, ValueError):
                child_lists_unreadable += 1
    return {"root_pid": pid, "root_process_rss_bytes": root_rss,
        "process_tree_summed_rss_bytes": rss, "visited_processes": len(visited),
        "unreadable_processes": unreadable, "unreadable_thread_child_lists": child_lists_unreadable,
        "walk_limit": limit, "walk_limit_reached": truncated or bool(pending),
        "measurement": "after-read snapshot, not peak; summed RSS double-counts shared pages"}


class HostSampler:
    """Host-wide samples, not per-cell attribution; unrelated workloads are confounders."""
    def __init__(self):
        self.stop = threading.Event()
        self.samples = []
        self.thread = threading.Thread(target=self.sample, daemon=True)

    def sample(self):
        while not self.stop.is_set():
            mem = {}
            for line in Path("/proc/meminfo").read_text().splitlines():
                fields = line.split()
                if fields[0] in {"MemTotal:", "MemAvailable:", "Cached:", "Dirty:"}:
                    mem[fields[0][:-1] + "_bytes"] = int(fields[1]) * 1024
            # Summed process RSS double-counts shared pages. Retain that caveat.
            rss = 0
            for statm in Path("/proc").glob("[0-9]*/statm"):
                try:
                    rss += int(statm.read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
                except (OSError, ValueError, IndexError):
                    pass
            self.samples.append({"monotonic": time.monotonic(), "host_summed_process_rss_bytes": rss, **mem})
            self.stop.wait(.25)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()


class Harness:
    def __init__(self, args):
        self.args = args
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.scratch = Path(tempfile.mkdtemp(prefix="agent-impact-", dir=args.workspace_parent))
        self.commands = []
        self.lock = threading.Lock()
        self.env = dict(os.environ, GIT_NO_LAZY_FETCH="1", GIT_TRACE_REDACT="1", GIT_TERMINAL_PROMPT="0")
        for key in list(self.env):
            if key.startswith("GIT_TRACE") or key in {"GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE",
                "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_CURL_VERBOSE"}:
                self.env.pop(key, None)
        self.result = {"complete": False, "cells": [], "restart_probes": [], "workspace": str(self.scratch),
            "environment": {"platform": platform.platform(), "cpus": os.cpu_count(), "os_page_cache": "not dropped",
                "scope": "warm full mirror and loopback proxy; upstream WAN population, Bazel and RBE excluded",
                "resources": "250ms host samples; summed RSS double-counts shared pages; host available-memory samples include unrelated processes",
                "bytes": "logical bytes read and on-disk allocation, NOT network bytes; reflinks may share physical extents",
                "timing": "client clone launch to verified reads; warm-sizes, mount and optional async prefetch included; audits/cleanup excluded"},
            "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
            "provenance": {}, "failures": []}

    def save(self):
        with self.lock:
            dump(self.output / "commands.json", self.commands)
            dump(self.output / "results.json", self.result)

    def run(self, argv, name, cwd=None, timeout=None):
        argv = list(map(str, argv))
        started = time.monotonic()
        with (self.output / (name + ".stdout")).open("wb") as out, (self.output / (name + ".stderr")).open("wb") as err:
            process = subprocess.Popen(argv, cwd=cwd or self.scratch, env=dict(self.env,
                GIT_TRACE2_EVENT=str(self.output / (name + ".trace2.jsonl"))), stdout=out, stderr=err, start_new_session=True)
            fd = os.pidfd_open(process.pid)
            timed_out = False
            try:
                ready, _, _ = select.select([fd], [], [], timeout or self.args.timeout)
                timed_out = not ready
                if timed_out:
                    os.killpg(process.pid, signal.SIGKILL)
            except BaseException:
                # The child has not been reaped; its process-group identity is owned.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                raise
            finally:
                process.wait()
                os.close(fd)
        row = {"argv": argv, "cwd": str(cwd or self.scratch), "name": name,
               "seconds": time.monotonic() - started, "returncode": process.returncode, "timed_out": timed_out}
        with self.lock:
            self.commands.append(row)
        if timed_out or process.returncode:
            raise RuntimeError(f"{name} failed; see retained stdout/stderr")
        return row

    def git(self, repo, *args):
        return subprocess.check_output([self.git_binary, "--no-replace-objects", "-C", str(repo), *args],
            env=self.env, stderr=subprocess.PIPE, timeout=self.args.timeout)

    def freeze(self):
        paths = {"git": Path(shutil.which(self.args.git) or self.args.git),
            "baseline-vfs": self.args.baseline_dir / "git-vfs", "baseline-proxy": self.args.baseline_dir / "git-proxy",
            "candidate-vfs": self.args.candidate_vfs, "candidate-proxy": self.args.candidate_proxy}
        self.binaries = {}
        frozen = self.output / "frozen"
        frozen.mkdir()
        for name, path in paths.items():
            path = path.resolve(strict=True)
            target = frozen / name
            shutil.copy2(path, target)
            self.binaries[name] = target
            self.result["provenance"][name] = {"input": str(path), "sha256": sha256(target)}
        self.git_binary = str(self.binaries["git"])
        self.result["provenance"]["harness_sha256"] = sha256(Path(__file__))
        shutil.copy2(Path(__file__), frozen / Path(__file__).name)
        self.result["provenance"]["baseline_source"] = self.args.baseline_source
        self.result["provenance"]["candidate_source"] = self.args.candidate_source
        self.result["storage_preflight"] = {
            "scratch_filesystem": filesystem_identity(self.scratch),
            "mirror_filesystem": filesystem_identity(self.args.mirror.resolve()),
            "scratch_free_bytes": shutil.disk_usage(self.scratch).free,
            "mirror": disk_usage(self.args.mirror.resolve()),
            "retention": "successful cells retire only owned fixture.git mirror copies; client/response caches remain after success or failure",
        }
        for source in (self.args.mirror.resolve(),):
            if self.git(source, "rev-parse", "--is-bare-repository").strip() != b"true":
                raise ValueError("mirror must be a bare full repository")
            if self.git(source, "rev-parse", "--show-object-format").strip() != b"sha1":
                raise ValueError("mirror must use SHA1")
            if (source / "shallow").exists() or (source / "objects/info/alternates").exists() or list((source / "objects/pack").glob("*.promisor")):
                raise ValueError("fixture must be standalone, complete, non-shallow")
            self.git(source, "cat-file", "-e", self.args.commit + "^{commit}")
        self.entries = []
        for raw in self.git(self.args.mirror, "ls-tree", "-r", "-z", self.args.commit).split(b"\0"):
            if raw:
                header, path = raw.split(b"\t", 1)
                mode, kind, oid = header.split()
                if kind == b"blob" and mode in (b"100644", b"100755"):
                    self.entries.append({"path": os.fsdecode(path), "oid": oid.decode()})
        self.manifests = {}
        for journey in self.args.journeys:
            paths = self.args.narrow_path
            selected = [e for e in self.entries if e["path"] in paths] if journey == "narrow" else [
                e for e in self.entries if any(e["path"].startswith(p.rstrip("/") + "/") for p in self.args.broad_prefix)]
            if not selected or (journey == "narrow" and {e["path"] for e in selected} != set(paths)):
                raise ValueError("journey paths absent in pinned fixture: " + journey)
            manifest = self.output / (journey + "-manifest.json")
            dump(manifest, selected)
            self.manifests[journey] = manifest
        self.save()

    def stats(self, origin):
        with urllib.request.urlopen(origin + "/api/v1/cache/stats", timeout=3) as response:
            return json.load(response)

    def start_proxy(self, binary, cache, name):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        origin = f"http://127.0.0.1:{port}"
        command = [binary, "--git", self.git_binary, "--repo", "fixture=" + str(self.args.mirror.resolve()),
            "--cache-dir", cache, "--listen", f"127.0.0.1:{port}", "--refresh-ttl-seconds", "86400",
            "--pack-cache-ttl-seconds", "86400", "--pack-cache-max-entry-bytes", "268435456"]
        log = (self.output / (name + ".stderr")).open("wb")
        started = time.monotonic()
        process = subprocess.Popen(list(map(str, command)), stdout=subprocess.DEVNULL, stderr=log,
            env=dict(self.env, GIT_TRACE2_EVENT=str(self.output / (name + ".trace2.jsonl"))), start_new_session=True)
        with self.lock:
            command_row = {"name": name, "argv": list(map(str, command)), "kind": "owned proxy"}
            self.commands.append(command_row)
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("proxy exited during readiness gate")
                try:
                    self.stats(origin)
                    command_row["seconds_to_ready"] = time.monotonic() - started
                    return origin, (process, log)
                except (OSError, ValueError):
                    time.sleep(.02)
            raise RuntimeError("proxy readiness timeout")
        except BaseException:
            self.stop(process, log)
            raise

    @staticmethod
    def stop(process, log):
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            # wait timed out without reaping; process identity remains reserved.
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        log.close()

    def prepare_cache(self, directory, name):
        storage = self.result["storage_preflight"]
        free = shutil.disk_usage(directory).free
        source_fs, target_fs = storage["mirror_filesystem"], storage["scratch_filesystem"]
        reflink_possible = (source_fs["type"] in {"btrfs", "xfs"} and source_fs == target_fs)
        # Same-filesystem CoW copies need metadata, not a full mirror-sized free
        # extent. cp --reflink=auto remains authoritative about actual support.
        budget = 64 * 1024 * 1024 + (0 if reflink_possible else storage["mirror"]["allocated_bytes"])
        check = {"cell": name, "free_bytes": free, "required_free_budget_bytes": budget,
            "same_filesystem_reflink_possible": reflink_possible}
        self.result.setdefault("storage_copy_checks", []).append(check)
        self.save()
        if free < budget:
            raise RuntimeError(f"Insufficient scratch free-space budget for {name}: {free} < {budget}; use a larger or same-filesystem CoW workspace parent")
        cache = directory / "proxy-cache"
        cache.mkdir()
        mirror = cache / "fixture.git"
        self.run(["cp", "--reflink=auto", "-a", self.args.mirror.resolve(), mirror], name + "-copy-mirror")
        for key, value in (("gitproxy.upstream", str(self.args.mirror.resolve())), ("uploadpack.allowFilter", "true"),
            ("uploadpack.allowAnySHA1InWant", "true"), ("transfer.advertiseObjectInfo", "true"), ("http.receivepack", "false")):
            self.git(mirror, "config", key, value)
        return cache

    def retire_mirror(self, cache, row):
        """Delete only this campaign's disposable copy, after proxy shutdown."""
        target = cache / "fixture.git"
        record = row["mirror_retirement"] = {"path": str(target), "complete": False}
        try:
            scratch = self.scratch.resolve(strict=True)
            if (cache.name != "proxy-cache" or cache.is_symlink() or target.is_symlink() or not target.is_dir()
                    or not cache.resolve(strict=True).is_relative_to(scratch)
                    or cache.resolve(strict=True).parent.parent != scratch
                    or target.resolve(strict=True) != cache.resolve(strict=True) / "fixture.git"
                    or target.resolve(strict=True) == self.args.mirror.resolve(strict=True)):
                raise RuntimeError("refusing to retire an unowned or symlinked fixture mirror")
            shutil.rmtree(target)
            record["complete"] = True
        except BaseException as error:
            record["error"] = repr(error)
            row["complete"] = False
            self.result["failures"].append({"phase": "owned mirror retirement", "error": repr(error), "path": str(target)})
            raise

    def clone(self, origin, repo, name, checkout):
        args = [self.git_binary, "-c", "protocol.version=2", "clone", "--depth=1", "--revision=" + self.args.commit]
        if not checkout:
            args += ["--bare", "--filter=blob:none"]
        return self.run(args + [origin + "/git/fixture.git", repo], name + "-clone")

    def agent(self, directory, origin, arm, journey, name, barrier):
        directory.mkdir()
        repo = directory / "repo"
        mount = directory / "mount"
        mount_state = None
        phases = {}
        try:
            barrier.wait(timeout=30)
            start = time.monotonic()
            phases["clone"] = self.clone(origin, repo, name, arm == "checkout")
            if arm == "checkout":
                view = repo
            else:
                binary = self.binaries[arm]
                phases["warm_sizes"] = self.run([binary, "--git", self.git_binary, "warm-sizes", "--repo", repo,
                    "--size-source", "git"], name + "-sizes")
                mount.mkdir()
                command = [binary, "--git", self.git_binary, "mount", "--repo", repo,
                    "--size-source", "git", "--overlay", directory / "overlay"]
                if self.args.prefetch:
                    command += ["--prefetch-bazel"]
                command += [mount]
                log = (self.output / (name + "-mount.stderr")).open("wb")
                mounted = time.monotonic()
                process = subprocess.Popen(list(map(str, command)), stdout=subprocess.DEVNULL, stderr=log,
                    env=dict(self.env, GIT_TRACE2_EVENT=str(self.output / (name + "-mount.trace2.jsonl"))), start_new_session=True)
                mount_state = (process, log)
                with self.lock:
                    self.commands.append({"name": name + "-mount", "argv": list(map(str, command)), "kind": "owned mount"})
                while not os.path.ismount(mount):
                    if process.poll() is not None or time.monotonic() - mounted > self.args.timeout:
                        raise RuntimeError("mount readiness failed: " + name)
                    time.sleep(.005)
                phases["mount"] = {"seconds": time.monotonic() - mounted}
                view = mount
            ready = time.monotonic()
            phases["verified_reads"] = self.run([sys.executable, Path(__file__).resolve(), "--internal-read",
                view, self.manifests[journey]], name + "-reads")
            end = time.monotonic()
            verified = json.loads((self.output / (name + "-reads.stdout")).read_text())
            return {"name": name, "seconds_to_ready": ready - start, "seconds_to_verified_reads": end - start,
                "phases": phases, "correctness": verified,
                "vfs_process_memory_after_reads": process_tree_rss(mount_state[0].pid) if mount_state else None,
                "client_disk_after_reads": disk_usage(directory)}
        finally:
            if mount_state:
                self.stop(*mount_state)
                if os.path.ismount(mount):
                    subprocess.run(["fusermount3", "-u", str(mount)], check=True, timeout=30, capture_output=True)
                if os.path.ismount(mount):
                    raise RuntimeError("owned mount cleanup failed: " + str(mount))

    def cell(self, trial, arm, journey, count):
        name = f"trial-{trial}-{arm}-{journey}-{count}"
        directory = self.scratch / name
        directory.mkdir()
        row = {"name": name, "trial": trial, "arm": arm, "journey": journey, "agents": count, "complete": False}
        self.result["cells"].append(row)
        self.save()
        cache = self.prepare_cache(directory, name)
        version = "baseline" if arm == "baseline-vfs" else "candidate"
        origin, state = self.start_proxy(self.binaries[version + "-proxy"], cache, name + "-proxy")
        try:
            # Matched warm generated clone response. Different lazy demand patterns
            # remain deliberately cold: their cost is part of the agent journey.
            self.clone(origin, directory / "prime", name + "-prime", arm == "checkout")
            before = self.stats(origin)
            barrier = threading.Barrier(count)
            with HostSampler() as resources:
                started = time.monotonic()
                with ThreadPoolExecutor(max_workers=count) as pool:
                    futures = [pool.submit(self.agent, directory / f"agent-{i}", origin, arm, journey, name + f"-agent-{i}", barrier)
                        for i in range(count)]
                    completed = [future.result() for future in as_completed(futures)]
                row["group_seconds_including_cleanup"] = time.monotonic() - started
            row["agents_results"] = completed
            row["slowest_agent_seconds"] = max(a["seconds_to_verified_reads"] for a in completed)
            row["median_agent_seconds"] = statistics.median(a["seconds_to_verified_reads"] for a in completed)
            row["throughput_agents_per_second"] = count / row["slowest_agent_seconds"]
            row["resource_samples"] = resources.samples
            if resources.samples:
                row["resources"] = {
                    "host_peak_summed_process_rss_bytes": max(s["host_summed_process_rss_bytes"] for s in resources.samples),
                    "host_min_available_bytes": min(s["MemAvailable_bytes"] for s in resources.samples),
                    "host_max_available_drop_from_first_bytes": max(0, resources.samples[0]["MemAvailable_bytes"] - min(
                        s["MemAvailable_bytes"] for s in resources.samples)),
                    "samples": len(resources.samples),
                }
            row["proxy_stats_before"] = before
            row["proxy_stats_after"] = self.stats(origin)
            row["proxy_stats_delta"] = delta(before, row["proxy_stats_after"])
            row["proxy_process_memory_after_agents"] = process_tree_rss(state[0].pid)
            row["proxy_disk_after"] = disk_usage(cache)
            row["trace2"] = trace_summary(self.output, name + "-agent-")
            row["complete"] = True
        finally:
            try:
                self.stop(*state)
                if row["complete"]:
                    self.retire_mirror(cache, row)
            except BaseException:
                row["complete"] = False
                raise
            finally:
                self.save()

    def restart_probe(self, trial, version):
        name = f"restart-{trial}-{version}"
        directory = self.scratch / name
        directory.mkdir()
        cache = self.prepare_cache(directory, name)
        row = {"name": name, "trial": trial, "version": version, "complete": False}
        self.result["restart_probes"].append(row)
        origin, state = self.start_proxy(self.binaries[version + "-proxy"], cache, name + "-before")
        try:
            row["prime_clone"] = self.clone(origin, directory / "prime", name + "-prime", False)
            before = self.stats(origin)
            row["warm_clone"] = self.clone(origin, directory / "warm-proof", name + "-warm-proof", False)
            after = self.stats(origin)
            row["warm_fetch_stats_delta"] = delta(before, after)
            if row["warm_fetch_stats_delta"].get("pack_cache", {}).get("hits", 0) < 1:
                raise RuntimeError("restart probe request not cache-eligible; cannot claim a persistent-cache comparison")
        finally:
            self.stop(*state)
        origin, state = self.start_proxy(self.binaries[version + "-proxy"], cache, name + "-after")
        try:
            # Ref refresh is excluded; it must complete before measuring restart reuse.
            self.run([self.git_binary, "ls-remote", origin + "/git/fixture.git", "HEAD"], name + "-refresh")
            before = self.stats(origin)
            row["clone_after_restart"] = self.clone(origin, directory / "after", name + "-clone-after", False)
            after = self.stats(origin)
            row["stats_before"] = before
            row["stats_after"] = after
            row["stats_delta"] = delta(before, after)
            row["persistent_reuse_observed"] = row["stats_delta"].get("pack_cache", {}).get("hits", 0) > 0
            if version == "candidate" and self.args.require_persistent_reuse and not row["persistent_reuse_observed"]:
                raise RuntimeError("candidate did not reuse a generated response after restart")
            row["correct_commit"] = self.git(directory / "after", "rev-parse", "HEAD").decode().strip() == self.args.commit
            if not row["correct_commit"]:
                raise RuntimeError("restart clone returned wrong commit")
            row["complete"] = True
        finally:
            try:
                self.stop(*state)
                if row["complete"]:
                    self.retire_mirror(cache, row)
            except BaseException:
                row["complete"] = False
                raise
            finally:
                row["trace2"] = trace_summary(self.output, name + "-")
                self.save()


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mirror", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workspace-parent", type=Path, required=True, help="Private fresh workspace parent, outside output and fixture")
    parser.add_argument("--git", default="git")
    parser.add_argument("--baseline-dir", type=Path, default=Path("artifacts/followups-20261019/baseline"))
    parser.add_argument("--candidate-vfs", type=Path, default=Path("target/release/git-vfs"))
    parser.add_argument("--candidate-proxy", type=Path, default=Path("target/release/git-proxy"))
    parser.add_argument("--baseline-source", default="17287e3d78b53ff07ee1c438ea4aa6712b20df99")
    parser.add_argument("--candidate-source", required=True, help="Exact commit plus dirty-diff SHA, or build manifest identity")
    parser.add_argument("--commit", default=COMMIT)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--agents", nargs="+", type=int, default=[1, 4], choices=[1, 4, 8])
    parser.add_argument("--journeys", nargs="+", choices=["narrow", "broad"], default=["narrow"])
    parser.add_argument("--arms", nargs="+", choices=["checkout", "baseline-vfs", "candidate-vfs"], default=["checkout", "baseline-vfs", "candidate-vfs"])
    parser.add_argument("--narrow-path", action="append", default=None)
    parser.add_argument("--broad-prefix", action="append", default=None)
    parser.add_argument("--prefetch", action="store_true", help="Async Bazel metadata prefetch on both VFS arms")
    parser.add_argument("--restart-only", action="store_true")
    parser.add_argument("--skip-restart", action="store_true")
    parser.add_argument("--require-persistent-reuse", action="store_true", help="Fail if candidate restart probe has no actual pack-cache hit")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    args.narrow_path = args.narrow_path or ["MODULE.bazel", "src/main/cpp/util/strings.h", "src/main/cpp/util/strings.cc"]
    args.broad_prefix = args.broad_prefix or ["src/main/cpp"]
    if args.trials < 1 or args.timeout <= 0 or (args.restart_only and args.skip_restart):
        parser.error("positive trials/timeout and compatible restart options required")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("output must be new or empty")
    output, mirror, parent = (p.resolve() for p in (args.output, args.mirror, args.workspace_parent))
    if output.is_relative_to(mirror) or mirror.is_relative_to(output) or parent.is_relative_to(output) or parent.is_relative_to(mirror):
        parser.error("workspace parent must be outside output/fixture; output and fixture must be disjoint")
    for path in args.narrow_path + args.broad_prefix:
        if Path(path).is_absolute() or ".." in Path(path).parts:
            parser.error("journey selections must be safe repository-relative paths")
    return args


def main():
    if len(sys.argv) == 4 and sys.argv[1] == "--internal-read":
        read_manifest(sys.argv[2], sys.argv[3])
        return
    args = arguments()
    if not args.run:
        print(json.dumps({"execution": False, "trials": args.trials, "arms": args.arms, "agents": args.agents,
            "journeys": args.journeys, "prefetch": args.prefetch, "restart_probes": not args.skip_restart,
            "readiness_gates": ["full standalone SHA1 mirror", "Linux pidfd/FUSE", "frozen binary hashes",
                "actual warm cache hit before restart comparison", "every selected file verified against tree blob SHA1"],
            "practical_matrix": "narrow / 1,4 agents / 3 arms / 3 rotated trials + 6 restart probes; broad and 8-agent runs are opt-in",
            "limitations": ["No WAN comparison; upstream mirror population excluded", "OS pages not dropped",
                "Host RSS is not attributable process memory", "No Bazel build or RBE action in this harness",
                "Compare resource medians across trials; do not infer speedups from a single cell"]}, indent=2))
        return
    if not hasattr(os, "pidfd_open") or not Path("/proc/meminfo").exists():
        raise RuntimeError("Linux pidfd and /proc required")
    if not args.restart_only and any(a != "checkout" for a in args.arms) and not os.access("/dev/fuse", os.R_OK | os.W_OK):
        raise RuntimeError("real FUSE access required for VFS journeys")
    args.workspace_parent.mkdir(parents=True, exist_ok=True)
    harness = Harness(args)
    try:
        harness.freeze()
        for trial in range(1, args.trials + 1):
            if not args.skip_restart:
                versions = ["baseline", "candidate"]
                if trial % 2 == 0:
                    versions.reverse()
                for version in versions:
                    harness.restart_probe(trial, version)
            if not args.restart_only:
                for journey in args.journeys:
                    for count in args.agents:
                        arms = args.arms[(trial - 1) % len(args.arms):] + args.arms[:(trial - 1) % len(args.arms)]
                        for arm in arms:
                            harness.cell(trial, arm, journey, count)
        harness.result["complete"] = True
    except BaseException as error:
        harness.result["failures"].append({"error": repr(error), "traceback": traceback.format_exc()})
        raise
    finally:
        harness.save()
        # Per-cell retirement removes only disposable full mirror copies. Keep
        # client workspaces and response caches, even on campaign success.
        harness.result["workspace_retained"] = True
        harness.save()
    print(json.dumps({"complete": True, "results": str(harness.output / "results.json")}))


if __name__ == "__main__":
    main()
