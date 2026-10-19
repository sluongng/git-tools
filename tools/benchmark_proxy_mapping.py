#!/usr/bin/env python3
"""Measure the object mapping endpoint with warm Git objects and fresh Fjall DBs.

No subprocesses or artifact writes occur without --run. Fixture preparation,
validation, and summaries are deliberately outside each HTTP request timer.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import time
import urllib.request
import urllib.error

UPSTREAM = "https://github.com/bazelbuild/bazel.git"
REVISION = "c9af12d1db12d1b2df2137feb9ea8d39e3a25c79"


def command(argv, **kwargs):
    return subprocess.run([str(x) for x in argv], check=True, timeout=900,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs).stdout


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def allocation(path):
    return sum(p.stat().st_blocks * 512 for p in path.rglob("*") if p.is_file()) if path.exists() else 0


def proc_metrics(pid):
    # Linux parent counters include reaped Git children. RSS is proxy-only.
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        ticks = os.sysconf("SC_CLK_TCK")
        hwm = next(line.split()[1] for line in Path(f"/proc/{pid}/status").read_text().splitlines()
                   if line.startswith("VmHWM:"))
        return {"cpu_seconds": sum(int(fields[i]) for i in (11, 12, 13, 14)) / ticks,
                "proxy_high_water_rss_bytes": int(hwm) * 1024}
    except (OSError, StopIteration):
        return {}


def trace_events(path, offset):
    blob, other, commands = 0, 0, {}
    with path.open("rb") as stream:
        stream.seek(offset)
        for line in stream:
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("event") != "start":
                continue
            argv = event.get("argv", [])
            if "cat-file" in argv and "blob" in argv:
                blob += 1
            else:
                other += 1
            name = next((x for x in argv[1:] if x in ("cat-file", "config", "fetch", "ls-remote", "clone", "rev-parse")), "unknown")
            commands[name] = commands.get(name, 0) + 1
    return {"cat_file_blob_starts": blob, "other_git_starts": other, "command_histogram": commands}


class Server:
    def __init__(self, args, binary, directory, git):
        self.args, self.directory = args, directory
        self.trace = directory / "trace2.jsonl"
        self.trace.touch()
        self.log = (directory / "proxy.log").open("ab")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        argv = [str(binary), "--cache-dir", str(directory / "cache"), "--git", git,
                "--repo", "bazel=" + UPSTREAM, "--listen", f"127.0.0.1:{port}",
                "--enable-blake3", "--refresh-ttl-seconds", "3600"]
        env = dict(os.environ, GIT_TRACE2_EVENT=str(self.trace), GIT_NO_LAZY_FETCH="1")
        self.process = subprocess.Popen(argv, stdout=self.log, stderr=self.log, env=env,
                                        start_new_session=True)
        with (directory / "server-commands.jsonl").open("a") as commands:
            commands.write(json.dumps({"argv": argv, "pid": self.process.pid,
                           "trace_offset_at_launch": self.trace.stat().st_size,
                           "environment_overrides": {k: env[k] for k in ("GIT_TRACE2_EVENT", "GIT_NO_LAZY_FETCH")}}) + "\n")
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError(f"proxy exited {self.process.returncode}")
                try:
                    with urllib.request.urlopen(self.url + "/api/v1/cache/stats", timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    time.sleep(.05)
            else:
                raise TimeoutError("proxy readiness timed out")
        except BaseException:
            self.stop()
            raise

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        self.log.close()

    def request(self, oids, sizes, name, blake3=True, reference=None):
        payload = json.dumps({"oids": oids, "blake3": blake3}).encode()
        request = urllib.request.Request(self.url + "/api/v1/repos/bazel/objects", data=payload,
                                         headers={"Content-Type": "application/json"})
        write_json(self.directory / (name + ".request.json"), {"oids": oids, "blake3": blake3})
        offset = self.trace.stat().st_size
        before_disk = allocation(self.directory / "cache/digests.fjall")
        before = proc_metrics(self.process.pid)
        start = time.perf_counter()
        try:
            with urllib.request.urlopen(request, timeout=self.args.request_timeout) as response:
                raw = response.read()
                status = response.status
        except urllib.error.HTTPError as error:
            (self.directory / (name + ".failed-response.txt")).write_bytes(error.read())
            raise
        elapsed = time.perf_counter() - start
        after = proc_metrics(self.process.pid)
        (self.directory / (name + ".response.json")).write_bytes(raw)
        objects = json.loads(raw)["objects"]
        assert status == 200 and len(objects) == len(oids), "response cardinality/status mismatch"
        assert [x["oid"] for x in objects] == oids, "response OIDs/order mismatch"
        mapping = {}
        for obj in objects:
            oid = obj["oid"]
            assert obj["object_type"] == "blob" and obj["size_bytes"] == sizes[oid], obj
            digest = obj["blake3"]
            if blake3:
                assert digest["size_bytes"] == sizes[oid] and re.fullmatch("[0-9a-f]{64}", digest["hash"]), obj
                mapping[oid] = digest["hash"]
            else:
                assert digest is None, obj
        if reference is not None:
            assert mapping == reference, "mapping changed across requests/restart"
        logical_bytes = sum(sizes[oid] for oid in oids)
        result = {"phase": name, "seconds": elapsed, "objects": len(oids), "blake3": blake3,
                  "objects_per_second": len(oids) / elapsed, "logical_unique_blob_bytes": logical_bytes,
                  "logical_mib_per_second": logical_bytes / 1048576 / elapsed,
                  "response_bytes": len(raw), "request_bytes": len(payload),
                  "fjall_allocated_bytes_before": before_disk,
                  "fjall_allocated_bytes_after": allocation(self.directory / "cache/digests.fjall"),
                  "native_git": trace_events(self.trace, offset),
                  "proxy_high_water_rss_bytes": after.get("proxy_high_water_rss_bytes"),
                  "proxy_plus_reaped_children_cpu_seconds": after.get("cpu_seconds", 0) - before.get("cpu_seconds", 0)}
        result["fjall_allocated_growth_bytes"] = result["fjall_allocated_bytes_after"] - before_disk
        if name == "cold_mapping":
            assert result["native_git"]["cat_file_blob_starts"] == len(oids), "cold DB did not hash every blob"
        else:
            assert result["native_git"]["cat_file_blob_starts"] == 0, "cached/metadata request read blob contents"
        return result, mapping


def snapshot(git, mirror, revision):
    entries = []
    for row in command([git, "-C", mirror, "ls-tree", "-r", "-l", "-z", revision], env=dict(os.environ, GIT_NO_LAZY_FETCH="1")).split(b"\0"):
        if not row:
            continue
        metadata, path = row.split(b"\t", 1)
        mode, kind, oid, size = metadata.split()
        if kind == b"blob":
            entries.append({"oid": oid.decode(), "size_bytes": int(size), "mode": mode.decode(), "path_hex": path.hex()})
    assert entries, "empty snapshot"
    return entries


def verify_samples(git, mirror, entries, oids, sizes, mapping, b3sum):
    ordered = sorted(oids, key=lambda oid: (sizes[oid], oid))
    wanted = {ordered[0]: ["minimum_size"], ordered[-1]: ["maximum_size"]}
    examples = {}
    for entry in entries:
        oid = entry["oid"]
        if oid not in mapping:
            continue
        path = bytes.fromhex(entry["path_hex"]).lower()
        if entry["mode"] == "120000":
            examples.setdefault("symlink", oid)
        if sizes[oid] == 0:
            examples.setdefault("empty", oid)
        if path.endswith((b".png", b".ico", b".zip", b".jar", b".gif")):
            examples.setdefault("binary_path_candidate", oid)
        if path.endswith((b".bzl", b".bazel", b".cc", b".md")):
            examples.setdefault("text_path_candidate", oid)
    for category, oid in examples.items():
        wanted.setdefault(oid, []).append(category)
    # Search deterministic candidates for independent text/binary checks off-clock.
    categories = set()
    for oid in list(examples.values()) + ordered[:16] + ordered[-16:]:
        data = command([git, "-C", mirror, "cat-file", "blob", oid])
        category = "binary_nul" if b"\0" in data else "text_or_non_nul"
        if category not in categories:
            wanted.setdefault(oid, []).append(category)
            categories.add(category)
    for oid in sorted(oids):
        if len(wanted) >= min(8, len(oids)):
            break
        wanted.setdefault(oid, []).append("deterministic_fill")
    checked = []
    for oid, reasons in wanted.items():
        data = command([git, "-C", mirror, "cat-file", "blob", oid])
        assert len(data) == sizes[oid]
        assert hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest() == oid
        actual = command([b3sum, "--no-names"], input=data).decode().strip()
        assert actual == mapping[oid], f"independent BLAKE3 mismatch: {oid}"
        checked.append({"oid": oid, "size_bytes": len(data), "reasons": reasons, "blake3": actual})
    return {"scope": "sampled, not every blob", "checks": checked, "content_categories_found": sorted(categories)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--proxy-binary", type=Path, required=True)
    parser.add_argument("--git", default="git")
    parser.add_argument("--b3sum", default="b3sum")
    parser.add_argument("--mirror", type=Path, required=True, help="Existing CLOSED, fully populated GitHub bare mirror")
    parser.add_argument("--revision", default=REVISION)
    parser.add_argument("--source-revision", help="Independently verified source revision of the supplied binary")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--subset-json", type=Path, help="Query presence manifest with present_oids or hydrated_entries; omitted means snapshot only")
    parser.add_argument("--request-timeout", type=float, default=900)
    parser.add_argument("--metadata-baseline", action="store_true")
    args = parser.parse_args()
    if args.trials < 1 or args.request_timeout <= 0:
        parser.error("trials and timeout must be positive")
    if not args.run:
        print(json.dumps({"dry_run": True, "config": vars(args)}, default=str, indent=2))
        return
    args.output = args.output.resolve()
    if args.output.is_relative_to(args.mirror.resolve()):
        parser.error("output must be outside the read-only source mirror")
    args.output.mkdir(parents=True, exist_ok=False)
    results = {"status": "running", "config": vars(args).copy(), "trials": [],
               "boundaries": {"mirror_population": "excluded; existing full mirror required",
                              "fixture_copy_and_alternates": "excluded; owned fixture shared read-only by per-trial mirror clones",
                              "os_page_cache": "not dropped; warm Git objects, cold mapping DB only",
                              "request_timer": "HTTP POST through full response read; parsing/audits excluded",
                              "throughput": "logical blob MiB per HTTP second, NOT standalone BLAKE3 algorithm throughput",
                              "cpu": "Linux /proc proxy utime+stime+cutime+cstime delta, including reaped Git children",
                              "rss": "proxy process lifetime high-water RSS, not aggregate process-family RSS",
                              "trace2": "enabled consistently in all measured server requests"}}
    # Serialize Paths in config once; all later checkpoints are ordinary JSON.
    results["config"] = json.loads(json.dumps(results["config"], default=str))
    server = None
    try:
        git = shutil.which(args.git) or str(Path(args.git).resolve())
        b3sum = shutil.which(args.b3sum) or str(Path(args.b3sum).resolve())
        binary = args.output / "git-proxy"
        shutil.copy2(args.proxy_binary, binary)
        results["provenance"] = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                                 "source_revision": args.source_revision, "upstream": UPSTREAM,
                                 "git_version": command([git, "--version"]).decode().strip(),
                                 "b3sum_version": command([b3sum, "--version"]).decode().strip()}
        fixture = args.output / "fixture.git"
        command(["cp", "-a", "--reflink=auto", args.mirror.resolve(), fixture])
        # Only mutate our owned fixture, never the supplied mirror or its digest DB.
        for key in ("gitproxy.upstream", "remote.origin.url"):
            command([git, "-C", fixture, "config", key, UPSTREAM])
        assert command([git, "-C", fixture, "rev-parse", "--is-bare-repository"]).strip() == b"true"
        assert not (fixture / "shallow").exists(), "full, not shallow, mirror required"
        assert not (fixture / "objects/info/alternates").exists(), "source mirror must be self-contained"
        entries = snapshot(git, fixture, args.revision)
        sizes = {entry["oid"]: entry["size_bytes"] for entry in entries}
        write_json(args.output / "snapshot.json", entries)
        workloads = {"snapshot": sorted(sizes)}
        if args.subset_json:
            manifest = json.loads(args.subset_json.read_text())
            subset = manifest.get("present_oids")
            if subset is None:
                subset = [entry["oid"] for entry in manifest["hydrated_entries"]]
            subset = sorted(set(subset))
            assert subset and set(subset) <= sizes.keys(), "subset does not belong to snapshot"
            if "snapshot_commit" in manifest:
                assert manifest["snapshot_commit"] == args.revision, "subset revision mismatch"
            workloads = {"query_subset": subset, **workloads}
            results["subset_manifest_sha256"] = hashlib.sha256(args.subset_json.read_bytes()).hexdigest()
        for workload, oids in workloads.items():
            assert len(oids) <= 16384, "one request exceeds API limit"
            for trial in range(1, args.trials + 1):
                directory = args.output / f"{workload}-{trial}"
                (directory / "cache").mkdir(parents=True)
                command([git, "clone", "--shared", "--mirror", fixture, directory / "cache/bazel.git"])
                for key in ("gitproxy.upstream", "remote.origin.url"):
                    command([git, "-C", directory / "cache/bazel.git", "config", key, UPSTREAM])
                row = {"workload": workload, "trial": trial, "phases": [], "status": "running"}
                results["trials"].append(row)
                write_json(args.output / "results.json", results)
                server = Server(args, binary, directory, git)
                if args.metadata_baseline:
                    phase, _ = server.request(oids, sizes, "metadata_only", blake3=False)
                    row["phases"].append(phase)
                phase, mapping = server.request(oids, sizes, "cold_mapping")
                row["phases"].append(phase)
                write_json(args.output / "results.json", results)
                for repeat in range(1, 4):
                    phase, _ = server.request(oids, sizes, f"warm_mapping_{repeat}", reference=mapping)
                    row["phases"].append(phase)
                server.stop()
                server = None
                server = Server(args, binary, directory, git)
                phase, _ = server.request(oids, sizes, "restart_cached_first", reference=mapping)
                row["phases"].append(phase)
                server.stop()
                server = None
                row["independent_verification"] = verify_samples(git, fixture, entries, oids, sizes, mapping, b3sum)
                row["status"] = "complete"
                write_json(args.output / "results.json", results)
        results["status"] = "complete"
    except BaseException as error:
        results["status"] = "failed"
        results["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        if server:
            server.stop()
        write_json(args.output / "results.json", results)


if __name__ == "__main__":
    main()
