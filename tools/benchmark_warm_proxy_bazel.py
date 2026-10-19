#!/usr/bin/env python3
"""Warm local Git proxy, fresh clients, BLAKE3 Bazel query comparison.

No process starts without --run. Mirror population is NOT measured: --mirror
must be an existing full fixture, which is copied before any server mutation.
Use benchmark_github_bazel.example.json plus proxy_binary and real tool paths.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import statistics
import subprocess
import time
import traceback
import urllib.request

from benchmark_github_bazel import Benchmark, SOURCE, COMMIT, dump, sha256

ARMS = ("checkout", "vfs-blake3-normal", "vfs-blake3-xattr")


class WarmProxyBenchmark(Benchmark):
    def __init__(self, args):
        # Never overwrite prior measurement evidence.
        if args.output.exists() and any(args.output.iterdir()):
            raise ValueError("Use a fresh empty output directory")
        fixture = args.mirror.resolve()
        if args.output.resolve().is_relative_to(fixture):
            raise ValueError("Output must not be inside the read-only mirror fixture")
        super().__init__(args)
        if not self.config.get("proxy_binary"):
            raise ValueError("Configuration requires proxy_binary")
        if self.config.get("prefetch_mode", "before-mount") != "before-mount":
            raise ValueError("This comparison requires blocking prefetch in both VFS arms")
        if self.config.get("mount_options"):
            raise ValueError("mount_options are owned by this comparison")
        self.config["digest_function"] = "BLAKE3"
        self.config["digest_attribute"] = ""
        self.config["prefetch_mode"] = "before-mount"
        if self.config.get("source") != SOURCE or self.config.get("commit") != COMMIT:
            raise ValueError("Use the pinned GitHub Bazel snapshot configuration")
        self.proxy = self.root / "git-proxy-frozen"
        self.proxy_state = None
        self.current_attribute = ""
        self.mapping_reference = []
        self.result["environment"].update({
            "upstream": SOURCE, "source": "owned loopback warm Git proxy (URL recorded after startup)",
            "cache_policy": "warm copied full mirror, warm generated packs and BLAKE3 mappings; fresh clone, VFS metadata cache, overlay and Bazel output base per cell; shared warm dependency/archive caches; OS caches not dropped",
            "ablation": "Both VFS arms import mappings and sizes identically and mount with BLAKE3 enabled. normal disables only Bazel xattr use; it is not an unconfigured VFS baseline.",
            "query_scope": "query parses Starlark and may not request content digests; xattr counters, not the BLAKE3 flag alone, establish mapping consumption",
            "timing": "continuous clone launch to first query exit, including client warm-blake3, blocking prefetch and mount; server preparation and post-query audits excluded",
        })

    def bazel_args(self, base):
        args = super().bazel_args(base)
        return [arg if not arg.startswith("--unix_digest_hash_attribute_name=") else
                "--unix_digest_hash_attribute_name=" + self.current_attribute for arg in args]

    def http(self, path, payload=None):
        request = urllib.request.Request(self.origin + path,
            data=None if payload is None else json.dumps(payload).encode(),
            headers={} if payload is None else {"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=600) as response:
            return json.load(response)

    def proxy_stats(self):
        return self.http("/api/v1/cache/stats")

    def freeze(self):
        super().freeze()
        shutil.copy2(self.config["proxy_binary"], self.proxy)
        self.result["frozen_proxy"] = {"path": str(self.proxy), "sha256": sha256(self.proxy),
            "input_path": self.config["proxy_binary"]}
        self.result["tool_provenance"] = {}
        for key in ("git", "bazel"):
            path = Path(shutil.which(self.config[key]) or self.config[key]).resolve()
            self.result["tool_provenance"][key] = {"path": str(path), "sha256": sha256(path)}
        dump(self.root / "frozen-config.json", self.config)
        self.save()

    def proxy_extra_args(self):
        return []

    def prepare_proxy(self):
        fixture = self.args.mirror.resolve()
        if not fixture.is_dir() or self.git(fixture, ["rev-parse", "--is-bare-repository"]).strip() != b"true":
            raise ValueError("--mirror must be an existing bare full mirror fixture")
        if self.git(fixture, ["rev-parse", "--show-object-format"]).strip() != b"sha1":
            raise ValueError("Mirror must use SHA1")
        if (fixture / "objects/info/alternates").exists() or (fixture / "shallow").exists():
            raise ValueError("Mirror must be standalone and complete, not shallow or alternate-backed")
        if list((fixture / "objects/pack").glob("*.promisor")):
            raise ValueError("Mirror must not be a partial clone")
        self.git(fixture, ["cat-file", "-e", COMMIT + "^{commit}"])
        setup = self.result["server_preparation"] = {"excluded_from_client_totals": True,
            "fixture": str(fixture), "full_mirror_population_seconds": None,
            "population_note": "Preexisting full mirror supplied; upstream mirror population is not measured"}
        cache = self.root / "proxy-cache"
        cache.mkdir()
        mirror = cache / "bazel.git"
        setup["mirror_copy"] = self.run(["cp", "--reflink=auto", "-a", fixture, mirror], "server-copy-mirror", timeout=1200)
        for key, value in (("gitproxy.upstream", SOURCE), ("remote.origin.url", SOURCE),
                           ("uploadpack.allowFilter", "true"), ("uploadpack.allowAnySHA1InWant", "true"),
                           ("transfer.advertiseObjectInfo", "true"), ("http.receivepack", "false")):
            self.git(mirror, ["config", key, value])
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        self.origin = f"http://127.0.0.1:{port}"
        self.clone_url = self.origin + "/git/bazel.git"
        command = [str(self.proxy), "--git", self.config["git"], "--repo", "bazel=" + SOURCE,
                   "--cache-dir", str(cache), "--listen", f"127.0.0.1:{port}", "--enable-blake3",
                   "--refresh-ttl-seconds", "86400", "--pack-cache-ttl-seconds", "86400",
                   "--pack-cache-max-entry-bytes", "268435456", *self.proxy_extra_args()]
        log = (self.root / "server.stderr").open("wb")
        start = time.perf_counter()
        process = subprocess.Popen(command, env=self.traced_env("server"), cwd=self.root,
            stdout=subprocess.DEVNULL, stderr=log, start_new_session=True)
        self.proxy_state = (process, log)
        while True:
            if process.poll() is not None or time.perf_counter() - start > 30:
                raise RuntimeError("Owned proxy did not start; inspect server.stderr")
            try:
                before = self.proxy_stats()  # Stats endpoint deliberately cannot trigger refresh.
                break
            except (OSError, ValueError):
                time.sleep(.01)
        setup["proxy_start_seconds"] = time.perf_counter() - start
        setup["proxy_command"] = command
        setup["initial_inert_stats"] = before
        setup["initial_smart_http_refresh"] = self.run(
            [self.config["git"], "ls-remote", self.clone_url, "HEAD"], "server-initial-refresh")
        setup["stats_after_refresh"] = self.proxy_stats()
        # Keep the fixture's pinned snapshot universe separate from mutable upstream HEAD.
        ids = sorted({line.split()[2].decode() for line in self.git(mirror,
            ["ls-tree", "-r", COMMIT]).splitlines() if line.split()[1] == b"blob"})
        if len(ids) != 11850:
            raise RuntimeError(f"Unexpected pinned snapshot blob universe: {len(ids)}")
        start = time.perf_counter()
        mappings = []
        for offset in range(0, len(ids), 4096):
            mappings += self.http("/api/v1/repos/bazel/objects", {"oids": ids[offset:offset + 4096], "blake3": True})["objects"]
        if {obj["oid"] for obj in mappings} != set(ids) or any(
                obj["object_type"] != "blob" or not obj.get("blake3")
                or obj["blake3"]["size_bytes"] != obj["size_bytes"] for obj in mappings):
            raise RuntimeError("Server prewarm returned incomplete BLAKE3 mappings")
        setup["mapping_prewarm"] = {"seconds": time.perf_counter() - start,
            "unique_blobs": len(mappings), "logical_bytes": sum(obj["size_bytes"] for obj in mappings),
            "note": "Cold owned Fjall mapping cache; full mirror and OS pages already warm; includes HTTP serialization and client validation"}
        dump(self.root / "server-mappings.json", mappings)
        self.config["source"] = self.clone_url
        self.config["mount_options"] = ["--blake3-proxy-url", self.origin, "--blake3-proxy-repo", "bazel"]
        self.result["environment"]["client_source"] = self.clone_url
        for checkout in (True, False):
            name = "server-prime-" + ("checkout" if checkout else "vfs")
            repo = self.root / (name + ".git")
            setup[name] = {"clone": self.clone(repo, name, checkout)}
            if not checkout:
                self.warm_mapping(repo, name, setup[name])
                setup[name]["prefetch"] = self.run(self.expand(self.config.get("prefetch_command", [
                    "{vfs}", "--git", "{git}", "prefetch-bazel", "--repo", "{repo}"]), repo), name + "-prefetch")
        setup["stats_after_priming"] = self.proxy_stats()
        dump(self.root / "execution-config.json", self.config)
        self.save()

    def warm_mapping(self, repo, name, phases):
        phases["warm_blake3_sizes"] = self.run(self.vfs_args("warm-blake3", "--repo", repo,
            "--proxy-url", self.origin, "--proxy-repo", "bazel"), name + "-warm-blake3")

    def digest_probe(self, repo, overlay, cwd, name, before):
        available = set(before["union_present_oids"])
        # A header outside the query-prefetched metadata is a digest-only test.
        path = "src/main/cpp/util/strings.h"
        oid = self.git(repo, ["rev-parse", COMMIT + ":" + path]).decode().strip()
        if oid in available:
            return {"verified": False, "reason": "Selected probe blob already present", "path": path}
        value = os.getxattr(cwd / path, "user.blake3")
        after = self.presence(repo, name, "after_digest_probe", overlay)
        expected = next(obj["blake3"]["hash"] for obj in self.mapping_reference if obj["oid"] == oid)
        if value.hex() != expected or after["union_present_oids"] != before["union_present_oids"]:
            raise RuntimeError("Digest xattr changed blob availability or disagreed with proxy")
        return {"verified": True, "path": path, "oid": oid, "digest": value.hex(),
                "no_hydration": True, "outside_query_timing": True}

    def same_size_edit_proof(self, cwd, base, name, row):
        """Exercise Bazel's real edit/rebuild path, outside query timing."""
        relative = "src/main/cpp/util/strings.cc"
        source = cwd / relative
        original = source.read_bytes()
        anchor = b'static const char kSeparator[] = "'
        if original.count(anchor) != 1:
            raise RuntimeError("Pinned strings.cc separator fixture changed")
        offset = original.index(anchor) + len(anchor)
        if original[offset:offset + 1] != b" ":
            raise RuntimeError("Unexpected parser separator fixture")
        changed = original[:offset] + b"!" + original[offset + 1:]
        assert len(changed) == len(original) and changed != original
        proof = {"path": relative, "source_bytes": len(original),
                 "mutation": "Replace the parser separator constant's space with '!' (one byte)",
                 "outside_query_timing": True, "source_restored": False,
                 "before_source_sha256": sha256(source),
                 "before_outputs": row["build_outputs"]}
        row["same_size_edit"] = proof
        self.save()
        try:
            source.write_bytes(changed)
            if source.read_bytes() != changed or source.stat().st_size != len(original):
                raise RuntimeError("Same-size source edit was not observable")
            proof["edited_source_sha256"] = sha256(source)
            phase = "same_size_edit_build"
            row["phases"][phase] = self.run(self.build_command(base, self.config["build_target"], name, phase),
                name + "-" + phase, cwd=cwd)
            evidence = self.build_evidence(cwd, name, phase)
            proof["build_evidence"] = evidence
            actual = self.canonical_outputs(evidence["outputs"])
            if actual == self.canonical_outputs(row["build_outputs"]):
                raise RuntimeError("Same-size edit did not change built output hashes")
            if not evidence["valid_action_summary"] or evidence.get("content_actions_executed", 0) == 0:
                raise RuntimeError("Same-size edit did not execute a verified content action")
            reference = self.result.get("same_size_edit_output_reference")
            if reference is None:
                if row["variant"] != "checkout":
                    raise RuntimeError("Edited build reference must come from checkout")
                reference = {"source": name, "outputs": actual}
                self.result["same_size_edit_output_reference"] = reference
            if actual != reference["outputs"]:
                raise RuntimeError("Same-size edited outputs differ across arms")
            proof.update({"output_changed": True, "matches_checkout_edited_outputs": True})
        finally:
            source.write_bytes(original)
            proof["source_restored"] = source.read_bytes() == original
            self.save()
            if not proof["source_restored"]:
                raise RuntimeError("Failed to restore original source bytes")

    def cell(self, trial, variant):
        name = f"trial-{trial}-{variant}"
        checkout = variant == "checkout"
        self.current_attribute = "user.blake3" if variant == "vfs-blake3-xattr" else ""
        repo = self.root / (name + ("-checkout" if checkout else ".git"))
        overlay = None if checkout else self.root / (name + "-overlay")
        mountpoint = self.root / (name + "-mount")
        base = self.root / (name + "-output")
        row = {"trial": trial, "variant": variant, "phases": {}, "complete": False,
               "digest_function": "BLAKE3", "digest_attribute": self.current_attribute}
        self.result["active_cell"] = row
        row["proxy_stats_before"] = self.proxy_stats()
        self.save()
        state, started, cwd = None, False, repo
        self.critical = True
        try:
            row["phases"]["clone"] = self.clone(repo, name, checkout)
            if not checkout:
                self.warm_mapping(repo, name, row["phases"])
                row["phases"]["prefetch"] = self.run(self.expand(self.config.get("prefetch_command", [
                    "{vfs}", "--git", "{git}", "prefetch-bazel", "--repo", "{repo}"]), repo), name + "-prefetch")
                state, row["phases"]["mount"] = self.mount(repo, overlay, mountpoint, name)
                cwd = mountpoint
            flags = list(dict.fromkeys(["--output=label", "--lockfile_mode=off", "--watchfs",
                "--repository_cache=" + self.config["repository_cache"], "--noshow_progress", "--color=no",
                "--curses=no", *self.config.get("query_flags", [])]))
            query = self.bazel_args(base) + ["query", self.config["query_expression"], *flags]
            started = True
            row["phases"]["cold_query"] = self.run(query + ["--profile=" + str(self.root / (name + "-cold_query.profile.gz"))], name + "-cold_query", cwd=cwd)
            row["real_wall_clone_to_first_query_seconds"] = row["phases"]["cold_query"]["wait_end_perf_counter"] - row["phases"]["clone"]["start_perf_counter"]
            self.critical = False
            if self.git(repo, ["rev-parse", "HEAD"]).decode().strip() != COMMIT:
                raise RuntimeError("Unexpected cloned revision")
            if self.git(repo, ["remote", "get-url", "origin"]).decode().strip() != self.clone_url:
                raise RuntimeError("Unexpected clone origin")
            for index in range(self.args.warm_queries + 1):
                phase = "cold_query" if index == 0 else f"warm_query_{index}"
                if index:
                    row["phases"][phase] = self.run(query, name + "-" + phase, cwd=cwd)
                row.update(self.verify_labels(name, phase))
            row["presence_after_queries"] = self.presence(repo, name, "after_queries", overlay)
            if not checkout:
                row["digest_probe"] = self.digest_probe(repo, overlay, cwd, name, row["presence_after_queries"])
            if self.args.build_proof and trial == 1:
                phase = "build_after_queries"
                row["phases"][phase] = self.run(self.build_command(base, self.config["build_target"], name), name + "-build", cwd=cwd)
                evidence = self.build_evidence(cwd, name)
                self.verify_build_outputs(evidence["outputs"], name, phase, checkout)
                row["build_outputs"] = evidence["outputs"]
                row["build_target"] = self.config["build_target"]
                row["build_evidence"] = evidence
                self.same_size_edit_proof(cwd, base, name, row)
            self.stop_bazel(base, cwd, name)
            started = False
        finally:
            self.critical = False
            if started:
                try:
                    self.stop_bazel(base, cwd, name + "-failure")
                except BaseException as error:
                    row["cleanup_error"] = repr(error)
            if state:
                self.stop_mount(mountpoint, state)
                reports = [json.loads(line.split(": ", 1)[1]) for line in
                    (self.root / (name + "-mount.stderr")).read_text().splitlines() if line.startswith("BLAKE3 xattrs: ")]
                if not reports:
                    raise RuntimeError("Missing VFS BLAKE3 xattr shutdown counters")
                counters = reports[-1]
                row["blake3_xattrs_total_including_explicit_probe"] = counters
                explicit_hits = int(row.get("digest_probe", {}).get("verified", False))
                row["explicit_probe_base_hits"] = explicit_hits
                if counters["base_hits"] < explicit_hits:
                    raise RuntimeError("Explicit mapped digest probe was not counted")
                row["bazel_blake3_xattrs"] = dict(counters, base_hits=counters["base_hits"] - explicit_hits)
                row["bazel_blake3_counter_scope"] = "queries + pristine build + same-size edit rebuild" if row.get("build_target") else "queries only"
                if row.get("build_target") and variant == "vfs-blake3-xattr" and row["bazel_blake3_xattrs"]["base_hits"] == 0:
                    raise RuntimeError("BLAKE3 build proof did not consume any mapped digest xattrs")
                if row.get("same_size_edit") and variant == "vfs-blake3-xattr" and counters["local_hashes"] == 0:
                    raise RuntimeError("BLAKE3 edited-source rebuild did not use any local-content digest xattrs")
            row["proxy_stats_after"] = self.proxy_stats()
            self.save()
        row["complete"] = True
        self.result["results"].append(row)
        self.result.pop("active_cell", None)
        self.save()
        print(json.dumps({"cell": name, "clone_to_query_seconds": row["real_wall_clone_to_first_query_seconds"]}), flush=True)

    def stop_proxy(self):
        if self.proxy_state:
            process, log = self.proxy_state
            try:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            finally:
                log.close()
                self.proxy_state = None

    def report(self):
        lines = ["# Warm Git proxy + BLAKE3 Bazel query", "",
            "Server preparation is reported separately from warm-client totals. Full mirror population was not measured.", "",
            "Both VFS arms fetch the same mappings and sizes and use blocking prefetch; only Bazel's digest attribute differs.",
            "Query may not hash inputs. Shutdown xattr totals include one explicit post-query no-hydration probe per VFS cell; do not attribute that hit to Bazel.", "",
            "| Arm | N | Clone to query median (s) | Query median (s) | Warm query median (s) |",
            "|---|---:|---:|---:|---:|"]
        for arm in ARMS:
            rows = [row for row in self.result["results"] if row["variant"] == arm]
            if not rows:
                continue
            warm = [phase["seconds"] for row in rows for key, phase in row["phases"].items() if key.startswith("warm_query_")]
            lines.append(f"| {arm} | {len(rows)} | {statistics.median(row['real_wall_clone_to_first_query_seconds'] for row in rows):.6f} | {statistics.median(row['phases']['cold_query']['seconds'] for row in rows):.6f} | {statistics.median(warm) if warm else float('nan'):.6f} |")
        lines += ["", f"Complete: {self.result['complete']}; error: {self.result.get('error', 'none')}", "",
            "Raw results retain server preparation phases, per-client phases, proxy counters, frozen tool hashes, golden label checks, and base/private object-presence manifests."]
        (self.root / "REPORT.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mirror", type=Path, required=True, help="existing full bare mirror; copied, never changed")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--warm-queries", type=int, default=1)
    parser.add_argument("--build-proof", action="store_true", help="build strings, edit one source byte, and rebuild once per arm after first-trial queries; verify outputs and digest counters")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    if args.trials < 1 or args.warm_queries < 0:
        parser.error("trials must be positive; warm queries nonnegative")
    args.workflow = "query"
    bench = WarmProxyBenchmark(args)
    if not args.run:
        print(json.dumps({"ready": True, "execution": False, "arms": ARMS,
            "required_extra_config_keys": ["proxy_binary"], "output": str(bench.root),
            "mirror": str(args.mirror), "trials": args.trials}, indent=2))
        return
    try:
        bench.freeze()
        bench.prepare_proxy()
        bench.mapping_reference = json.loads((bench.root / "server-mappings.json").read_text())
        orders = [list(ARMS[index % 3:] + ARMS[:index % 3]) for index in range(args.trials)]
        bench.result["environment"]["trial_order"] = orders
        for trial, order in enumerate(orders, 1):
            for arm in order:
                bench.cell(trial, arm)
        bench.completed_build_consistency()
        for tool in bench.result["tool_provenance"].values():
            if sha256(Path(tool["path"])) != tool["sha256"]:
                raise RuntimeError("Git or Bazel executable changed during benchmark")
        bench.result["complete"] = True
    except BaseException as error:
        bench.result["error"] = repr(error)
        bench.result["traceback"] = traceback.format_exc()
        raise
    finally:
        bench.critical = False
        bench.stop_proxy()
        bench.save()
        if bench.writable_evidence:
            bench.report()


if __name__ == "__main__":
    main()
