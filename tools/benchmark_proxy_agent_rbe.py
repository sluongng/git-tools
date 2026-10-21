#!/usr/bin/env python3
"""Serial warm-proxy / fresh-agent first-RBE-build comparison; dry-run by default.

Mirror population, proxy mapping generation, and optional CAS prewarming are
retained setup costs, excluded from continuous clone-to-successful-build totals.
"""
import argparse
import json
import shutil
from pathlib import Path
import statistics
import subprocess
import time
import traceback

from benchmark_agent_rbe import (
    credential_helper, grpc_log_summary, json_records, pilot_environment, rbe_flags, rbe_settings, workspace_log_summary,
)
from benchmark_github_bazel import COMMIT, sha256
from benchmark_warm_proxy_bazel import ARMS, WarmProxyBenchmark

KEY_ENV = "GIT_TOOLS_PROXY_CAS_KEY"
TARGET = "//src/main/cpp/util:strings"


class AgentRbeBenchmark(WarmProxyBenchmark):
    def __init__(self, args):
        super().__init__(args)
        self.prefetch_mode = self.config["prefetch_mode"] = "after-mount"
        self.mount_prefetch_args = self.prefetch_mount_args()
        self.config["build_target"] = TARGET
        # Historical goldens used the local toolchain, not the Ubuntu RBE image.
        self.config.pop("golden_build_outputs", None)
        self.env = dict(pilot_environment(), GIT_NO_LAZY_FETCH="1")
        self.credential = None
        self.cas_key = None
        self.rbe_endpoint, self.cas_instance, self.rbe_extra_flags = rbe_settings(self.config)
        scheme, authority = self.rbe_endpoint.split("://", 1)
        self.cas_origin = ("https://" if scheme == "grpcs" else "http://") + authority
        self.result["environment"].update({
            "timing": "continuous clone launch through first successful RBE build exit; no prior query or timed object audits; warm-blake3 and async prefetch/mount included",
            "prefetch_mode": "after-mount",
            "prefetch_timing": "mount-owned background prefetch overlaps first build; never an additive blocking client phase",
            "cache_policy": "warm owned proxy mirror/packs/mappings; optional prewarmed remote workspace CAS; shared archive cache verified warm by prerequisite pilot; fresh source/overlay/output base/local repo-contents cache per cell; r2c2 reads enabled, hits measured rather than assumed; OS caches not dropped",
            "remote": {"endpoint": self.cas_origin, "instance": self.cas_instance, "digest": "BLAKE3", "remote_ttl_seconds": 10800,
                       "action_policy": "remote_accept_cached=true preserves r2c2; CppCompile/CppArchive/CppLink bypass action-cache results and execute remotely"},
            "query_scope": "optional golden query occurs only after successful first build; digest counters then cover build plus query",
            "credential_policy": "one explicitly selected local credential via host-restricted helper; proxy receives key only in its own environment; no secret configuration/argv/artifacts",
        })
        self.result["execution_options"].update({"proxy_cas_sync": args.proxy_cas_sync,
            "post_build_query": args.post_build_query, "force_execution": True})

    def traced_env(self, name):
        env = super().traced_env(name)
        if name == "server" and self.cas_key is not None:
            env[KEY_ENV] = self.cas_key
        return env

    def proxy_extra_args(self):
        if not self.args.proxy_cas_sync:
            return []
        return ["--cas-endpoint", self.cas_origin, "--cas-instance-name", self.cas_instance,
                "--cas-header-env", "x-buildbuddy-api-key=" + KEY_ENV,
                "--cas-remote-ttl-seconds", "10800"]

    def load_proxy_credential(self):
        if not self.args.proxy_cas_sync:
            return
        # Never log helper stdout, stderr, or parse errors containing its response.
        reply = subprocess.run([str(self.credential)], input=json.dumps({"uri": self.cas_origin}).encode(),
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                               timeout=15, env=pilot_environment())
        try:
            value = json.loads(reply.stdout)["headers"]["x-buildbuddy-api-key"]
            if reply.returncode or not isinstance(value, list) or len(value) != 1:
                raise ValueError()
            value = value[0]
            if not isinstance(value, str) or not value or any(c in value for c in "\r\n"):
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise RuntimeError("Selected credential helper did not return one valid BuildBuddy key") from None
        self.cas_key = value

    def bazel_args(self, base):
        return super().bazel_args(base) + ["--experimental_remote_repo_contents_cache"]

    def verify_toolchain(self, phase):
        manifest = getattr(self, "toolchain_manifest", None)
        if manifest is None:
            source = self.config.get("toolchain_provisioning_manifest")
            if not source:
                return
            self.toolchain_manifest = manifest = json.loads(Path(source).read_text())
            (self.root / "toolchain-provisioning.json").write_text(json.dumps(manifest, indent=2) + "\n")
        override = manifest["override_repository"]
        if "--override_repository=" + override not in self.config.get("rbe_extra_flags", []):
            raise RuntimeError("Toolchain manifest override differs from execution flags")
        directory = Path(override.split("=", 1)[1])
        expected = {row["path"]: row for row in manifest["files"]}
        actual = {str(path.relative_to(directory)): path for path in directory.rglob("*") if path.is_file()}
        if actual.keys() != expected.keys() or any(path.is_symlink() for path in directory.rglob("*")):
            raise RuntimeError("Provisioned toolchain file set changed")
        for name, path in actual.items():
            row = expected[name]
            if sha256(path) != row["sha256"] or path.stat().st_size != row["size_bytes"] or (path.stat().st_mode & 0o777) != int(row["installed_mode"], 8):
                raise RuntimeError("Provisioned toolchain hash/size/mode changed")
        self.result.setdefault("toolchain_verification", {})[phase] = {"verified": True, "files": len(actual), "manifest_sha256": sha256(self.root / "toolchain-provisioning.json")}
        self.save()

    def freeze(self):
        super().freeze()
        self.verify_toolchain("before")
        harness = self.root / "harness"
        harness.mkdir()
        sources = (Path(__file__).name, "benchmark_warm_proxy_bazel.py",
                   "benchmark_github_bazel.py", "benchmark_agent_rbe.py")
        for name in sources:
            shutil.copy2(Path(__file__).with_name(name), harness / name)
        self.result["harness_provenance"] = {name: sha256(harness / name) for name in sources}
        self.result["harness_source_directory"] = str(harness)
        self.save()

    def prepare_proxy(self):
        # Parent setup primes both clone pack variants and prefetch, not clients.
        preparation_started = time.perf_counter()
        super().prepare_proxy()
        setup = self.result["server_preparation"]
        if self.args.proxy_cas_sync:
            start = time.perf_counter()
            while True:
                stats = self.proxy_stats().get("cas_sync")
                if not isinstance(stats, dict):
                    raise RuntimeError("Proxy CAS sync stats are unavailable")
                setup["cas_prewarm"] = {"excluded_from_client_totals": True,
                    "wait_seconds_after_other_setup": time.perf_counter() - start,
                    "server_preparation_through_cas_settled_seconds": time.perf_counter() - preparation_started,
                    "stats": stats,
                    "note": "Asynchronous CAS jobs also overlap mapping and pack preparation; this wait is not total CAS generation/upload duration"}
                self.save()
                if stats.get("failed_jobs", 0) or stats.get("queue_rejections", 0) or stats.get("cancelled_jobs", 0):
                    raise RuntimeError("Proxy CAS prewarm failed; retained server stats identify the failure class")
                if stats.get("accepted_jobs", 0) > 0 and stats.get("pending_objects") == 0:
                    if stats.get("confirmed_objects", 0) < 11850:
                        raise RuntimeError("Proxy CAS warmup did not confirm the complete snapshot")
                    break
                if time.perf_counter() - start >= 600:
                    raise RuntimeError("Fixed 600-second CAS setup wait exceeded; not retried")
                time.sleep(.1)
        self.save()

    def remote_build_command(self, base, name, contents_cache, raw_workspace_log, raw_grpc_log):
        return self.bazel_args(base) + ["build", TARGET, *rbe_flags(
            self.credential, force_execution=True, repo_contents_cache=contents_cache,
            accept_remote_cache=True, repository_cache=self.config["repository_cache"],
            endpoint=self.rbe_endpoint, instance=self.cas_instance, extra_flags=self.rbe_extra_flags),
            "--watchfs", "--noshow_progress", "--color=no", "--curses=no",
            "--build_event_publish_all_actions",
            "--profile=" + str(self.root / (name + "-first_build.profile.gz")),
            "--build_event_json_file=" + str(self.root / (name + "-first_build.bep.jsonl")),
            "--execution_log_json_file=" + str(self.root / (name + "-first_build.execution.json")),
            "--experimental_workspace_rules_log_file=" + str(raw_workspace_log),
            "--remote_grpc_log=" + str(raw_grpc_log)]

    def verify_remote_build(self, cwd, name, row):
        actions = json_records(self.root / (name + "-first_build.execution.json"))
        required = {"CppCompile", "CppArchive", "CppLink"}
        content = [a for a in actions if a.get("mnemonic") in required]
        observed = {a.get("mnemonic") for a in content if a.get("runner") == "remote"
                    and not a.get("cacheHit", a.get("cache_hit", False))}
        row["remote_actions"] = [{"mnemonic": a.get("mnemonic"), "runner": a.get("runner"),
            "cache_hit": a.get("cacheHit", a.get("cache_hit", False)),
            "digest_function": a.get("digest", {}).get("hashFunctionName"),
            "metrics": a.get("metrics", {})} for a in content]
        row["action_metrics_note"] = "Action metrics overlap and are not additive command phases"
        row["forced_remote_execution_verified"] = len(content) == 3 and observed == required and all(
            a.get("runner") == "remote" and not a.get("cacheHit", a.get("cache_hit", False))
            and a.get("digest", {}).get("hashFunctionName") == "BLAKE3" for a in content)
        if not row["forced_remote_execution_verified"]:
            raise RuntimeError("Expected exactly three uncached remote BLAKE3 CppCompile/CppArchive/CppLink actions")
        evidence = self.build_evidence(cwd, name, "first_build")
        if not evidence["valid_action_summary"]:
            raise RuntimeError("Missing or inconsistent BEP action summary")
        self.verify_build_outputs(evidence["outputs"], name, "first_build", row["variant"] == "checkout")
        row.update(build_target=TARGET, build_outputs=evidence["outputs"], build_evidence=evidence)

    def cell(self, trial, variant):
        name = f"trial-{trial}-{variant}"
        checkout = variant == "checkout"
        self.current_attribute = "user.blake3" if variant == "vfs-blake3-xattr" else ""
        repo = self.root / (name + ("-checkout" if checkout else ".git"))
        overlay = None if checkout else self.root / (name + "-overlay")
        mountpoint, base = self.root / (name + "-mount"), self.root / (name + "-output")
        contents = self.root / (name + "-repository-contents-cache")
        raw_workspace_log = self.credential.parent / (name + "-workspace-events.bin")
        raw_grpc_log = self.credential.parent / (name + "-remote-grpc.bin")
        row = {"trial": trial, "variant": variant, "phases": {}, "complete": False,
            "digest_function": "BLAKE3", "digest_attribute": self.current_attribute,
            "local_repo_contents_cache": str(contents), "prefetch_mode": "not-applicable" if checkout else "after-mount"}
        self.result["active_cell"] = row
        row["proxy_stats_before"] = self.proxy_stats()
        self.save()
        state, started, cwd = None, False, repo
        try:
            self.critical = True
            row["phases"]["clone"] = self.clone(repo, name, checkout)
            if not checkout:
                self.warm_mapping(repo, name, row["phases"])
                state, row["phases"]["mount"] = self.mount(repo, overlay, mountpoint, name)
                cwd = mountpoint
            started = True
            row["phases"]["first_build"] = self.run(self.remote_build_command(base, name, contents, raw_workspace_log, raw_grpc_log),
                name + "-first_build", cwd=cwd, timeout=600)
            row["real_wall_clone_to_first_build_seconds"] = row["phases"]["first_build"]["wait_end_perf_counter"] - row["phases"]["clone"]["start_perf_counter"]
            self.critical = False
            row["workspace_rule_events"] = workspace_log_summary(raw_workspace_log)
            row["r2c2_rpc"] = grpc_log_summary(raw_grpc_log)
            row["r2c2_rpc_scope"] = "timed first build only; method, repository/action ID, numeric status and action-cache key retained; no response bodies or status details"
            row["r2c2_evidence"] = "Match GetActionResult repository action IDs and action_digest keys across cells: status 0 is a successful lookup, 5 is NOT_FOUND. Workspace operations show repository-rule execution; flag enablement or absence alone does not prove a hit."
            self.verify_remote_build(cwd, name, row)
            if self.git(repo, ["rev-parse", "HEAD"]).decode().strip() != COMMIT:
                raise RuntimeError("Unexpected cloned revision")
            if self.git(repo, ["remote", "get-url", "origin"]).decode().strip() != self.clone_url:
                raise RuntimeError("Unexpected clone origin")
            row["presence_after_first_build"] = self.presence(repo, name, "after_first_build", overlay)
            if not checkout:
                row["background_prefetch_at_build_exit"] = self.background_prefetch(name)
            if self.args.post_build_query:
                flags = ["--output=label", "--lockfile_mode=off", "--watchfs", "--noshow_progress", "--color=no", "--curses=no"]
                # Preserve the same RBE/r2c2 context for repository resolution.
                flags += rbe_flags(self.credential, True, contents, True, self.config["repository_cache"],
                                   endpoint=self.rbe_endpoint, instance=self.cas_instance,
                                   extra_flags=self.rbe_extra_flags)
                row["phases"]["post_build_query"] = self.run(self.bazel_args(base) + ["query", self.config["query_expression"], *flags], name + "-post_build_query", cwd=cwd)
                row.update(self.verify_labels(name, "post_build_query"))
            self.stop_bazel(base, cwd, name)
            started = False
            row["complete"] = True
        finally:
            self.critical = False
            # Keep only a safe operation summary, even for failed builds.
            if raw_workspace_log.exists():
                try:
                    if "workspace_rule_events" not in row:
                        row["workspace_rule_events"] = workspace_log_summary(raw_workspace_log)
                except Exception as error:
                    row["workspace_rule_summary_error"] = type(error).__name__
                finally:
                    raw_workspace_log.unlink()
            if raw_grpc_log.exists():
                try:
                    if "r2c2_rpc" not in row:
                        row["r2c2_rpc"] = grpc_log_summary(raw_grpc_log)
                except Exception as error:
                    row["r2c2_rpc_summary_error"] = type(error).__name__
                finally:
                    raw_grpc_log.unlink()
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
                    raise RuntimeError("Missing VFS digest shutdown counters")
                row["bazel_blake3_xattrs"] = reports[-1]
                row["bazel_blake3_counter_scope"] = "first build plus post-build query" if self.args.post_build_query else "first build only"
                row["background_prefetch_after_shutdown"] = self.background_prefetch(name)
            row["proxy_stats_after"] = self.proxy_stats()
            self.save()
        self.result["results"].append(row)
        self.result.pop("active_cell", None)
        self.save()
        print(json.dumps({"cell": name, "clone_to_first_build_seconds": row["real_wall_clone_to_first_build_seconds"]}), flush=True)

    def report(self):
        lines = ["# Warm proxy: first Bazel RBE build", "",
            "Server preparation is excluded and retained in results.json. Fresh clients and local r2c2 caches; shared archive cache warm. No query precedes a build.", "",
            "| Arm | N | Clone → build median (s) | Build median (s) |",
            "|---|---:|---:|---:|"]
        for arm in ARMS:
            rows = [r for r in self.result["results"] if r["variant"] == arm]
            if rows:
                lines.append(f"| {arm} | {len(rows)} | {statistics.median(r['real_wall_clone_to_first_build_seconds'] for r in rows):.6f} | {statistics.median(r['phases']['first_build']['seconds'] for r in rows):.6f} |")
        lines += ["", f"Complete: {self.result['complete']}; error: {self.result.get('error', 'none')}", "",
            "BLAKE3 xattr ablation uses identical VFS mappings and async prefetch. Action metrics overlap; they are not additive wall-time phases. r2c2 enablement alone does not prove hits."]
        (self.root / "REPORT.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mirror", type=Path, required=True, help="Existing standalone full bare mirror, copied before mutation")
    parser.add_argument("--credential-rc", type=Path, required=True, help="Explicitly selected local credential rc; never copied")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--arms", nargs="+", choices=ARMS, default=list(ARMS), help="Diagnostics may select checkout only; comparisons require all arms")
    parser.add_argument("--proxy-cas-sync", action="store_true", help="Prewarm snapshot BLAKE3 CAS through the proxy before timing clients")
    parser.add_argument("--post-build-query", action="store_true", help="Verify golden query after timed first build; digest counters then include query")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("trials must be positive")
    if len(set(args.arms)) != len(args.arms) or args.arms[0] != "checkout":
        parser.error("arms must be unique and start with checkout to establish the output reference")
    args.workflow, args.warm_queries, args.build_proof = "build-first", 0, False
    bench = AgentRbeBenchmark(args)
    if not args.run:
        print(json.dumps({"ready": True, "execution": False, "arms": ARMS, "trials": args.trials,
            "target": TARGET, "proxy_cas_sync": args.proxy_cas_sync,
            "prefetch": "asynchronous after mount", "output": str(bench.root)}, indent=2))
        return
    try:
        with credential_helper(args.credential_rc, endpoint=bench.rbe_endpoint) as helper:
            bench.credential = helper
            bench.load_proxy_credential()
            bench.freeze()
            bench.prepare_proxy()
            arms = tuple(args.arms)
            orders = [list(arms[i % len(arms):] + arms[:i % len(arms)]) for i in range(args.trials)]
            bench.result["environment"]["trial_order"] = orders
            for trial, order in enumerate(orders, 1):
                for arm in order:
                    bench.cell(trial, arm)
            bench.completed_build_consistency()
            bench.verify_toolchain("after")
            for tool in bench.result["tool_provenance"].values():
                if sha256(Path(tool["path"])) != tool["sha256"]:
                    raise RuntimeError("Git or Bazel executable changed during matrix")
            bench.result["complete"] = True
    except BaseException as error:
        bench.result["error"] = repr(error)
        bench.result["traceback"] = traceback.format_exc()
        raise
    finally:
        bench.critical = False
        bench.stop_proxy()
        bench.cas_key = None
        bench.save()
        if bench.writable_evidence:
            bench.report()


if __name__ == "__main__":
    main()
