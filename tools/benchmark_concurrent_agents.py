#!/usr/bin/env python3
"""Two-agent correctness smoke, or opt-in paired 1/2/4/8-agent RBE measurement.

Run only after the serial primary matrix. Each agent forces three real BLAKE3
RBE actions and compares outputs against that matrix's checkout reference.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
import copy
import json
import math
import resource
import statistics
import subprocess
import time
from pathlib import Path
import shutil
import threading
import traceback

from benchmark_agent_rbe import credential_helper
from benchmark_github_bazel import COMMIT, dump, sha256
from benchmark_proxy_agent_rbe import AgentRbeBenchmark

# Single-byte C++ string contents: preserve size without introducing escapes.
EDIT_SEPARATOR_BYTES = b"!#$%&()*+,-./:;=?"


class IsolatedAgent(AgentRbeBenchmark):
    def __init__(self, args, identity, barrier):
        super().__init__(args)
        self.identity = identity
        self.barrier = barrier
        self.marker = b"git-tools isolated agent " + identity.encode() + b"\n"

    def mount(self, repo, overlay, mountpoint, name):
        state, phase = super().mount(repo, overlay, mountpoint, name)
        try:
            marker = mountpoint / ".git-tools-agent-isolation"
            marker.write_bytes(self.marker)
            # Both agents have written different bytes at the same workspace
            # path before either checks its own view or starts Bazel.
            self.barrier.wait(timeout=60)
            if marker.read_bytes() != self.marker:
                raise RuntimeError("Agent overlay marker leaked across workspaces")
            self.result["isolation"] = {"agent": self.identity,
                "same_relative_path": marker.name, "distinct_content_verified_before_build": True}
            return state, phase
        except BaseException:
            self.barrier.abort()
            self.stop_mount(mountpoint, state)
            raise

    def stop_bazel(self, base, cwd, name):
        marker = cwd / ".git-tools-agent-isolation"
        try:
            correct = marker.is_file() and marker.read_bytes() == self.marker
        except OSError:
            correct = False
        # Always shut down the owned server, including a marker failure.
        super().stop_bazel(base, cwd, name)
        if not correct:
            raise RuntimeError("Agent isolation marker changed during build")
        self.result.setdefault("isolation", {})["distinct_content_verified_after_build"] = True


def arguments_for(args, config, output):
    return argparse.Namespace(config=config, output=output, mirror=args.mirror,
        credential_rc=args.credential_rc, trials=1, workflow="build-first", warm_queries=0,
        build_proof=False, post_build_query=False, proxy_cas_sync=args.proxy_cas_sync,
        arms=["vfs-blake3-xattr"], run=True)


class PerformanceAgent(AgentRbeBenchmark):
    """Fresh isolated client; synchronize assignment, never mount completion."""
    def __init__(self, args, identity, barrier, edit_cycles):
        super().__init__(args)
        self.identity, self.barrier, self.edit_cycles = identity, barrier, edit_cycles
        self.marker = (identity + "\n").encode()
        self.did_cycles = False

    def traced_env(self, name):
        env = dict(super().traced_env(name), GIT_TOOLS_METADATA_TIMINGS="1")
        if name.endswith("-save"):
            # Native commit validates promised indexed objects. Permit and time
            # that hydration; audit commands retain NO_LAZY_FETCH.
            env.pop("GIT_NO_LAZY_FETCH", None)
        return env

    def clone(self, repo, name, checkout):
        self.barrier.wait(timeout=120)
        self.assigned = time.perf_counter()
        phase = super().clone(repo, name, checkout)
        if checkout:
            (repo / ".git-tools-agent-isolation").write_bytes(self.marker)
        return phase

    def mount(self, repo, overlay, mountpoint, name):
        state, phase = super().mount(repo, overlay, mountpoint, name)
        try:
            (mountpoint / ".git-tools-agent-isolation").write_bytes(self.marker)
            return state, phase
        except BaseException:
            self.stop_mount(mountpoint, state)
            raise

    def stop_bazel(self, base, cwd, name):
        # cell() also calls this during failure cleanup: never turn failure into
        # another build or commit, and always terminate the owned Bazel server.
        try:
            row = self.result.get("active_cell", {})
            if not self.did_cycles and "build_outputs" in row:
                self.did_cycles = True
                row["assignment_perf_counter"] = self.assigned
                row["assignment_to_first_build_seconds"] = (
                    row["phases"]["first_build"]["wait_end_perf_counter"] - self.assigned)
                start = time.perf_counter()
                for output in row["build_outputs"]:
                    # build_evidence has already verified these. Retain read
                    # latency separately, not as part of clone-to-first-build.
                    path = cwd / output["path"]
                    with path.open("rb") as stream:
                        while stream.read(1024 * 1024):
                            pass
                row["first_consumer_read_seconds"] = time.perf_counter() - start
                row["consumer_note"] = "After output verification/audits; OS cache may be warm. Not first physical hydration."
                row["edit_cycles"] = []
                source = cwd / "src/main/cpp/util/strings.cc"
                original = source.read_bytes()
                anchor = b'static const char kSeparator[] = " '
                if original.count(anchor) != 1:
                    raise RuntimeError("Pinned same-size edit fixture changed")
                for cycle in range(self.edit_cycles):
                    begin = time.perf_counter()
                    changed = original.replace(anchor, anchor[:-1] + bytes([EDIT_SEPARATOR_BYTES[cycle % len(EDIT_SEPARATOR_BYTES)]]), 1)
                    source.write_bytes(changed)
                    edit_name = name + f"-edit-{cycle + 1}"
                    workspace_log = self.credential.parent / (edit_name + ".workspace.bin")
                    grpc_log = self.credential.parent / (edit_name + ".grpc.bin")
                    try:
                        build = self.run(self.remote_build_command(base, edit_name,
                            self.root / (name + "-repository-contents-cache"), workspace_log, grpc_log),
                            edit_name, cwd=cwd)
                        edited = {"variant": "checkout"}
                        reference = self.result.pop("build_output_reference", None)
                        try:
                            self.verify_remote_build(cwd, edit_name, edited)
                        finally:
                            self.result["build_output_reference"] = reference
                        if self.canonical_outputs(edited["build_outputs"]) == self.canonical_outputs(row["build_outputs"]):
                            raise RuntimeError("Edited source did not change output")
                        save = self.run([self.config["git"], "-c", "core.hooksPath=/dev/null",
                            "-c", "user.name=Benchmark Agent", "-c", "user.email=agent@example.invalid",
                            "-c", "commit.gpgSign=false", "commit", "--only", "-m", "benchmark edit",
                            "--", "src/main/cpp/util/strings.cc"], edit_name + "-save", cwd=cwd)
                        row["edit_cycles"].append({"cycle": cycle + 1, "build": build, "save": save,
                            "edit_to_saved_seconds": time.perf_counter() - begin,
                            "save_policy": "Native Git commit; indexed promised blobs may hydrate and that cost is included",
                            "outputs": edited["build_outputs"], "remote_actions": edited["remote_actions"],
                            "forced_remote_execution_verified": edited["forced_remote_execution_verified"]})
                    finally:
                        workspace_log.unlink(missing_ok=True)
                        grpc_log.unlink(missing_ok=True)
                if (cwd / ".git-tools-agent-isolation").read_bytes() != self.marker:
                    raise RuntimeError("Agent marker leaked across private workspace")
                row["isolation_verified"] = True
        finally:
            super().stop_bazel(base, cwd, name)


def quantiles(values):
    values = sorted(values)
    return {"samples": len(values), "p50": statistics.median(values),
        "p95": values[max(0, math.ceil(.95 * len(values)) - 1)],
        "p95_method": "nearest-rank; small cohorts are not a stable tail estimate"}


def performance(args, config, expected):
    """Paired cohorts reuse one explicitly warmed service, not a cold-origin claim."""
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(Path(__file__), root / Path(__file__).name)
    result = {"complete": False, "mode": "performance", "cohorts": [],
        "config_sha256": sha256(args.config), "harness_sha256": sha256(Path(__file__)),
        "reference_results": {"path": str(args.reference_results.resolve()), "sha256": sha256(args.reference_results)},
        "credential_config": args.credential_config,
        "cache_policy": "Service preparation measured separately. Fresh private clones, overlays, Bazel roots and r2c2 clients per cohort; warm shared proxy and existing archive cache. r2c2 hits/misses measured, not assumed; remote executor state uncontrolled (action setup metrics retained); OS caches NOT dropped.",
        "cold_service_scope": "Fresh proxy mapping/pack cache on preexisting mirror; upstream population not measured. Preparation is NOT a cold first-client latency.",
        "commit_scope": "Same pinned commit across agents. Use metadata probe for distinct commits; this RBE fixture intentionally rejects arbitrary revisions.",
        "edited_output_references": {},
        "resource_scope": "Cohort disk bytes plus reaped child rusage deltas after cleanup (live shared proxy excluded). Parallel commands overlap; maxrss is process-lifetime high-water, not additive agent RSS."}
    server = None
    try:
        with ExitStack() as contexts:
            server = AgentRbeBenchmark(arguments_for(args, args.config, root / "server"))
            server.credential = contexts.enter_context(credential_helper(args.credential_rc, args.credential_config))
            server.load_proxy_credential()
            server.freeze()
            setup_start = time.perf_counter()
            server.prepare_proxy()
            result["service_preparation_seconds"] = time.perf_counter() - setup_start
            result["service_preparation"] = server.result["server_preparation"]
            for trial in range(1, args.trials + 1):
                for count in args.agent_counts:
                    # Counterbalance paired order across trials, but never mix
                    # checkout and VFS within one load cohort.
                    arms = ["checkout", "vfs-blake3-xattr"]
                    if trial % 2 == 0:
                        arms.reverse()
                    for arm in arms:
                        free_disk = shutil.disk_usage(root).free
                        available_memory = next(int(line.split()[1]) * 1024
                            for line in Path("/proc/meminfo").read_text().splitlines()
                            if line.startswith("MemAvailable:"))
                        if free_disk < args.min_free_disk_gib * 1024 ** 3:
                            raise RuntimeError("Insufficient free disk for next cohort; retained prior receipts")
                        if available_memory < count * args.min_memory_gib_per_agent * 1024 ** 3:
                            raise RuntimeError("Insufficient available memory for next cohort; retained prior receipts")
                        cohort_root = root / f"trial-{trial}-{count}-{arm}"
                        cohort_root.mkdir(parents=True)
                        barrier = threading.Barrier(count)
                        agents = []
                        for index in range(count):
                            identity = f"agent-{index + 1}"
                            agent_config = copy.deepcopy(config)
                            agent_config["output_user_root"] = str(cohort_root / (identity + "-bazel-user"))
                            config_path = cohort_root / (identity + "-config.json")
                            dump(config_path, agent_config)
                            agent = PerformanceAgent(arguments_for(args, config_path, cohort_root / identity), identity, barrier, args.edit_cycles)
                            agent.credential = contexts.enter_context(credential_helper(args.credential_rc, args.credential_config))
                            agent.freeze()
                            agent.origin, agent.clone_url = server.origin, server.clone_url
                            agent.config["source"] = server.clone_url
                            agent.config["mount_options"] = ["--blake3-proxy-url", server.origin, "--blake3-proxy-repo", "bazel"]
                            agent.result["build_output_reference"] = copy.deepcopy(expected)
                            agents.append(agent)
                        before = resource.getrusage(resource.RUSAGE_CHILDREN)
                        start = time.perf_counter()
                        cohort = {"trial": trial, "agents": count, "variant": arm, "complete": False,
                            "cache_state": "warm-proxy-new-agent",
                            "host_resources_before": {"free_disk_bytes": free_disk, "available_memory_bytes": available_memory},
                            "proxy_stats_before": server.proxy_stats(), "results": [], "errors": []}
                        result["cohorts"].append(cohort)
                        def execute(agent):
                            try:
                                agent.cell(1, arm)
                                row = agent.result["results"][0]
                                if arm != "checkout":
                                    row["bazel_blake3_counter_scope"] = "Whole mount lifetime: initial build, audits, edit builds and native Git save activity; not first-build-only"
                                row["metadata_timings"] = [dict(log=log.name, **json.loads(line.split("METADATA_TIMING ", 1)[1]))
                                    for log in agent.root.glob("*.stderr")
                                    for line in log.read_text(errors="replace").splitlines()
                                    if line.startswith("METADATA_TIMING ")]
                                agent.completed_build_consistency()
                                agent.verify_toolchain("after")
                                agent.result["complete"] = True
                            except BaseException as error:
                                barrier.abort()
                                agent.result["error"] = repr(error)
                                raise
                            finally:
                                agent.critical = False
                                agent.save()
                        with ThreadPoolExecutor(max_workers=count) as pool:
                            futures = {pool.submit(execute, agent): agent for agent in agents}
                            for future in as_completed(futures):
                                agent = futures[future]
                                try:
                                    future.result()
                                    cohort["results"].append({"identity": agent.identity,
                                        "receipt": str(agent.root / "results.json"),
                                        "row": agent.result["results"][0]})
                                except BaseException as error:
                                    cohort["errors"].append({"identity": agent.identity, "error": repr(error),
                                        "receipt": str(agent.root / "results.json")})
                        cohort["cohort_wall_including_audits_cleanup_seconds"] = time.perf_counter() - start
                        after = resource.getrusage(resource.RUSAGE_CHILDREN)
                        cohort["child_resources"] = {"user_cpu_seconds": after.ru_utime - before.ru_utime,
                            "system_cpu_seconds": after.ru_stime - before.ru_stime,
                            "lifetime_maxrss_kib": after.ru_maxrss,
                            "block_inputs": after.ru_inblock - before.ru_inblock,
                            "block_outputs": after.ru_oublock - before.ru_oublock}
                        cohort["disk_allocated_bytes"] = int(subprocess.check_output(["du", "-s", "--block-size=1", str(cohort_root)]).split()[0])
                        cohort["proxy_stats_after"] = server.proxy_stats()
                        rows = [value["row"] for value in cohort["results"]]
                        if len(rows) == count:
                            for row in rows:
                                for edit in row["edit_cycles"]:
                                    key = str(edit["cycle"])
                                    actual = AgentRbeBenchmark.canonical_outputs(edit["outputs"])
                                    expected_edit = result["edited_output_references"].setdefault(key, actual)
                                    if actual != expected_edit:
                                        raise RuntimeError("Edited outputs differ between agents or paired arms")
                            cohort["assignment_to_first_build_seconds"] = quantiles([r["assignment_to_first_build_seconds"] for r in rows])
                            cohort["assignment_to_all_first_builds_seconds"] = max(r["phases"]["first_build"]["wait_end_perf_counter"] for r in rows) - min(r["assignment_perf_counter"] for r in rows)
                            cohort["first_consumer_read_seconds"] = quantiles([r["first_consumer_read_seconds"] for r in rows])
                            cohort["edit_cycle_seconds"] = {str(index + 1): {
                                "edit_to_saved": quantiles([r["edit_cycles"][index]["edit_to_saved_seconds"] for r in rows]),
                                "build": quantiles([r["edit_cycles"][index]["build"]["seconds"] for r in rows]),
                                "save": quantiles([r["edit_cycles"][index]["save"]["seconds"] for r in rows])}
                                for index in range(args.edit_cycles)}
                            cohort["phase_seconds"] = {key: quantiles([r["phases"][key]["seconds"] for r in rows]) for key in rows[0]["phases"]}
                            cohort["complete"] = True
                        dump(root / "results.json", result)
                        if cohort["errors"]:
                            raise RuntimeError("Performance cohort failed; partial receipts retained")
            server.verify_toolchain("after")
            for tool in server.result["tool_provenance"].values():
                if sha256(Path(tool["path"])) != tool["sha256"]:
                    raise RuntimeError("Git or Bazel executable changed during performance matrix")
            result["complete"] = True
    except BaseException as error:
        result["error"] = repr(error)
        raise
    finally:
        if server:
            server.stop_proxy()
            server.cas_key = None
            server.save()
        dump(root / "results.json", result)
    print(json.dumps({"complete": result["complete"], "cohorts": len(result["cohorts"]), "results": str(root / "results.json")}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mirror", type=Path, required=True)
    parser.add_argument("--credential-rc", type=Path, required=True)
    parser.add_argument("--credential-config", choices=["remote-shared", "rbe"], default="remote-shared",
        help="Explicit credential run class selected before retrieval; no fallback")
    parser.add_argument("--reference-results", type=Path, required=True,
        help="Completed serial primary results.json with verified checkout output reference")
    parser.add_argument("--proxy-cas-sync", action="store_true")
    parser.add_argument("--performance", action="store_true", help="Opt-in paired checkout/VFS cohort timing; default remains two-agent correctness smoke")
    parser.add_argument("--agent-counts", nargs="+", type=int, default=[1, 2, 4, 8])
    parser.add_argument("--min-free-disk-gib", type=float, default=10, help="Fail before a new performance cohort below this free-disk floor")
    parser.add_argument("--min-memory-gib-per-agent", type=float, default=2, help="Required MemAvailable per concurrent performance agent")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--edit-cycles", type=int, default=1)
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.min_free_disk_gib <= 1024 or not .5 <= args.min_memory_gib_per_agent <= 128:
        parser.error("resource floors require disk1..1024GiB and memory0.5..128GiB per agent")
    if not 1 <= args.trials <= 20 or not 0 <= args.edit_cycles <= 16 or not args.agent_counts or len(set(args.agent_counts)) != len(args.agent_counts) or any(n not in (1, 2, 4, 8) for n in args.agent_counts):
        parser.error("trials1..20, edits0..16, and unique agent counts chosen from 1/2/4/8 required")
    root = args.output.resolve()
    if root.exists() and any(root.iterdir()):
        parser.error("use a fresh empty output directory")
    if root.is_relative_to(args.mirror.resolve()):
        parser.error("output must not be inside mirror fixture")
    if not args.run:
        print(json.dumps({"execution": False, "agents": args.agent_counts if args.performance else 2, "shared_proxy": True,
            "mode": "performance" if args.performance else "correctness", "trials": args.trials if args.performance else 1,
            "purpose": ("paired cohort startup/build/edit/save measurement" if args.performance else "functional correctness and overlay isolation, not performance evidence"),
            "reference_results": str(args.reference_results), "output": str(root)}, indent=2))
        return
    reference = json.loads(args.reference_results.read_text())
    config = json.loads(args.config.read_text())
    if not reference.get("complete") or reference.get("config", {}).get("commit") != COMMIT:
        parser.error("reference must be a completed pinned serial matrix")
    if not any(r.get("complete") and r.get("variant") == "checkout" for r in reference.get("results", [])):
        parser.error("reference has no completed checkout cell")
    if reference["config"].get("rbe_extra_flags", []) != config.get("rbe_extra_flags", []):
        parser.error("reference and smoke must use identical public RBE/toolchain flags")
    expected = copy.deepcopy(reference["build_output_reference"])
    AgentRbeBenchmark.canonical_outputs(expected["outputs"])
    if args.performance:
        performance(args, config, expected)
        return
    root.mkdir(parents=True, exist_ok=True)
    result = {"complete": False, "purpose": "two-agent concurrent correctness/isolation smoke; not performance evidence",
        "reference_results": {"path": str(args.reference_results.resolve()), "sha256": sha256(args.reference_results)},
        "config_sha256": sha256(args.config), "harness_sha256": sha256(Path(__file__)),
        "agents": {}, "proxy_cas_sync": args.proxy_cas_sync}
    shutil.copy2(Path(__file__), root / Path(__file__).name)
    server = None
    agents = []
    barrier = threading.Barrier(2)

    def execute(agent):
        try:
            agent.cell(1, "vfs-blake3-xattr")
            agent.completed_build_consistency()
            agent.verify_toolchain("after")
            agent.result["complete"] = True
            return agent.result
        except BaseException as error:
            barrier.abort()
            agent.result["error"] = repr(error)
            agent.result["traceback"] = traceback.format_exc()
            raise
        finally:
            agent.critical = False
            agent.save()
            agent.report()

    try:
        with ExitStack() as contexts:
            server = AgentRbeBenchmark(arguments_for(args, args.config, root / "server"))
            server.credential = contexts.enter_context(credential_helper(args.credential_rc, args.credential_config))
            server.load_proxy_credential()
            server.freeze()
            server.prepare_proxy()
            result["shared_proxy_origin"] = server.origin
            result["server_preparation"] = server.result["server_preparation"]
            for identity in ("agent-a", "agent-b"):
                agent_config = copy.deepcopy(config)
                # Separate output-user roots in addition to separate output bases.
                agent_config["output_user_root"] = str(root / (identity + "-bazel-user"))
                config_path = root / (identity + "-config.json")
                dump(config_path, agent_config)
                agent = IsolatedAgent(arguments_for(args, config_path, root / identity), identity, barrier)
                agent.credential = contexts.enter_context(credential_helper(args.credential_rc, args.credential_config))
                agent.freeze()
                agent.origin, agent.clone_url = server.origin, server.clone_url
                agent.config["source"] = server.clone_url
                agent.config["mount_options"] = ["--blake3-proxy-url", server.origin, "--blake3-proxy-repo", "bazel"]
                agent.result["build_output_reference"] = copy.deepcopy(expected)
                agent.result["environment"].update({"source": server.clone_url,
                    "purpose": "concurrent functional smoke; barrier synchronization means timings are not comparative performance evidence"})
                dump(agent.root / "execution-config.json", agent.config)
                agents.append(agent)
            # Each instance owns its mutable cell state, paths, helper directory,
            # and lifecycle. Only the proxy and immutable fixtures are shared.
            errors = []
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = {pool.submit(execute, agent): agent for agent in agents}
                for future in as_completed(futures):
                    agent = futures[future]
                    try:
                        future.result()
                        result["agents"][agent.identity] = {"complete": True,
                            "results": str(agent.root / "results.json"), "isolation": agent.result["isolation"]}
                    except BaseException as error:
                        errors.append(error)
                        result["agents"][agent.identity] = {"complete": False,
                            "results": str(agent.root / "results.json"), "error": repr(error)}
                    dump(root / "results.json", result)
            if errors:
                raise RuntimeError("Concurrent agent smoke failed; retained per-agent evidence") from errors[0]
            if any(not a.result.get("isolation", {}).get("distinct_content_verified_after_build") for a in agents):
                raise RuntimeError("Incomplete overlay isolation proof")
            server.verify_toolchain("after")
            result["final_proxy_stats"] = server.proxy_stats()
            result["complete"] = True
    except BaseException as error:
        result["error"] = repr(error)
        result["traceback"] = traceback.format_exc()
        raise
    finally:
        try:
            if server is not None:
                server.stop_proxy()
                server.cas_key = None
                server.save()
        finally:
            dump(root / "results.json", result)
    print(json.dumps({"complete": result["complete"], "agents": result["agents"]}, indent=2))


if __name__ == "__main__":
    main()
