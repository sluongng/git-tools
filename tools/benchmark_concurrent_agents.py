#!/usr/bin/env python3
"""Two isolated VFS agents sharing one warm proxy: functional smoke, not latency evidence.

Run only after the serial primary matrix. Each agent forces three real BLAKE3
RBE actions and compares outputs against that matrix's checkout reference.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
import copy
import json
from pathlib import Path
import shutil
import threading
import traceback

from benchmark_agent_rbe import credential_helper
from benchmark_github_bazel import COMMIT, dump, sha256
from benchmark_proxy_agent_rbe import AgentRbeBenchmark


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mirror", type=Path, required=True)
    parser.add_argument("--credential-rc", type=Path, required=True)
    parser.add_argument("--reference-results", type=Path, required=True,
        help="Completed serial primary results.json with verified checkout output reference")
    parser.add_argument("--proxy-cas-sync", action="store_true")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    root = args.output.resolve()
    if root.exists() and any(root.iterdir()):
        parser.error("use a fresh empty output directory")
    if root.is_relative_to(args.mirror.resolve()):
        parser.error("output must not be inside mirror fixture")
    if not args.run:
        print(json.dumps({"execution": False, "agents": 2, "shared_proxy": True,
            "purpose": "functional correctness and overlay isolation, not performance evidence",
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
            server.credential = contexts.enter_context(credential_helper(args.credential_rc))
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
                agent.credential = contexts.enter_context(credential_helper(args.credential_rc))
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
