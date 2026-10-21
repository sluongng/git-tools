#!/usr/bin/env python3
"""One real-RBE readiness pilot; timings are not comparative benchmark results."""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
from urllib.parse import urlsplit

DEFAULT_RBE_ENDPOINT = "grpcs://remote.buildbuddy.io"

HELPER = r'''#!/usr/bin/env python3
import json, shlex, sys
from pathlib import Path
from urllib.parse import urlsplit
try:
    request = json.load(sys.stdin)
    uri = urlsplit(request["uri"])
    if (uri.hostname != __CREDENTIAL_HOST__ or (uri.port or (443 if uri.scheme in ("https", "grpcs") else 80)) != __CREDENTIAL_PORT__
            or uri.scheme not in __CREDENTIAL_SCHEMES__ or uri.username is not None
            or uri.password is not None):
        raise ValueError("unsupported host")
    selected = []
    for line in Path(__CREDENTIAL_RC__).open():
        if not line.startswith(__CREDENTIAL_CONFIG__ + " "):
            continue
        words = shlex.split(line, comments=True)
        selected.extend(w.split("=", 1)[1] for w in words[1:] if w.startswith("--remote_header="))
    if len(selected) != 1:
        raise ValueError("expected exactly one selected credential")
    name, value = selected[0].split("=", 1)
    if name.lower() != "x-buildbuddy-api-key" or not value or any(c in value for c in "\r\n"):
        raise ValueError("unsupported credential format")
    json.dump({"headers": {name: [value]}}, sys.stdout)
except Exception:
    print("Selected BuildBuddy credential is unavailable or request host is unsupported", file=sys.stderr)
    sys.exit(1)
'''


@contextmanager
def credential_helper(rc_path, config="remote-shared", *, endpoint=DEFAULT_RBE_ENDPOINT):
    """Yield a host-restricted helper; the selected credential never leaves memory."""
    if config not in {"remote-shared", "rbe"}:
        raise ValueError("Unknown credential configuration class")
    endpoint_uri = urlsplit(validate_rbe_endpoint(endpoint))
    secure = endpoint_uri.scheme == "grpcs"
    port = endpoint_uri.port or (443 if secure else 80)
    schemes = ("grpcs", "https") if secure else ("grpc", "http")
    with tempfile.TemporaryDirectory(prefix="git-tools-rbe-credential-") as directory:
        helper_dir = Path(directory)
        os.chmod(helper_dir, 0o700)
        helper = helper_dir / "credential-helper"
        helper.write_text(HELPER.replace("__CREDENTIAL_RC__", repr(str(Path(rc_path).resolve())))
                          .replace("__CREDENTIAL_CONFIG__", repr("common:" + config))
                          .replace("__CREDENTIAL_HOST__", repr(endpoint_uri.hostname))
                          .replace("__CREDENTIAL_PORT__", repr(port))
                          .replace("__CREDENTIAL_SCHEMES__", repr(schemes)))
        helper.chmod(0o700)
        yield helper


def validate_rbe_endpoint(endpoint):
    """Accept a single gRPC authority, never userinfo or URL routing suffixes."""
    if not isinstance(endpoint, str) or any(c.isspace() for c in endpoint):
        raise ValueError("rbe_endpoint must be a gRPC URL")
    uri = urlsplit(endpoint)
    if (uri.scheme not in {"grpc", "grpcs"} or not uri.hostname
            or uri.username is not None or uri.password is not None
            or uri.path or uri.query or uri.fragment):
        raise ValueError("rbe_endpoint must contain only a gRPC scheme and authority")
    # Accessing port rejects malformed/out-of-range port numbers.
    uri.port
    return endpoint


def rbe_settings(config):
    """Read portable benchmark settings from the existing private JSON config."""
    endpoint = validate_rbe_endpoint(config.get("rbe_endpoint", DEFAULT_RBE_ENDPOINT))
    instance = config.get("rbe_instance_name", "")
    extra = config.get("rbe_extra_flags", [])
    if not isinstance(instance, str) or any(c in instance for c in "\r\n"):
        raise ValueError("rbe_instance_name must be a single-line string")
    if not isinstance(extra, list) or any(not isinstance(flag, str) for flag in extra):
        raise ValueError("rbe_extra_flags must be a list of Bazel flags")
    connection_flags = ("--remote_executor", "--remote_cache", "--remote_instance_name", "--credential_helper")
    if any(flag.split("=", 1)[0] in connection_flags for flag in extra):
        raise ValueError("Configure RBE connections with rbe_endpoint and rbe_instance_name, not rbe_extra_flags")
    return endpoint, instance, extra


def rbe_flags(helper, force_execution=True, repo_contents_cache=None,
              accept_remote_cache=True, repository_cache=None, *,
              endpoint=DEFAULT_RBE_ENDPOINT, instance="", extra_flags=()):
    """Public pilot configuration; caller owns fresh output/contents directories."""
    if force_execution:
        accept_remote_cache = True  # Do not disable r2c2 while bypassing action results.
    endpoint = validate_rbe_endpoint(endpoint)
    flags = ["--nogoogle_default_credentials",
             "--remote_executor=" + endpoint,
             "--remote_cache=" + endpoint, "--remote_instance_name=" + instance,
             f"--credential_helper={urlsplit(endpoint).hostname}={helper}",
             "--strategy=CppCompile=remote", "--strategy=CppLink=remote",
             "--remote_accept_cached=" + str(accept_remote_cache).lower(),
             "--remote_download_toplevel", "--experimental_remote_cache_ttl=3h",
             "--remote_timeout=120s", "--jobs=8", "--disk_cache=", "--lockfile_mode=off"]
    if force_execution:
        flags += ["--strategy=CppArchive=remote",
                  "--modify_execution_info=CppCompile=+no-remote-cache,CppArchive=+no-remote-cache,CppLink=+no-remote-cache"]
    if repository_cache is not None:
        flags.append(f"--repository_cache={repository_cache}")
    if repo_contents_cache is not None:
        flags.append(f"--repo_contents_cache={repo_contents_cache}")
    return flags + list(extra_flags)


def pilot_environment():
    """Public-workload environment; credentials come only from the scoped helper."""
    names = {"PATH", "HOME", "USER", "LOGNAME", "SHELL", "TMPDIR", "XDG_RUNTIME_DIR",
             "LANG", "LC_ALL", "JAVA_HOME"}
    return {key: value for key, value in os.environ.items() if key in names}


def _varint(data, offset):
    value = 0
    for shift in range(0, 70, 7):
        if offset >= len(data):
            raise ValueError("truncated workspace event")
        byte = data[offset]
        offset += 1
        value |= (byte & 127) << shift
        if byte < 128:
            return value, offset
    raise ValueError("oversized workspace event varint")


def _wire_fields(data):
    """Iterate proto3 scalar/message fields without decoding arbitrary payloads."""
    offset = 0
    while offset < len(data):
        tag, offset = _varint(data, offset)
        field, wire = tag >> 3, tag & 7
        if field == 0:
            raise ValueError("invalid protobuf field zero")
        if wire == 0:
            value, offset = _varint(data, offset)
        elif wire in (1, 5):
            width = 8 if wire == 1 else 4
            value, offset = data[offset:offset + width], offset + width
        elif wire == 2:
            length, offset = _varint(data, offset)
            value, offset = data[offset:offset + length], offset + length
        else:
            raise ValueError("unsupported protobuf wire type")
        if offset > len(data):
            raise ValueError("protobuf field exceeds message boundary")
        yield field, value


def grpc_log_summary(path):
    """Summarize Bazel 9.2 delimited remote_execution_log.LogEntry records.

    Returns {total_calls, method_status_counts, bytes_read, bytes_written, calls}.
    Each call retains method, action_id, status_code, optional action_digest
    {hash,size_bytes} for Get/UpdateActionResult, and optional bytes_read/written.
    Status messages/details, response bodies, resource names, and request payloads
    are omitted. Raw private logs remain owned/deleted by the caller. Metadata
    action_id is the repository name for r2c2 and an action hash for build RPCs.
    """
    data = Path(path).read_bytes() if Path(path).exists() else b""
    offset, calls, counts, read_bytes, written_bytes = 0, [], {}, 0, 0
    while offset < len(data):
        length, offset = _varint(data, offset)
        end = offset + length
        if end > len(data):
            raise ValueError("truncated delimited gRPC log entry")
        entry = dict(_wire_fields(data[offset:end]))
        offset = end
        metadata = dict(_wire_fields(entry.get(1, b"")))
        status = dict(_wire_fields(entry.get(2, b"")))
        row = {"method": entry.get(3, b"").decode("utf-8", errors="replace"),
               "action_id": metadata.get(2, b"").decode("utf-8", errors="replace"),
               "status_code": status.get(1, 0)}
        details = dict(_wire_fields(entry.get(4, b"")))
        for field in (8, 13):  # GetActionResultDetails / UpdateActionResultDetails
            if field in details:
                action_details = dict(_wire_fields(details[field]))
                request = dict(_wire_fields(action_details.get(1, b"")))
                digest = dict(_wire_fields(request.get(2, b"")))
                row["action_digest"] = {"hash": digest.get(1, b"").decode("ascii", errors="replace"),
                                        "size_bytes": digest.get(2, 0)}
        if 5 in details:  # ReadDetails.bytes_read = 3
            count = dict(_wire_fields(details[5])).get(3, 0)
            row["bytes_read"] = count
            read_bytes += count
        if 6 in details:  # WriteDetails.bytes_sent = 3
            count = dict(_wire_fields(details[6])).get(3, 0)
            row["bytes_written"] = count
            written_bytes += count
        key = row["method"] + " status=" + str(row["status_code"])
        counts[key] = counts.get(key, 0) + 1
        calls.append(row)
    return {"total_calls": len(calls), "method_status_counts": counts,
            "bytes_read": read_bytes, "bytes_written": written_bytes, "calls": calls}


def workspace_log_summary(path):
    """Keep context/type counts only, never execute environments or file contents."""
    data = Path(path).read_bytes() if Path(path).exists() else b""
    offset, counts, total = 0, {}, 0
    event_names = {3: "execute", 4: "download", 5: "download_and_extract", 6: "file",
                   7: "os", 8: "symlink", 9: "template", 10: "which", 11: "extract",
                   12: "read", 13: "delete", 14: "patch", 15: "rename", 16: "load_wasm", 17: "execute_wasm"}
    while offset < len(data):
        length, offset = _varint(data, offset)
        end = offset + length
        if end > len(data):
            raise ValueError("truncated delimited workspace event")
        context, event = "", "unknown"
        while offset < end:
            tag, offset = _varint(data, offset)
            field, wire = tag >> 3, tag & 7
            if field in event_names:
                event = event_names[field]
            if wire == 0:
                _, offset = _varint(data, offset)
            elif wire in (1, 5):
                offset += 8 if wire == 1 else 4
            elif wire == 2:
                size, offset = _varint(data, offset)
                if field == 2:
                    context = data[offset:offset + size].decode("utf-8", errors="replace")
                offset += size
            else:
                raise ValueError("unsupported workspace-event wire type")
            if offset > end:
                raise ValueError("workspace field exceeds event boundary")
        count = counts.setdefault(context, {})
        count[event] = count.get(event, 0) + 1
        total += 1
    return {"total_events": total, "by_context": counts,
            "note": "Repository-context operations show rule execution; module-extension events are separate. Absence alone does not prove remote hits."}


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for data in iter(lambda: f.read(1 << 20), b""):
            h.update(data)
    return h.hexdigest()


def json_records(path):
    if not path.exists():
        return []
    text = path.read_text()
    decoder, records = json.JSONDecoder(), []
    while text.strip():
        obj, end = decoder.raw_decode(text.lstrip())
        records.extend(obj if isinstance(obj, list) else [obj])
        text = text.lstrip()[end:]
    return records


def terminate_owned_bazel(output_base):
    """Attempt SIGTERM only for this output base's UID/PID/cmdline-verified server."""
    base = Path(output_base).resolve()
    pid_file = base / "server/server.pid.txt"
    pidfd = None
    try:
        pid = int(pid_file.read_text().strip())
        if pid <= 1 or pid == os.getpid():
            return {"status": "refused-invalid-pid"}
        process = Path("/proc") / str(pid)
        if process.stat().st_uid != os.getuid():
            return {"status": "refused-unowned-pid"}
        if hasattr(os, "pidfd_open"):
            pidfd = os.pidfd_open(pid)
        argv = process.joinpath("cmdline").read_bytes().split(b"\0")
        expected = str(base).encode()
        matches = b"--output_base=" + expected in argv or any(
            arg == b"--output_base" and index + 1 < len(argv) and argv[index + 1] == expected
            for index, arg in enumerate(argv))
        if not matches or b"com.google.devtools.build.lib.bazel.Bazel" not in argv:
            return {"status": "refused-output-base-or-server-mismatch"}
        if pidfd is not None and hasattr(signal, "pidfd_send_signal"):
            signal.pidfd_send_signal(pidfd, signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGTERM)
        return {"status": "sigterm-attempted", "pid": pid}
    except (FileNotFoundError, ProcessLookupError):
        return {"status": "server-already-absent"}
    except (OSError, ValueError) as exc:
        return {"status": "fallback-unavailable", "error_type": type(exc).__name__}
    finally:
        if pidfd is not None:
            os.close(pidfd)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", type=Path, required=True, help="Existing frozen public Bazel benchmark configuration")
    p.add_argument("--workspace", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--fresh-repo-contents-cache", action="store_true",
                   help="Use a private empty local repo-contents cache, retaining shared archive cache")
    p.add_argument("--force-execution", action="store_true",
                   help="Allow r2c2 reads but bypass remote action cache for CppCompile/CppArchive/CppLink")
    p.add_argument("--accept-remote-cache", action="store_true",
                   help="Enable both remote action-cache and r2c2 reads; default proves fresh execution")
    p.add_argument("--arm", choices=("checkout", "vfs-blake3-normal", "vfs-blake3-xattr"), default="checkout",
                   help="Workspace is provisioned/mounted by caller; xattr arm enables only digest reuse")
    p.add_argument("--credential-rc", type=Path, required=True, help="Existing local rc containing exactly one common:remote-shared API-key header; never copied")
    p.add_argument("--run", action="store_true")
    args = p.parse_args()
    if args.force_execution:
        args.accept_remote_cache = True
    config = json.loads(args.config.read_text())
    endpoint, instance, extra_flags = rbe_settings(config)
    workspace = args.workspace.resolve()
    root = args.output.resolve()
    if root == workspace or root.is_relative_to(workspace):
        p.error("output must be outside workspace")
    if not args.run:
        print(json.dumps({"mode": "dry-run", "workspace": str(workspace), "output": str(root),
                          "target": "//src/main/cpp/util:strings", "endpoint": endpoint,
                          "arm": args.arm, "cache_reads": args.accept_remote_cache, "force_execution": args.force_execution, "execution": "forced remote CppCompile and CppLink"}, indent=2))
        return
    if root.exists() and any(root.iterdir()):
        p.error("use a fresh output directory")
    revision = subprocess.check_output([config["git"], "-C", str(workspace), "rev-parse", "HEAD"], text=True).strip()
    if revision != config["commit"]:
        p.error("workspace does not match pinned configuration")
    if (workspace / "user.bazelrc").exists():
        p.error("pilot requires checkout without an imported user.bazelrc")
    root.mkdir(parents=True, exist_ok=True)
    result = {"status": "started", "purpose": "single RBE readiness pilot, not comparative latency evidence",
              "revision": revision, "workspace": str(workspace), "endpoint": endpoint,
              "instance": instance, "arm": args.arm, "force_execution": args.force_execution, "target": "//src/main/cpp/util:strings",
              "cache_policy": "fresh local output base; existing repository cache; remote action/r2c2 reads " + ("enabled" if args.accept_remote_cache else "disabled") + "; executor/CAS state unspecified",
              "credential_source": "one existing common:remote-shared remote_header via host-restricted helper; no credential values persisted"}
    baseline = {}
    for relative in config["build_output_paths"]:
        candidate = workspace / relative
        if candidate.is_file():
            baseline[relative] = {"size_bytes": candidate.stat().st_size, "sha256": sha256(candidate)}
    result["provenance"] = {"harness_sha256": sha256(Path(__file__)), "config_sha256": sha256(args.config),
                            "bazel_sha256": sha256(Path(config["bazel"]).resolve()),
                            "git_sha256": sha256(Path(shutil.which(config["git"]) or config["git"]).resolve())}
    result["preexisting_local_output_reference"] = baseline
    def save():
        (root / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    with credential_helper(args.credential_rc, endpoint=endpoint) as helper:
        startup = [config["bazel"], "--nosystem_rc", "--nohome_rc",
                   f"--output_user_root={root / 'bazel-user'}", f"--output_base={root / 'bazel-output'}",
                   "--experimental_remote_repo_contents_cache", "--digest_function=BLAKE3",
                   "--unix_digest_hash_attribute_name=" + ("user.blake3" if args.arm == "vfs-blake3-xattr" else "")]
        contents_cache = root / "repository-contents-cache" if args.fresh_repo_contents_cache else None
        raw_workspace_log = helper.parent / "workspace-events.bin"
        command = startup + ["build", result["target"]] + rbe_flags(
            helper, args.force_execution, contents_cache, args.accept_remote_cache, config["repository_cache"],
            endpoint=endpoint, instance=instance, extra_flags=extra_flags)
        command += ["--noshow_progress", "--color=no", "--curses=no",
                    f"--build_event_json_file={root / 'build.bep.jsonl'}", "--build_event_publish_all_actions",
                    f"--profile={root / 'build.profile.gz'}", f"--execution_log_json_file={root / 'build.execution.json'}",
                    f"--experimental_workspace_rules_log_file={raw_workspace_log}"]
        result["local_repo_contents_cache"] = str(contents_cache) if contents_cache else "Bazel default/shared with configured repository cache"
        environment = pilot_environment()
        result["environment_policy"] = {"names": sorted(environment), "note": "small public-workload whitelist; no environment values recorded"}
        result["command"] = command
        save()
        try:
            started = time.monotonic()
            with (root / "build.stdout.log").open("wb") as stdout, (root / "build.stderr.log").open("wb") as stderr:
                try:
                    completed = subprocess.run(command, cwd=workspace, stdout=stdout, stderr=stderr, timeout=600, env=environment)
                    result["exit_code"] = completed.returncode
                except subprocess.TimeoutExpired:
                    result["exit_code"] = None
                    result["failure"] = "fixed 600-second pilot wall budget exceeded; not retried"
            result["wall_seconds"] = time.monotonic() - started
            result["workspace_rule_events"] = workspace_log_summary(raw_workspace_log)
            actions = json_records(root / "build.execution.json")
            summary = {}
            for action in actions:
                key = (action.get("mnemonic", "unknown"), action.get("runner", "unknown"), bool(action.get("cacheHit", action.get("cache_hit", False))))
                label = f"{key[0]} runner={key[1]} cache_hit={key[2]}"
                summary[label] = summary.get(label, 0) + 1
            result["repository_contents_cache_evidence"] = ("r2c2 reads enabled, hits not independently established" if args.accept_remote_cache else "r2c2 startup enabled but reads disabled by remote_accept_cached=false")
            result["action_metrics"] = [{"mnemonic": a.get("mnemonic"), "runner": a.get("runner"),
                "cache_hit": a.get("cacheHit", a.get("cache_hit", False)),
                "digest_function": a.get("digest", {}).get("hashFunctionName"), "metrics": a.get("metrics", {})} for a in actions]
            result["metrics_note"] = "Action metrics can overlap and are not additive command phases; setup time is not compilation time"
            result["spawn_summary"] = summary
            result["remote_spawn_count"] = sum(1 for a in actions if a.get("runner") == "remote" and not a.get("cacheHit", a.get("cache_hit", False)))
            result["remote_cache_hit_spawn_count"] = sum(1 for a in actions if "remote" in a.get("runner", "") and a.get("cacheHit", a.get("cache_hit", False)))
            outputs = {}
            for relative in config["build_output_paths"]:
                candidate = workspace / relative
                if candidate.is_file():
                    outputs[relative] = {"size_bytes": candidate.stat().st_size, "sha256": sha256(candidate),
                        "matches_previous_local_toolchain": baseline.get(relative, {}).get("sha256") == sha256(candidate)}
            result["outputs"] = outputs
            if args.force_execution:
                required = {"CppCompile", "CppArchive", "CppLink"}
                observed = {a.get("mnemonic") for a in actions if a.get("runner") == "remote" and not a.get("cacheHit", a.get("cache_hit", False))}
                result["force_execution_verified"] = required <= observed
            else:
                result["force_execution_verified"] = None
            result["output_comparison_note"] = "Ubuntu RBE and prior native local toolchains may legitimately produce different bytes"
            result["status"] = "complete" if result.get("exit_code") == 0 and (result["remote_spawn_count"] + result["remote_cache_hit_spawn_count"]) > 0 and len(outputs) == len(config["build_output_paths"]) and all(o["size_bytes"] > 0 for o in outputs.values()) and (not args.force_execution or result["force_execution_verified"]) else "failed"
        except Exception as exc:
            result["status"] = "failed"
            result["failure"] = type(exc).__name__ + ": " + str(exc)
        finally:
            save()
            with (root / "shutdown.log").open("wb") as log:
                try:
                    shutdown = subprocess.run(startup + ["shutdown"], cwd=workspace, stdout=log, stderr=log, timeout=45, env=environment)
                    result["shutdown_exit_code"] = shutdown.returncode
                    if shutdown.returncode != 0:
                        result["shutdown_failure"] = "Bazel shutdown returned nonzero"
                except subprocess.TimeoutExpired:
                    result["shutdown_failure"] = "45-second shutdown budget exceeded"
                except OSError as exc:
                    result["shutdown_failure"] = "Bazel shutdown launch failed: " + type(exc).__name__
            if "shutdown_failure" in result:
                result["status"] = "failed"
                result["shutdown_fallback"] = terminate_owned_bazel(root / "bazel-output")
            save()
    print(json.dumps({k: result[k] for k in ("status", "wall_seconds", "exit_code", "remote_spawn_count") if k in result}, indent=2))
    if result["status"] != "complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
