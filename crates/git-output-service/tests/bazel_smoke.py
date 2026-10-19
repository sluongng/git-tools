#!/usr/bin/env python3
"""Real Bazel9.2 + operator-selected BuildBuddy CAS smoke (not a performance claim).

The selected credential stays in memory or the existing owner-only rc. Only a
host-restricted Bazel credential helper and the service's environment receive it.
Artifacts contain logs/receipts, never credential values or a credential rc copy.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import subprocess
import tempfile
import time
from urllib.parse import urlsplit


def selected(rc, profile):
    headers, endpoints = [], []
    for line in rc.read_text().splitlines():
        words = shlex.split(line, comments=True)
        if not words or words[0] != profile:
            continue
        headers += [w.split("=", 1)[1] for w in words[1:] if w.startswith("--remote_header=")]
        endpoints += [w.split("=", 1)[1] for w in words[1:] if w.startswith("--remote_cache=")]
    if len(headers) != 1 or len(endpoints) != 1:
        raise RuntimeError("select exactly one credential and CAS endpoint in the requested trusted profile")
    name, value = headers[0].split("=", 1)
    if name.lower() != "x-buildbuddy-api-key" or not value or any(c in value for c in "\r\n"):
        raise RuntimeError("unsupported selected credential")
    return endpoints[0], value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bazel", required=True, type=Path)
    parser.add_argument("--service-bin", required=True, type=Path)
    parser.add_argument("--credential-rc", required=True, type=Path)
    parser.add_argument("--credential-profile", default="common:rbe")
    parser.add_argument("--instance-name", default="")
    parser.add_argument("--execute-remotely", action="store_true", help="also force warm-cache genrules onto the selected BuildBuddy RBE backend")
    parser.add_argument("--container-image", default="docker://gcr.io/flame-public/rbe-ubuntu22-04@sha256:0d84a80bb0fc36ba5381942adcf6493249594dcc9044845c617b78c9b621cae3")
    parser.add_argument("--artifacts", required=True, type=Path)
    args = parser.parse_args()
    endpoint, credential = selected(args.credential_rc, args.credential_profile)
    host = urlsplit(endpoint).hostname
    args.artifacts.mkdir(parents=True, exist_ok=True)
    args.artifacts = args.artifacts.resolve()
    os.chmod(args.artifacts, 0o700)
    with tempfile.TemporaryDirectory(prefix="git-output-service-smoke-") as temporary:
        scratch = Path(temporary)
        workspace = scratch / "workspace"
        workspace.mkdir()
        (workspace / "MODULE.bazel").write_text('module(name = "output_service_smoke")\n')
        (workspace / "BUILD.bazel").write_text('genrule(name="output", outs=["message.txt"], cmd="printf git-output-service-smoke > $@")\n')
        helper = scratch / "credential-helper"
        # The helper reselects the one authorized key at the trusted host boundary.
        helper.write_text("#!/usr/bin/env python3\n" +
            "import json,sys,shlex\nfrom pathlib import Path\nfrom urllib.parse import urlsplit\n" +
            f"assert urlsplit(json.load(sys.stdin)['uri']).hostname == {host!r}\n" +
            f"lines=Path({str(args.credential_rc.resolve())!r}).read_text().splitlines()\n" +
            f"words=[shlex.split(line, comments=True) for line in lines]\n" +
            f"headers=[word.split('=',1)[1] for row in words if row and row[0]=={args.credential_profile!r} for word in row[1:] if word.startswith('--remote_header=')]\n" +
            "assert len(headers)==1\nname,value=headers[0].split('=',1)\nassert name.lower()=='x-buildbuddy-api-key'\njson.dump({'headers':{name:[value]}},sys.stdout)\n")
        helper.chmod(0o700)
        rc = scratch / "smoke.bazelrc"
        rc.write_text(f"common --noannounce_rc\ncommon --credential_helper={host}={helper}\n")
        root = scratch / "outputs"
        root.mkdir()
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        service_log = args.artifacts / "service.log"
        env = dict(os.environ, BUILDBUDDY_API_KEY=credential)
        del credential
        with service_log.open("w") as log:
            service = subprocess.Popen([str(args.service_bin.resolve()), "--listen", f"127.0.0.1:{port}", "--root", str(root), "--cas-endpoint", endpoint, "--instance-name", args.instance_name], env=env, stdout=log, stderr=subprocess.STDOUT)
        env.pop("BUILDBUDDY_API_KEY", None)
        receipts = []
        output_bases = []
        def run(phase, base, function, output_service=False, clean=False):
            command = [str(args.bazel.resolve()), "--nosystem_rc", "--nohome_rc", "--noworkspace_rc", f"--bazelrc={rc}", f"--output_base={base}", f"--digest_function={function}", "clean" if clean else "build"]
            command += [f"--remote_cache={endpoint}", f"--remote_instance_name={args.instance_name}", "--remote_timeout=60", "--color=no", "--curses=no", "--jobs=2"]
            if args.execute_remotely and phase.endswith("warm-cache"):
                command += [f"--remote_executor={endpoint}", "--spawn_strategy=remote", "--remote_accept_cached=false", f"--remote_default_exec_properties=container-image={args.container_image}"]
            if output_service:
                command += [f"--experimental_remote_output_service=grpc://127.0.0.1:{port}", f"--experimental_remote_output_service_output_path_prefix={root}"]
            if not clean:
                command += ["--remote_download_outputs=all", "//:output"]
            start = time.monotonic()
            with (args.artifacts / f"{phase}.log").open("w") as log:
                result = subprocess.run(command, cwd=workspace, stdout=log, stderr=subprocess.STDOUT, timeout=180)
            receipt = {"phase": phase, "returncode": result.returncode, "elapsed_seconds": round(time.monotonic()-start, 3)}
            receipts.append(receipt)
            if result.returncode:
                raise RuntimeError(f"{phase} failed; see {args.artifacts / (phase+'.log')}")
            if not clean:
                content = (workspace / "bazel-bin/message.txt").read_bytes()
                assert content == b"git-output-service-smoke", "incorrect materialized output"
                receipt["output_verified"] = True
                if output_service:
                    target = (workspace / "bazel-out").resolve()
                    assert target.is_relative_to(root), "Bazel did not adopt service output root"
                    receipt["service_output_root_verified"] = True
        try:
            for _ in range(100):
                if service.poll() is not None:
                    raise RuntimeError("output service exited before readiness")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.05)
            for function in ["SHA256", "BLAKE3"]:
                warm = scratch / f"warm-{function}"
                projected = scratch / f"service-{function}"
                output_bases += [warm, projected]
                run(f"{function}-warm-cache", warm, function)
                run(f"{function}-service-cache-hit", projected, function, True)
                run(f"{function}-service-incremental", projected, function, True)
                # Local tampering must not be trusted on the next build.
                (workspace / "bazel-bin/message.txt").chmod(0o755)
                (workspace / "bazel-bin/message.txt").write_bytes(b"tampered")
                run(f"{function}-service-repair", projected, function, True)
                run(f"{function}-service-clean", projected, function, True, True)
        finally:
            service.send_signal(signal.SIGINT)
            service.wait(timeout=10)
            for base in output_bases:
                subprocess.run([str(args.bazel.resolve()), "--nosystem_rc", "--nohome_rc", "--noworkspace_rc", f"--output_base={base}", "shutdown"], cwd=workspace, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
            stats = service_log.read_text()
            metrics = {key: int(value) for key, value in re.findall(r"(stage_requests|staged_files|verified_bytes): (\d+)", stats)}
            receipt = {"protocol": "Bazel9.2 BOS v1", "backend": "operator-selected BuildBuddy CAS", "remote_execution_requested": args.execute_remotely, "phases": receipts, "metrics": metrics}
            (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2)+"\n")
        assert metrics.get("staged_files", 0) >= 2, "Bazel did not stage real remote CAS outputs"
        assert all(row["returncode"] == 0 for row in receipts)
        print(json.dumps(receipt, indent=2))

if __name__ == "__main__":
    main()
