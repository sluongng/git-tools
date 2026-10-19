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
import stat
import subprocess
import sys
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
    credentials = parser.add_mutually_exclusive_group(required=True)
    credentials.add_argument("--credential-rc", type=Path)
    credentials.add_argument("--credential-env", help="name of the environment variable holding the selected API key")
    parser.add_argument("--cas-endpoint", help="required with --credential-env")
    parser.add_argument("--credential-profile", default="common:rbe")
    parser.add_argument("--instance-name", default="")
    parser.add_argument("--execute-remotely", action="store_true", help="also force warm-cache genrules onto the selected BuildBuddy RBE backend")
    parser.add_argument("--container-image", default="docker://gcr.io/flame-public/rbe-ubuntu22-04@sha256:0d84a80bb0fc36ba5381942adcf6493249594dcc9044845c617b78c9b621cae3")
    parser.add_argument("--artifacts", required=True, type=Path)
    args = parser.parse_args()
    def selection():
        if args.credential_rc is not None:
            if args.cas_endpoint is not None:
                parser.error("--cas-endpoint is only used with --credential-env")
            return selected(args.credential_rc, args.credential_profile)
        if not args.cas_endpoint or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.credential_env):
            parser.error("--credential-env requires a variable name and --cas-endpoint")
        key = os.environ.get(args.credential_env, "")
        if not key or any(c in key for c in "\r\n"):
            raise RuntimeError("selected credential environment variable is missing or invalid")
        return args.cas_endpoint, key
    endpoint, credential = selection()
    host = urlsplit(endpoint).hostname
    if not host or urlsplit(endpoint).scheme not in ("grpc", "grpcs", "http", "https"):
        parser.error("CAS endpoint must have an http(s)/grpc(s) host")
    windows = os.name == "nt"
    if windows and args.execute_remotely:
        parser.error("Windows smoke validates CAS only; Windows RBE worker selection is not configured")
    executable_name = "smoke.cmd" if windows else "smoke.sh"
    args.artifacts.mkdir(parents=True, exist_ok=True)
    args.artifacts = args.artifacts.resolve()
    os.chmod(args.artifacts, 0o700)
    with tempfile.TemporaryDirectory(prefix="git-output-service-smoke-") as temporary:
        scratch = Path(temporary).resolve()
        workspace = scratch / "workspace"
        workspace.mkdir()
        (workspace / "MODULE.bazel").write_text('module(name = "output_service_smoke")\n')
        if windows:
            (workspace / "BUILD.bazel").write_text(
                'genrule(name="output", outs=["message.txt"], cmd_bat=">$@ <nul set /p=git-output-service-smoke& exit /b 0")\n'
                'genrule(name="executable", outs=["smoke.cmd"], executable=True, '
                'cmd_bat=">$@ echo @echo off& >>$@ echo ^<nul set /p=executable-output-smoke& >>$@ echo exit /b 0")\n')
        else:
            (workspace / "BUILD.bazel").write_text(
                'genrule(name="output", outs=["message.txt"], cmd="printf git-output-service-smoke > $@")\n'
                'genrule(name="executable", outs=["smoke.sh"], executable=True, '
                "cmd=\"echo '#!/bin/sh' > $@; echo 'printf executable-output-smoke' >> $@; chmod +x $@\")\n")
        helper_script = scratch / "credential-helper.py"
        # Re-select only the authorized key, after checking the requested host.
        helper_source = ("import json,os,sys,shlex\nfrom pathlib import Path\nfrom urllib.parse import urlsplit\n" +
            f"assert urlsplit(json.load(sys.stdin)['uri']).hostname == {host!r}\n")
        if args.credential_env:
            helper_source += f"name='x-buildbuddy-api-key'\nvalue=os.environ[{args.credential_env!r}]\n"
        else:
            helper_source += (
                f"lines=Path({str(args.credential_rc.resolve())!r}).read_text().splitlines()\n" +
                "words=[shlex.split(line, comments=True) for line in lines]\n" +
                f"headers=[word.split('=',1)[1] for row in words if row and row[0]=={args.credential_profile!r} for word in row[1:] if word.startswith('--remote_header=')]\n" +
                "assert len(headers)==1\nname,value=headers[0].split('=',1)\nassert name.lower()=='x-buildbuddy-api-key'\n")
        helper_source += "assert value and not any(c in value for c in '\\r\\n')\njson.dump({'headers':{name:[value]}},sys.stdout)\n"
        helper_script.write_text(helper_source)
        helper = scratch / ("credential-helper.cmd" if windows else "credential-helper")
        if windows:
            # No credential is written into this launcher or the generated rc.
            helper.write_text(f'@echo off\n"{sys.executable}" "{helper_script}" %*\n')
        else:
            helper.write_text(f"#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(helper_script))} \"$@\"\n")
        helper.chmod(0o700)
        rc = scratch / "smoke.bazelrc"
        helper_option = f"--credential_helper={host}={helper.as_posix()}"
        rc.write_text(f"common --noannounce_rc\ncommon {json.dumps(helper_option)}\n")
        root = scratch / "outputs"
        root.mkdir()
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        service_log = args.artifacts / "service.log"
        del credential
        service_log.write_text("")
        receipts = []
        restarts = []
        output_bases = []
        service = None
        service_processes = []
        cleanup_errors = []
        primary_error = None
        def cleanup_failure(operation, error):
            # Avoid interpolating command/environment data into diagnostics.
            cleanup_errors.append(f"{operation}: {type(error).__name__}")
        def start_service():
            selected_endpoint, key = selection()
            if selected_endpoint != endpoint:
                raise RuntimeError("selected CAS endpoint changed during smoke")
            env = dict(os.environ, BUILDBUDDY_API_KEY=key)
            del key
            try:
                with service_log.open("a") as log:
                    process = subprocess.Popen([str(args.service_bin.resolve()), "--listen", f"127.0.0.1:{port}", "--root", str(root), "--cas-endpoint", endpoint, "--instance-name", args.instance_name], env=env, stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if windows else 0)
            finally:
                env.pop("BUILDBUDDY_API_KEY", None)
            service_processes.append(process)
            try:
                for _ in range(100):
                    if process.poll() is not None:
                        raise RuntimeError("output service exited before readiness")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                            return process
                    except OSError:
                        time.sleep(0.05)
                raise RuntimeError("output service did not become ready")
            except BaseException:
                try:
                    stop_service(process)
                except Exception as error:
                    cleanup_failure("readiness daemon cleanup", error)
                raise
        def stop_service(process):
            if process is None:
                return
            # Only signal this smoke's owned child, escalating with bounds.
            for action, timeout in [
                (lambda: process.send_signal(signal.CTRL_BREAK_EVENT if windows else signal.SIGINT), 10),
                (process.terminate, 5),
                (process.kill, 5),
            ]:
                if process.poll() is not None:
                    return
                try:
                    action()
                except ProcessLookupError:
                    return
                except OSError:
                    # A Windows non-console supervisor cannot send CTRL_BREAK.
                    # Continue to bounded terminate/kill, rather than leaking it.
                    continue
                try:
                    process.wait(timeout=timeout)
                    return
                except subprocess.TimeoutExpired:
                    continue
            raise RuntimeError("owned output-service process did not exit after kill")
        def run(phase, base, function, output_service=False, clean=False):
            clean_root = (workspace / "bazel-out").resolve() if clean and output_service else None
            if clean_root is not None:
                assert clean_root.is_relative_to(root) and clean_root != root and clean_root.is_dir(), "clean must target the adopted service output tree"
            command = [str(args.bazel.resolve()), "--nosystem_rc", "--nohome_rc", "--noworkspace_rc", f"--bazelrc={rc}", f"--output_base={base}", f"--digest_function={function}", "clean" if clean else "build"]
            command += [f"--remote_cache={endpoint}", f"--remote_instance_name={args.instance_name}", "--remote_timeout=60", "--color=no", "--curses=no", "--jobs=2"]
            if args.execute_remotely and phase.endswith("warm-cache"):
                command += [f"--remote_executor={endpoint}", "--spawn_strategy=remote", "--remote_accept_cached=false", f"--remote_default_exec_properties=container-image={args.container_image}"]
            if output_service:
                command += [f"--experimental_remote_output_service=grpc://127.0.0.1:{port}", f"--experimental_remote_output_service_output_path_prefix={root}"]
            if not clean:
                command += ["--remote_download_outputs=all", "//:output", "//:executable"]
            start = time.monotonic()
            with (args.artifacts / f"{phase}.log").open("w") as log:
                result = subprocess.run(command, cwd=workspace, stdout=log, stderr=subprocess.STDOUT, timeout=180)
            receipt = {"phase": phase, "returncode": result.returncode, "elapsed_seconds": round(time.monotonic()-start, 3)}
            receipts.append(receipt)
            if result.returncode:
                raise RuntimeError(f"{phase} failed; see {args.artifacts / (phase+'.log')}")
            if clean_root is not None:
                assert not clean_root.exists(), "native bazel clean did not remove the adopted service output tree"
                receipt["service_output_root_removed"] = True
            if not clean:
                content = (workspace / "bazel-bin/message.txt").read_bytes()
                assert content == b"git-output-service-smoke", "incorrect materialized output"
                receipt["output_verified"] = True
                executable = workspace / "bazel-bin" / executable_name
                if not windows:
                    assert executable.stat().st_mode & stat.S_IXUSR, "staged executable lost execute mode"
                executable_command = [os.environ.get("COMSPEC", "cmd.exe"), "/d", "/c", str(executable)] if windows else [str(executable)]
                execution = subprocess.run(executable_command, capture_output=True, timeout=10)
                assert execution.returncode == 0 and execution.stdout == b"executable-output-smoke", "staged executable did not run correctly"
                receipt["executable_mode_verified"] = not windows
                receipt["executable_execution_verified"] = True
                if output_service:
                    target = (workspace / "bazel-out").resolve()
                    assert target.is_relative_to(root), "Bazel did not adopt service output root"
                    receipt["service_output_root_verified"] = True
        try:
            service = start_service()
            for function in ["SHA256", "BLAKE3"]:
                warm = scratch / f"warm-{function}"
                projected = scratch / f"service-{function}"
                output_bases += [warm, projected]
                run(f"{function}-warm-cache", warm, function)
                run(f"{function}-service-cache-hit", projected, function, True)
                run(f"{function}-service-incremental", projected, function, True)
                # Restart the actual daemon, not merely a new build session.
                # Reuse root, listen address, CAS configuration and Bazel base.
                projected_root = (workspace / "bazel-out").resolve()
                outputs = [workspace / "bazel-bin/message.txt", workspace / "bazel-bin" / executable_name]
                checkpoint = [path.read_bytes() for path in outputs]
                old_pid = service.pid
                stop_service(service)
                assert [path.read_bytes() for path in outputs] == checkpoint, "daemon shutdown removed outputs"
                service = start_service()
                assert service.pid != old_pid, "smoke did not create a new daemon process"
                assert [path.read_bytes() for path in outputs] == checkpoint, "restart clobbered existing output bytes"
                run(f"{function}-service-after-restart", projected, function, True)
                assert (workspace / "bazel-out").resolve() == projected_root, "restart changed the output tree"
                restarts.append({"digest_function": function, "new_process_verified": True,
                    "same_root_and_endpoint": True, "existing_bytes_preserved": True,
                    "next_bazel_build_verified": True})
                # Local tampering must not be trusted on the next build.
                (workspace / "bazel-bin/message.txt").chmod(0o755)
                (workspace / "bazel-bin/message.txt").write_bytes(b"tampered")
                run(f"{function}-service-repair", projected, function, True)
                run(f"{function}-service-clean", projected, function, True, True)
        except BaseException as error:
            primary_error = error
            raise
        finally:
            # Complete every cleanup/receipt step even when another one fails;
            # never replace the original build/assertion/readiness exception.
            for process in service_processes:
                try:
                    stop_service(process)
                except Exception as error:
                    cleanup_failure("daemon cleanup", error)
            for base in output_bases:
                try:
                    result = subprocess.run([str(args.bazel.resolve()), "--nosystem_rc", "--nohome_rc", "--noworkspace_rc", f"--output_base={base}", "shutdown"], cwd=workspace, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
                    if result.returncode:
                        cleanup_errors.append(f"Bazel shutdown returned {result.returncode}")
                except Exception as error:
                    cleanup_failure("Bazel shutdown", error)
            metrics = {}
            try:
                stats = service_log.read_text()
                for key, value in re.findall(r"(stage_requests|staged_files|verified_bytes): (\d+)", stats):
                    metrics[key] = metrics.get(key, 0) + int(value)
            except Exception as error:
                cleanup_failure("service metrics", error)
            receipt = {"protocol": "Bazel9.2 BOS v1", "backend": "operator-selected BuildBuddy CAS", "remote_execution_requested": args.execute_remotely, "phases": receipts, "service_restarts": restarts, "metrics": metrics, "cleanup_errors": cleanup_errors}
            try:
                (args.artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2)+"\n")
            except Exception as error:
                cleanup_failure("receipt writing", error)
            if cleanup_errors:
                print("Smoke cleanup errors: " + "; ".join(cleanup_errors), file=sys.stderr)
                if primary_error is None:
                    raise RuntimeError("smoke cleanup failed; see diagnostics and receipt")
        assert metrics.get("staged_files", 0) >= 2, "Bazel did not stage real remote CAS outputs"
        assert len(restarts) == 2, "both digest functions must exercise an actual service restart"
        assert all(row["returncode"] == 0 for row in receipts)
        print(json.dumps(receipt, indent=2))

if __name__ == "__main__":
    main()
