"""Remote-capable Rust fixtures with declared subprocess tools.

Pure Rust suites should use rust_test directly. This launcher supplies per-test
scratch and the Git/Python executables needed by subprocess integration fixtures;
it does not force a local execution strategy or inherit the developer's PATH.
"""

def _runfile(file):
    return "${TEST_SRCDIR}/${TEST_WORKSPACE}/" + file.short_path

def _fixture_test_impl(ctx):
    binary = ctx.executable.binary
    files = [binary]
    runfiles = ctx.runfiles().merge(ctx.attr.binary[DefaultInfo].default_runfiles)
    tool_scripts = []
    if ctx.attr.git_tool:
        git = ctx.executable.git_tool
        http = ctx.executable.git_http
        https = ctx.executable.git_https
        backend = ctx.executable.git_http_backend
        files.extend([git, http, https, backend])
        for target in [ctx.attr.git_tool, ctx.attr.git_http, ctx.attr.git_https, ctx.attr.git_http_backend]:
            runfiles = runfiles.merge(target[DefaultInfo].default_runfiles)
        git_launcher = ctx.actions.declare_file(ctx.label.name + ".tools/git")
        ctx.actions.write(
            git_launcher,
            '#!/bin/sh\nexec "%s" --exec-path="%s" "$@"\n' % (
                _runfile(git),
                _runfile(http).rsplit("/", 1)[0],
            ),
            is_executable = True,
        )
        tool_scripts.append(git_launcher)

        # Local/file transports execute these through a shell. Do not let them
        # fall through to an older Git installed in the execution image.
        for command in ["upload-pack", "receive-pack", "upload-archive"]:
            helper = ctx.actions.declare_file(ctx.label.name + ".tools/git-" + command)
            ctx.actions.write(
                helper,
                '#!/bin/sh\nexec "%s" --exec-path="%s" %s "$@"\n' % (
                    _runfile(git),
                    _runfile(http).rsplit("/", 1)[0],
                    command,
                ),
                is_executable = True,
            )
            tool_scripts.append(helper)
    if ctx.attr.python:
        runtime = ctx.toolchains["@rules_python//python:toolchain_type"].py3_runtime
        if not runtime.interpreter:
            fail("fixture_test requires a hermetic Python interpreter")
        files.append(runtime.interpreter)
        runfiles = runfiles.merge(ctx.runfiles(transitive_files = runtime.files))
        python_launcher = ctx.actions.declare_file(ctx.label.name + ".tools/python3")
        ctx.actions.write(
            python_launcher,
            '#!/bin/sh\nexec "%s" "$@"\n' % _runfile(runtime.interpreter),
            is_executable = True,
        )
        tool_scripts.append(python_launcher)
    launcher = ctx.actions.declare_file(ctx.label.name + ".sh")
    script = '#!/bin/sh\nset -eu\nexport TMPDIR="${TEST_TMPDIR}"\n'

    # POSIX utilities are supplied by the pinned execution image, not the host.
    script += 'export PATH="/usr/bin:/bin"\n'
    if tool_scripts:
        tool_dir = _runfile(tool_scripts[0]).rsplit("/", 1)[0]
        script += 'export PATH="%s:${PATH}"\n' % tool_dir
    if ctx.attr.git_tool:
        script += 'export GIT_PROXY_TEST_GIT="%s/git"\n' % tool_dir
    if ctx.file.script:
        if not ctx.attr.binary_env:
            fail("script fixtures require binary_env")
        files.append(ctx.file.script)

        # Native mount utilities live in sbin on macOS; do not inherit host PATH.
        script += 'export PATH="${PATH}:/usr/sbin:/sbin"\n'
        script += 'export %s="%s"\n' % (ctx.attr.binary_env, _runfile(binary))
        script += 'exec /bin/sh "%s" "$@"\n' % _runfile(ctx.file.script)
    else:
        script += 'exec "%s" "$@"\n' % _runfile(binary)
    ctx.actions.write(launcher, script, is_executable = True)
    return [DefaultInfo(
        executable = launcher,
        runfiles = runfiles.merge(ctx.runfiles(files = files + tool_scripts)),
    )]

_fixture_test = rule(
    implementation = _fixture_test_impl,
    attrs = {
        "binary": attr.label(executable = True, cfg = "target", mandatory = True),
        "script": attr.label(allow_single_file = True),
        "binary_env": attr.string(),
        "git_tool": attr.label(executable = True, cfg = "target"),
        "git_http": attr.label(executable = True, cfg = "target"),
        "git_https": attr.label(executable = True, cfg = "target"),
        "git_http_backend": attr.label(executable = True, cfg = "target"),
        "python": attr.bool(),
    },
    toolchains = ["@rules_python//python:toolchain_type"],
    test = True,
)

def fixture_test(name, binary, git = False, python = False, **kwargs):
    """Wrap a Rust fixture with only its explicitly requested runtime tools."""
    _fixture_test(
        name = name,
        binary = binary,
        git_tool = "@git//:git" if git else None,
        git_http = "@git//:git-remote-http" if git else None,
        git_https = "@git//:git_remote_https" if git else None,
        git_http_backend = "@git//:git-http-backend" if git else None,
        python = python,
        **kwargs
    )
