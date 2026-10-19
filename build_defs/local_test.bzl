"""Run native host fixtures locally without restricting Rust compilation."""

def _local_test_impl(ctx):
    binary = ctx.executable.binary
    launcher = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(
        launcher,
        "#!/bin/sh\nexport TMPDIR=\"${TEST_TMPDIR}\"\nexec \"${TEST_SRCDIR}/${TEST_WORKSPACE}/%s\" \"$@\"\n" % binary.short_path,
        is_executable = True,
    )
    runfiles = ctx.runfiles(files = [binary]).merge(ctx.attr.binary[DefaultInfo].default_runfiles)
    return [
        DefaultInfo(executable = launcher, runfiles = runfiles),
        testing.ExecutionInfo({"local": "1"}),
        testing.TestEnvironment(environment = {}, inherited_environment = ["PATH"]),
    ]

local_test = rule(
    implementation = _local_test_impl,
    attrs = {"binary": attr.label(executable = True, cfg = "target", mandatory = True)},
    test = True,
)
