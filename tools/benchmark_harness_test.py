"""Network-free public harness regressions; no real Bazel process is launched."""
import argparse
from contextlib import nullcontext, redirect_stdout, redirect_stderr
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import benchmark_agent_rbe as pilot
import benchmark_shared_daemon as shared


class SharedDaemonTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.args = argparse.Namespace(plan=self.root / 'plan.json', output=self.root / 'artifacts',
            endpoint='grpc://cache.example:1234', label='fixture', cache_state='prepared',
            run=False, noop=False, status_command_json=None, timeout=5)
        self.command = ['bazel', '--output_base={output_base}', 'build', '//:fixture',
                        '--remote_cache={endpoint}']

    def run_plan(self, command, **extra):
        plan = dict(command=command, cwd=str(self.root), source_revision='fixture', **extra)
        self.args.plan.write_text(json.dumps(plan))
        with redirect_stdout(io.StringIO()) as output:
            shared.run(self.args)
        return json.loads(output.getvalue())

    def test_unsafe_plans_rejected_before_execution_or_artifacts(self):
        cases = {
            'missing owned output base': ['bazel', 'build', '//:fixture', '--remote_cache={endpoint}'],
            'overridden owned output base': self.command[:2] + ['--output_base=/unowned'] + self.command[2:],
            'late output base': self.command + ['--output_base=/unowned'],
            'unused endpoint': self.command[:-1],
            'mismatched endpoint': self.command[:-1] + ['--remote_cache=grpc://other.example'],
            'overridden endpoint': self.command + ['--remote_cache=grpc://other.example'],
            'mismatched executor': self.command + ['--remote_executor=grpc://other.example'],
            'shell command prefix': ['sh', '-c', 'bazel build //:fixture'],
            'unsupported startup option': ['bazel', '--unknown-startup'] + self.command[1:],
            'batch has no fresh server': ['bazel', '--batch'] + self.command[1:],
            'endpoint consumed as test argument': self.command[:2] + ['test', '--test_arg', '--remote_cache={endpoint}'],
            'missing test argument cannot consume moved endpoint': self.command + ['--test_arg'],
            'ambiguous unknown arity': self.command + ['--unknown-option'],
            'short option can consume endpoint': self.command[:2] + ['test', '-c', '--remote_cache={endpoint}'],
            'endpoint after real separator with bool-or-enum': self.command[:2] + ['build',
                '--subcommands', '--', '//:fixture', '--remote_cache={endpoint}'],
        }
        with patch.object(shared.subprocess, 'run') as execute:
            for name, command in cases.items():
                with self.subTest(name=name), self.assertRaises(ValueError):
                    self.run_plan(command)
                self.assertFalse(self.args.output.exists())
            self.args.endpoint = ''
            with self.assertRaisesRegex(ValueError, 'nonempty remote cache endpoint'):
                self.run_plan(self.command)
            execute.assert_not_called()

    def test_hidden_unowned_shutdown_rejected(self):
        with patch.object(shared.subprocess, 'run') as execute, self.assertRaises(ValueError):
            self.run_plan(self.command, shutdown_command=['bazel', 'shutdown'])
        execute.assert_not_called()
        self.assertFalse(self.args.output.exists())

    def test_split_option_values_are_not_mistaken_for_options_or_separator(self):
        command = self.command[:2] + ['test', '--test_arg', '--', '--test_arg',
            '--remote_cache=argument-not-a-connection', '--remote_cache={endpoint}',
            '--noshow_progress', '//:fixture']
        report = self.run_plan(command)
        self.assertEqual(report['command'][3:7], ['--test_arg', '--', '--test_arg',
            '--remote_cache=argument-not-a-connection'])
        self.assertEqual(report['command'][-1], '--remote_cache=' + self.args.endpoint)
        # This literal -- belongs to test_arg, not a target separator. Moving
        # remote_cache before it would turn the endpoint into the test argument.
        command = self.command[:2] + ['test', '--remote_cache={endpoint}',
            '--test_arg', '--', '//:fixture']
        report = self.run_plan(command)
        self.assertEqual(report['command'][2:], ['test', '--test_arg', '--',
            '//:fixture', '--remote_cache=' + self.args.endpoint])
        report = self.run_plan(self.command + ['--test_arg', '', '--remote_executor', ''])
        self.assertEqual(report['command'][-4:], ['--test_arg', '',
            '--remote_cache=' + self.args.endpoint, '--remote_executor='])

    def test_bool_or_enum_option_does_not_consume_endpoint(self):
        for option in ('--subcommands', '--subcommands=pretty_print', '--nosubcommands'):
            with self.subTest(option=option):
                report = self.run_plan(self.command[:2] + ['build', option,
                    '--remote_cache={endpoint}', '//:fixture'])
                self.assertEqual(report['command'][2:], ['build', option, '//:fixture',
                    '--remote_cache=' + self.args.endpoint])

    def test_supported_wrapper_rc_and_config_preserve_owned_cleanup(self):
        command = ['env', 'FIXTURE=1', '/fixture/bazel-wrapper', '--bazelrc=/fixture/private.rc',
                   '--output_base', '{output_base}', '--output_user_root={output_user_root}',
                   'test', '--remote_cache', '{endpoint}', '--config=cache-only', '--', '//:fixture']
        report = self.run_plan(command)
        self.assertEqual(report['command'][-3:], ['--remote_cache=' + self.args.endpoint, '--', '//:fixture'])
        expected = ['env', 'FIXTURE=1', '/fixture/bazel-wrapper', '--bazelrc=/fixture/private.rc',
                    '--output_base', str(self.args.output / 'output-base'),
                    '--output_user_root=' + str(self.args.output / 'output-user-root'), 'shutdown']
        self.assertEqual(report['shutdown_command'], expected)
        self.args.run = True
        with patch.object(shared.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as execute:
            self.run_plan(command)
        self.assertEqual(execute.call_count, 2)
        self.assertEqual(execute.call_args_list[1].args[0], expected)


class PilotExecutableTest(unittest.TestCase):
    def test_path_and_absolute_selection_are_hashed_and_invoked_identically(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / 'fixture-bazel'
            executable.write_bytes(b'fixture executable, never launched\n')
            executable.chmod(0o700)
            config = root / 'config.json'
            for selection in ('fixture-bazel', str(executable)):
                with self.subTest(selection=selection):
                    workspace = root / ('workspace-' + str(len(selection)))
                    workspace.mkdir()
                    output = root / ('output-' + str(len(selection)))
                    config.write_text(json.dumps(dict(bazel=selection, git=str(executable), commit='fixture',
                        build_output_paths=['built'], repository_cache=str(root / 'repository-cache'))))
                    argv = ['pilot', '--config', str(config), '--workspace', str(workspace),
                            '--output', str(output), '--credential-rc', str(root / 'unused.rc'), '--run']

                    def execute(command, **kwargs):
                        if 'build' in command:
                            (workspace / 'built').write_bytes(b'output')
                            (output / 'build.execution.json').write_text(json.dumps([
                                {'runner': 'remote', 'mnemonic': 'CppCompile'}]))
                        return subprocess.CompletedProcess(command, 0)

                    with patch.dict(os.environ, {'PATH': str(root)}), patch.object(sys, 'argv', argv), \
                            patch.object(pilot.subprocess, 'check_output', return_value='fixture\n'), \
                            patch.object(pilot.subprocess, 'run', side_effect=execute) as invoked, \
                            patch.object(pilot, 'credential_helper', return_value=nullcontext(root / 'helper')), \
                            redirect_stdout(io.StringIO()):
                        pilot.main()
                    self.assertEqual(invoked.call_count, 2)
                    for invocation in invoked.call_args_list:
                        self.assertEqual(invocation.args[0][0], str(executable))
                    result = json.loads((output / 'results.json').read_text())
                    self.assertEqual(result['provenance']['bazel_sha256'],
                                     hashlib.sha256(executable.read_bytes()).hexdigest())

    def test_missing_executable_is_clear_and_has_no_side_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / 'config.json'
            config.write_text(json.dumps({'bazel': 'missing-fixture-bazel'}))
            output = root / 'output'
            argv = ['pilot', '--config', str(config), '--workspace', str(root), '--output', str(output),
                    '--credential-rc', str(root / 'unused.rc'), '--run']
            with patch.dict(os.environ, {'PATH': str(root)}), patch.object(sys, 'argv', argv), \
                    patch.object(pilot.subprocess, 'run') as execute, redirect_stderr(io.StringIO()) as error:
                with self.assertRaises(SystemExit) as stopped:
                    pilot.main()
            self.assertEqual(stopped.exception.code, 2)
            self.assertIn('Bazel executable not found or not executable', error.getvalue())
            self.assertFalse(output.exists())
            execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
