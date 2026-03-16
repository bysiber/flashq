"""Tests for the CLI interface."""

from __future__ import annotations

import pytest

from flashq.cli import _import_app, build_parser


class TestCLI:
    def test_parser_worker_command(self):
        parser = build_parser()
        args = parser.parse_args(["worker", "myapp:app"])
        assert args.command == "worker"
        assert args.app == "myapp:app"
        assert args.concurrency == 4
        assert args.queues is None

    def test_parser_worker_with_options(self):
        parser = build_parser()
        args = parser.parse_args([
            "worker", "myapp:app",
            "-q", "default,emails",
            "-c", "8",
            "--poll-interval", "0.5",
            "-n", "my-worker",
            "-l", "debug",
        ])
        assert args.queues == "default,emails"
        assert args.concurrency == 8
        assert args.poll_interval == 0.5
        assert args.name == "my-worker"
        assert args.log_level == "debug"

    def test_parser_info_command(self):
        parser = build_parser()
        args = parser.parse_args(["info", "myapp:app"])
        assert args.command == "info"

    def test_parser_purge_command(self):
        parser = build_parser()
        args = parser.parse_args(["purge", "myapp:app", "-f"])
        assert args.command == "purge"
        assert args.force is True

    def test_parser_no_command(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None

    def test_import_app_bad_path(self):
        with pytest.raises(SystemExit):
            _import_app("badpath")

    def test_import_app_nonexistent_module(self):
        with pytest.raises(SystemExit):
            _import_app("nonexistent_module_xyz:app")
