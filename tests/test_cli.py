"""CLI smoke tests using click.testing.CliRunner."""
from click.testing import CliRunner

from tpcds_fast_datagen.cli import main


def test_help_exits_cleanly():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "scale" in result.output.lower()
    assert "engine" in result.output.lower()


def test_engine_choice_validation_rejects_invalid():
    runner = CliRunner()
    result = runner.invoke(main, ["--engine", "spark", "--scale", "1"])
    # click rejects bad choice with exit code 2
    assert result.exit_code != 0
    assert "spark" in result.output.lower() or "invalid" in result.output.lower()


def test_engine_auto_is_default_in_help():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    # --engine option shows default
    assert "auto" in result.output.lower()
