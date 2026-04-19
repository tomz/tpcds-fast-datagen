# Live integration tests

These tests validate the Spark API against real platforms. Not part of the regular `pytest` suite — they require live cloud credentials.

The bootstrap script `live_integration_test.py` is submitted via the matching `*-spark-submit` helper (see `~/.local/bin/`). Each helper reads its `~/.<platform>-spark.json` profile and handles auth + artifact upload.

Run the test cycle from the matrix at `run_matrix.sh`. Logs are saved to `/tmp/tpcds-live-tests/` and parsed by `verify_results.py`.
