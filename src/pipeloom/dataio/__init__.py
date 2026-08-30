"""Sources and sinks. sources.py and sinks.py (IterableSource, JSONLinesSink,
CSVSink, etc.) are dependency-free; parquet.py, kafka.py, and db.py each add
one optional Sink behind their own extra (`parquet`, `kafka`, `db`) and are
never imported automatically -- import the specific module you need.
"""
