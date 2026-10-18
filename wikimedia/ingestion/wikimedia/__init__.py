"""Wikimedia EventStreams ingestion: the speed layer of the lambda architecture.

`consume_recentchange` tails the `recentchange` firehose and appends every event
to `workspace.wikimedia.raw_wikimedia`.
"""
