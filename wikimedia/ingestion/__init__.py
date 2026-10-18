"""Ingestion code for the Wikimedia lambda architecture.

Importing a submodule loads `.env` and nothing else; no module here makes a
network call or opens a warehouse connection at import time.
"""
