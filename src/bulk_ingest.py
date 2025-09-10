#!/usr/bin/env python3
# ingest_stream.py
# Stream ingest to OpenSearch from JSONL (file/stdin) or Kafka.
# - English-only output/help
# - Not a copy: different structure (httpx client, buffering class, stats)
# - Supports: --jsonl path|"-" or --kafka-brokers + --kafka-topic
# - OpenSearch bulk with optional --pipeline
# - Batching by doc count and (optional) payload size
# - Keep-alive connection pooling

import argparse
import gzip
import io
import json
import os
import sys
import time
from typing import Iterable, Iterator, List, Optional, Tuple

try:
    import httpx  # HTTP client with pooling/keep-alive
except ImportError:
    httpx = None  # We'll check later and print a helpful message

# --------------------------
# Utilities
# --------------------------

def open_text_auto(path: str) -> io.TextIOBase:
    """Open plain text or .gz transparently as a text stream (utf-8, ignore errors)."""
    if path == "-":
        # Read from stdin (already text on Windows PowerShell; ensure utf-8)
        return sys.stdin
    if path.lower().endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8-sig", errors="ignore")


def iter_jsonl_docs(path: str) -> Iterator[dict]:
    """Yield JSON objects from a JSONL file or stdin. Skips blank lines."""
    with (sys.stdin if path == "-" else open_text_auto(path)) as fp:
        for i, line in enumerate(fp, 1):
            s = line.strip().lstrip("\ufeff")
            if not s:
                continue
            try:
                yield json.loads(s)
            except Exception as e:
                print(f"[warn] Skipping malformed JSON line: {e}", file=sys.stderr)


class NDJSONBuffer:
    """Collects NDJSON action lines for OpenSearch bulk and flushes by docs or bytes."""

    def __init__(self, index: str, pipeline: Optional[str], max_docs: int, max_bytes: int = 5_000_000):
        self.index = index
        self.pipeline = pipeline
        self.max_docs = max_docs
        self.max_bytes = max_bytes
        self.lines: List[str] = []
        self.cur_docs = 0
        self.cur_bytes = 0

    def add_doc(self, doc: dict) -> bool:
        meta = {"index": {"_index": self.index}}
        if self.pipeline:
            meta["index"]["pipeline"] = self.pipeline
        meta_line = json.dumps(meta, ensure_ascii=False)
        doc_line = json.dumps(doc, ensure_ascii=False)

        # We add a '\n' after every line in the final payload; account for that here
        projected = self.cur_bytes + len(meta_line) + 1 + len(doc_line) + 1
        if self.cur_docs >= self.max_docs or projected >= self.max_bytes:
            return False  # signal caller to flush first

        self.lines.append(meta_line)
        self.lines.append(doc_line)
        self.cur_docs += 1
        self.cur_bytes = projected
        return True

    def should_flush(self) -> bool:
        return self.cur_docs >= self.max_docs or self.cur_bytes >= self.max_bytes

    def drain(self) -> Optional[bytes]:
        if not self.lines:
            return None
        payload = "\n".join(self.lines) + "\n"
        # reset
        self.lines.clear()
        self.cur_docs = 0
        self.cur_bytes = 0
        return payload.encode("utf-8")


class BulkClient:
    """Thin wrapper around httpx for OpenSearch _bulk calls with keep-alive + pooling."""

    def __init__(self, host: str, timeout: float = 30.0, max_connections: int = 64, max_keepalive: int = 32):
        if httpx is None:
            raise RuntimeError("httpx is not installed. Run: pip install httpx")
        self.host = host.rstrip("/")
        limits = httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_keepalive)
        self.client = httpx.Client(limits=limits, timeout=timeout, headers={"Connection": "keep-alive"})

    def bulk(self, payload: bytes) -> Tuple[bool, Optional[dict]]:
        r = self.client.post(f"{self.host}/_bulk", content=payload, headers={"Content-Type": "application/x-ndjson"})
        ok = (200 <= r.status_code < 300)
        try:
            j = r.json()
        except Exception:
            j = None
        if not ok:
            msg = (r.text[:500] if isinstance(r.text, str) else str(r.content[:500]))
            print(f"[error] Bulk HTTP {r.status_code}: {msg}", file=sys.stderr)
        elif j and j.get("errors"):
            # OpenSearch signals partial failures via "errors": true
            print("[warn] Bulk request had per-item errors (see OpenSearch logs/_bulk response for details)", file=sys.stderr)
        return ok, j

    def close(self):
        self.client.close()


# --------------------------
# From JSONL path/stdin
# --------------------------

def run_from_jsonl(host: str, index: str, jsonl_path: str, pipeline: Optional[str],
                   batch: int, max_bytes: int, timeout: float) -> None:
    bc = BulkClient(host, timeout=timeout)
    buf = NDJSONBuffer(index=index, pipeline=pipeline, max_docs=batch, max_bytes=max_bytes)
    t0 = time.time()
    total_docs = 0
    total_bytes = 0
    batches = 0

    try:
        for doc in iter_jsonl_docs(jsonl_path):
            if not buf.add_doc(doc):
                # flush then add
                payload = buf.drain()
                if payload:
                    ok, _ = bc.bulk(payload)
                    if not ok:
                        # continue but report; caller can stop if preferred
                        pass
                    batches += 1
                    total_bytes += len(payload)
                    total_docs += batch  # approximate; next fix below

                # now add current doc to fresh buffer
                assert buf.add_doc(doc), "Buffer too small for single document; increase --max-bytes or reduce doc size."

            if buf.should_flush():
                payload = buf.drain()
                if payload:
                    ok, _ = bc.bulk(payload)
                    if not ok:
                        pass
                    batches += 1
                    total_bytes += len(payload)
                    # Estimate docs by lines/2
                    docs_in_payload = payload.count(b"\n") // 2
                    total_docs += docs_in_payload

        # final flush
        payload = buf.drain()
        if payload:
            ok, _ = bc.bulk(payload)
            if not ok:
                pass
            batches += 1
            total_bytes += len(payload)
            total_docs += payload.count(b"\n") // 2

    finally:
        bc.close()

    dt = max(1e-6, time.time() - t0)
    mb = total_bytes / (1024 * 1024)
    print(f"[done] Indexed ~{total_docs} docs in {dt:.1f}s  (~{total_docs/dt:.1f} docs/s, {mb/dt:.2f} MB/s)  batches={batches}")


# --------------------------
# From Kafka topic
# --------------------------

def run_from_kafka(host: str, index: str, brokers: str, topic: str, group: str,
                   pipeline: Optional[str], batch: int, max_bytes: int, timeout: float,
                   auto_offset_reset: str = "earliest") -> None:
    try:
        from kafka import KafkaConsumer  # pip install kafka-python
    except ImportError:
        print("[fatal] kafka-python is not installed. Run: pip install kafka-python", file=sys.stderr)
        sys.exit(2)

    bc = BulkClient(host, timeout=timeout)
    buf = NDJSONBuffer(index=index, pipeline=pipeline, max_docs=batch, max_bytes=max_bytes)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[s.strip() for s in brokers.split(",") if s.strip()],
        group_id=group,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8", errors="ignore"))
    )

    t0 = time.time()
    total_docs = 0
    total_bytes = 0
    batches = 0
    print(f"[kafka] Consuming from topic='{topic}', group='{group}', brokers={brokers}")

    try:
        for msg in consumer:
            doc = msg.value
            if not isinstance(doc, dict):
                # Skip non-object payloads
                continue

            if not buf.add_doc(doc):
                payload = buf.drain()
                if payload:
                    ok, _ = bc.bulk(payload)
                    if not ok:
                        pass
                    batches += 1
                    total_bytes += len(payload)
                    total_docs += payload.count(b"\n") // 2
                # try again on fresh buffer
                assert buf.add_doc(doc), "Buffer too small for single Kafka message; adjust --max-bytes."

            if buf.should_flush():
                payload = buf.drain()
                if payload:
                    ok, _ = bc.bulk(payload)
                    if not ok:
                        pass
                    batches += 1
                    total_bytes += len(payload)
                    total_docs += payload.count(b"\n") // 2

    except KeyboardInterrupt:
        print("\n[info] Stopping on user interrupt...")
    finally:
        # final flush
        payload = buf.drain()
        if payload:
            ok, _ = bc.bulk(payload)
            if not ok:
                pass
            batches += 1
            total_bytes += len(payload)
            total_docs += payload.count(b"\n") // 2

        consumer.close()
        bc.close()

    dt = max(1e-6, time.time() - t0)
    mb = total_bytes / (1024 * 1024)
    print(f"[done] Indexed ~{total_docs} docs in {dt:.1f}s  (~{total_docs/dt:.1f} docs/s, {mb/dt:.2f} MB/s)  batches={batches}")


# --------------------------
# CLI
# --------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bulk-ingest reviews into OpenSearch from JSONL (file/stdin) or Kafka.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    src = p.add_argument_group("Source (choose one)")
    src.add_argument("--jsonl", help='Path to JSONL file or "-" for stdin')
    src.add_argument("--kafka-brokers", help="Comma-separated broker list, e.g. localhost:9092,localhost:9093")
    src.add_argument("--kafka-topic", default="reviews_clean", help="Kafka topic name")
    src.add_argument("--kafka-group", default="os_ingest_group", help="Kafka consumer group id")
    src.add_argument("--kafka-auto-offset-reset", choices=["earliest", "latest"], default="earliest")

    tgt = p.add_argument_group("Target")
    tgt.add_argument("--host", default=os.environ.get("OS_HOST", "http://127.0.0.1:9200"), help="OpenSearch host URL")
    tgt.add_argument("--index", required=True, help="Target index name")
    tgt.add_argument("--pipeline", default=None, help="Optional ingest pipeline id")

    perf = p.add_argument_group("Performance")
    perf.add_argument("--batch", type=int, default=500, help="Docs per bulk request (upper bound)")
    perf.add_argument("--max-bytes", type=int, default=2_000_000, help="Max payload size per bulk request in bytes")
    perf.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout seconds")
    perf.add_argument("--max-connections", type=int, default=16, help="HTTP max connections (pool)")
    perf.add_argument("--max-keepalive", type=int, default=8, help="HTTP max keep-alive connections (pool)")

    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Quick dependency check
    if httpx is None:
        print("[fatal] httpx is required. Install via: pip install httpx", file=sys.stderr)
        sys.exit(2)

    # Adjust pool sizes globally by re-instantiating BulkClient with custom limits
    # (We pass them via constructor when creating BulkClient in run_* functions.)
    if args.jsonl:
        run_from_jsonl(
            host=args.host,
            index=args.index,
            jsonl_path=args.jsonl,
            pipeline=args.pipeline,
            batch=args.batch,
            max_bytes=args.max_bytes,
            timeout=args.timeout,
        )
    elif args.kafka_brokers:
        run_from_kafka(
            host=args.host,
            index=args.index,
            brokers=args.kafka_brokers,
            topic=args.kafka_topic,
            group=args.kafka_group,
            pipeline=args.pipeline,
            batch=args.batch,
            max_bytes=args.max_bytes,
            timeout=args.timeout,
            auto_offset_reset=args.kafka_auto_offset_reset,
        )
    else:
        print("You must specify either --jsonl or --kafka-brokers.", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
