#!/usr/bin/env python3
# bench_pro.py
# Async OpenSearch benchmark: httpx + keep-alive + connection pooling
# Reports throughput (QPS), success/error counts, and latency percentiles.

import argparse
import asyncio
import json
import os
import statistics
import time
from typing import Any, Dict, List, Optional

import httpx

DEFAULT_QUERY = {
    "size": 0,
    "track_total_hits": 10000,
    "query": {
        "bool": {
            "must": [
                {"match": {"text": "lyrics"}},
                {"match": {"text": "vocals"}}
            ],
            "filter": [{"range": {"score": {"gt": 4}}}]
        }
    }
}

def load_query(query_file: Optional[str]) -> Dict[str, Any]:
    if query_file:
        with open(query_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return DEFAULT_QUERY

def percentile(vals: List[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    k = int((len(s) - 1) * (p / 100.0))
    return s[k]

async def worker(client: httpx.AsyncClient, url: str, body: Dict[str, Any],
                 stop_ts: float, lat_ms: List[float],
                 ok_counter: List[int], err_counter: List[int]) -> None:
    while time.time() < stop_ts:
        t0 = time.perf_counter()
        try:
            r = await client.post(url, json=body)
            _ = r.text  # drain to free connection for reuse
            if r.status_code == 200:
                ok_counter[0] += 1
            else:
                err_counter[0] += 1
        except Exception:
            err_counter[0] += 1
        finally:
            t1 = time.perf_counter()
            lat_ms.append((t1 - t0) * 1000.0)

async def run_once(host: str, index: str, body: Dict[str, Any], duration: int, conc: int,
                   timeout: float, max_conns: int, max_keepalive: int, warmup: int) -> Dict[str, Any]:
    url = f"{host.rstrip('/')}/{index}/_search"
    limits = httpx.Limits(max_connections=max_conns, max_keepalive_connections=max_keepalive)
    timeout_cfg = httpx.Timeout(timeout)

    # Warm-up (sequential) to stabilize caches/connection pool
    async with httpx.AsyncClient(limits=limits, timeout=timeout_cfg, headers={"Connection": "keep-alive"}) as client:
        for _ in range(max(1, warmup)):
            try:
                await client.post(url, json=body)
            except Exception:
                pass

    lat_ms: List[float] = []
    ok_counter = [0]
    err_counter = [0]
    stop_ts = time.time() + duration

    async with httpx.AsyncClient(limits=limits, timeout=timeout_cfg, headers={"Connection": "keep-alive"}) as client:
        tasks = [
            asyncio.create_task(worker(client, url, body, stop_ts, lat_ms, ok_counter, err_counter))
            for _ in range(conc)
        ]
        await asyncio.gather(*tasks)

    metrics = {
        "index": index,
        "concurrency": conc,
        "duration_s": duration,
        "requests_ok": ok_counter[0],
        "requests_err": err_counter[0],
        "throughput_qps": (ok_counter[0] / duration) if duration else 0.0,
        "samples": len(lat_ms),
        "avg_ms": (statistics.mean(lat_ms) if lat_ms else 0.0),
        "p50_ms": percentile(lat_ms, 50),
        "p90_ms": percentile(lat_ms, 90),
        "p95_ms": percentile(lat_ms, 95),
        "p99_ms": percentile(lat_ms, 99),
        "stdev_ms": (statistics.pstdev(lat_ms) if lat_ms else 0.0),
        "max_ms": (max(lat_ms) if lat_ms else 0.0),
    }
    return metrics

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Async benchmark for OpenSearch queries (httpx + keep-alive).")
    ap.add_argument("--host", default="http://127.0.0.1:9200", help="OpenSearch URL")
    ap.add_argument("--index", default="amazon-music-reviews", help="Index name")
    ap.add_argument("--query-file", default=None, help="Path to a JSON query file (uses a q1-like default if omitted)")
    ap.add_argument("--duration", type=int, default=30, help="Seconds to run each test")
    ap.add_argument("--concurrency", type=int, nargs="+", default=[4, 8, 16, 32], help="One or more concurrency levels")
    ap.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds")
    ap.add_argument("--max-connections", type=int, default=64, help="httpx connection pool size")
    ap.add_argument("--max-keepalive", type=int, default=32, help="httpx keep-alive pool size")
    ap.add_argument("--warmup", type=int, default=3, help="Warm-up request count (sequential)")
    ap.add_argument("--out-json", default=None, help="Write results as JSON array to this path")
    ap.add_argument("--out-csv", default=None, help="Append CSV lines (one per concurrency) to this file")
    return ap.parse_args()

def to_csv_row(m: Dict[str, Any]) -> str:
    fields = [
        m["index"], m["concurrency"], m["duration_s"], m["requests_ok"], m["requests_err"],
        f"{m['throughput_qps']:.2f}", m["samples"],
        f"{m['avg_ms']:.2f}", f"{m['p50_ms']:.2f}", f"{m['p90_ms']:.2f}",
        f"{m['p95_ms']:.2f}", f"{m['p99_ms']:.2f}", f"{m['stdev_ms']:.2f}", f"{m['max_ms']:.2f}"
    ]
    return ",".join(map(str, fields))

def main() -> None:
    args = parse_args()
    body = load_query(args.query_file)
    results: List[Dict[str, Any]] = []

    for c in args.concurrency:
        print(f"[bench] concurrency={c}, duration={args.duration}s ...")
        metrics = asyncio.run(run_once(
            host=args.host,
            index=args.index,
            body=body,
            duration=args.duration,
            conc=c,
            timeout=args.timeout,
            max_conns=max(args.max_connections, c * 2),
            max_keepalive=max(args.max_keepalive, c),
            warmup=args.warmup,
        ))
        print(json.dumps(metrics, indent=2))
        results.append(metrics)

        if args.out_csv:
            header = "index,concurrency,duration_s,requests_ok,requests_err,throughput_qps,samples,avg_ms,p50_ms,p90_ms,p95_ms,p99_ms,stdev_ms,max_ms"
            need_header = not os.path.exists(args.out_csv)
            with open(args.out_csv, "a", encoding="utf-8") as f:
                if need_header:
                    f.write(header + "\n")
                f.write(to_csv_row(metrics) + "\n")

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
