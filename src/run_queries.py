#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Run project queries, print a short summary, and save optional files.

import argparse, json, http.client, time, os, csv
from http.client import RemoteDisconnected

SEARCH_TIMEOUT = "45s"         # per-search server-side timeout
SAMPLER_SHARD_SIZE = 2000     # reduce if you still see 503/closures (e.g., 10000 or 5000)
RETRIES = 3
BACKOFF = 1.0                  # seconds; exponential

def req(host, method, path, body=None):
  host_clean = host.replace("http://","").replace("https://","")
  headers = {"Content-Type":"application/json"}
  payload = json.dumps(body) if isinstance(body, (dict, list)) else body
  for attempt in range(RETRIES):
    try:
      conn = http.client.HTTPConnection(host_clean, timeout=90)
      conn.request(method, path, body=payload, headers=headers)
      r = conn.getresponse()
      data = r.read()
      if r.status >= 300:
        raise SystemExit(f"HTTP {r.status} {data[:200]}")
      return json.loads(data.decode("utf-8"))
    except (ConnectionRefusedError, ConnectionResetError, TimeoutError, RemoteDisconnected) as e:
      if attempt == RETRIES - 1:
        raise
      time.sleep(BACKOFF * (2**attempt))

def _sampler(agg_name, inner_agg):
  return {"sample": {"sampler": {"shard_size": SAMPLER_SHARD_SIZE},
                     "aggs": {agg_name: inner_agg}}}

# ---- queries (tokens/shingles subfields assumed in mapping) ----

def q1(index, size):
  return {
    "size": size, "track_total_hits": False, "timeout": SEARCH_TIMEOUT,
    "query": {"bool":{"must":[{"match":{"text":"lyrics"}},{"match":{"text":"vocals"}}],
                      "filter":[{"range":{"score":{"gt":4}}}]}}
  }

def q2(index, size):
  return {
    "size": 0, "timeout": SEARCH_TIMEOUT,
    "query": {"term": {"score": 1.0}},
    "aggs": _sampler("top_terms", {
      "terms": {
        "field": "text.tokens",
        "size": 10,
        "shard_size": 200,      # limit per-shard candidates
        "min_doc_count": 50,     # skip very rare tokens
        "exclude":"^(i|you|we|they|have|has|had|all|one|just|also|really|very|music|album|cd|song|songs)$"
      }
      
    })
  }


def q3(index, size):
  return {
    "size": size, "track_total_hits": False, "timeout": SEARCH_TIMEOUT,
    "query": {"bool":{"must":[{"match_phrase":{"text":"catchy tunes"}}],
                      "filter":[{"range":{"helpfulness_ratio":{"gt":0.75}}}]}}
  }

def q4(index, size):
  return {
    "size": 0, "timeout": SEARCH_TIMEOUT,
    "query": {"match": {"text": "disappointed"}},
    "aggs": _sampler("top_terms", {
      "terms": { "field": "text.tokens", "size": 10, "shard_size": 200, "min_doc_count": 50 }
    })
  }


def q5(index, size):
  return {
    "size": 0, "timeout": SEARCH_TIMEOUT,
    "query": {"match_phrase":{"text":"feel good"}},
    "aggs": _sampler("by_product", {"terms":{"field":"productId","size":10}})
  }

def q6(index, size):
  return {
    "size": size, "track_total_hits": False, "timeout": SEARCH_TIMEOUT,
    "query": {"bool":{"must":[{"match":{"summary":"inspiring"}},
                              {"match":{"text":"positive"}}]}}
  }


def q7(index, size):
  return {
    "size": 0, "timeout": SEARCH_TIMEOUT,
    "query": {"range": {"helpfulness_ratio": {"gt": 0.75}}},
    "aggs": _sampler("top_terms", {
      "terms": { "field": "text.tokens", "size": 10, "shard_size": 200, "min_doc_count": 50 }
    })
  }

def q8(index, size):
  return {
    "size": size, "track_total_hits": False, "timeout": SEARCH_TIMEOUT,
    "query": {"bool":{"must":[{"match":{"text":"blues"}}],
                      "must_not":[{"match":{"text":"sad"}}]}}
  }

def q9(index, size):
  return {
    "size": 0, "timeout": SEARCH_TIMEOUT,
    "query": {"match":{"text":"fun funny fun!"}},
    "aggs": _sampler("by_user", {"terms":{"field":"profileName","size":10}})
  }


def q10(index, size):
  return {
    "size": 0, "timeout": SEARCH_TIMEOUT,
    "query": {
      "bool": {
        "must":   [{"match": {"text": "emotional"}}],
        "filter": [{"term":  {"score": 5.0}}]
      }
    }
    ,
    "aggs": _sampler("phrases", {
      "terms": { "field": "text.shingles", "size": 10, "shard_size": 100, "min_doc_count": 5 }
    })
  }

def q11(index, size):
  return {
    "size": 0, "timeout": SEARCH_TIMEOUT,
    "aggs": {"by_user":{"terms":{"field":"profileName","size":10},
                        "aggs":{"avg_help":{"avg":{"field":"helpfulness_ratio"}}}}}
  }

QS = {"q1":q1,"q2":q2,"q3":q3,"q4":q4,"q5":q5,"q6":q6,"q7":q7,"q8":q8,"q9":q9,"q10":q10,"q11":q11}

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--host", default="http://127.0.0.1:9200")
  ap.add_argument("--index", default="amazon-music-reviews")
  ap.add_argument("--q", default="all", help="q1..q11 or all")
  ap.add_argument("--size", type=int, default=100)
  ap.add_argument("--out-dir", default="outputs")
  args = ap.parse_args()

  os.makedirs(args.out_dir, exist_ok=True)
  to_run = list(QS.keys()) if args.q == "all" else [args.q]

  for name in to_run:
    body = QS[name](args.index, args.size)
    res = req(args.host, "POST", f"/{args.index}/_search", body)
    print(f"== {name} ==")

    # Save raw for debugging
    raw_path = os.path.join(args.out_dir, f"{name}_raw.json")
    with open(raw_path, "w", encoding="utf-8") as f:
      json.dump(res, f, ensure_ascii=False, indent=2)
    print(f"saved raw: {raw_path}")

    # Summaries
    if "aggs" in body:
      # unwrap sampler if present
      aggs = body["aggs"]
      if "sample" in aggs:
        inner = aggs["sample"]["aggs"]
        aggname = next(iter(inner.keys()))
      else:
        aggname = next(iter(aggs.keys()))
      buckets = res["aggregations"]
      # account for sampler in response
      if "sample" in buckets:
        buckets = buckets["sample"]
      buckets = buckets[aggname]["buckets"]
      for b in buckets[:10]:
        key = b.get("key_as_string", b.get("key"))
        print(f"{key:>20}  {b['doc_count']}")
      # CSV
      with open(os.path.join(args.out_dir, f"{name}_buckets.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["key","doc_count"])
        for b in buckets: w.writerow([b.get("key_as_string", b.get("key")), b["doc_count"]])
    else:
      hits = res["hits"]["hits"]
      for h in hits[:5]:
        src = h.get("_source",{})
        print("-", (src.get("summary","(no summary)"))[:120].replace("\n"," "))
      with open(os.path.join(args.out_dir, f"{name}_hits.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["_id","_score","productId","score","helpfulness_ratio","time","summary"])
        for h in hits:
          s = h.get("_source",{})
          w.writerow([h.get("_id"), h.get("_score"), s.get("productId"), s.get("score"),
                      s.get("helpfulness_ratio"), s.get("time"),
                      (s.get("summary") or "").replace("\n"," ")[:200]])
    total = res.get("hits",{}).get("total",{})
    if isinstance(total, dict):
      print(f"total hits: {total.get('value')} (relation: {total.get('relation')})")
    print()

if __name__ == "__main__":
  main()
