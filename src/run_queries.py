#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# اجرا و نمایش خلاصه‌ی نتایج کوئری‌های پروژه.
import argparse, json, http.client

def req(host, method, path, body=None):
  conn = http.client.HTTPConnection(host.replace("http://","").replace("https://",""))
  headers = {"Content-Type":"application/json"}
  payload = json.dumps(body) if isinstance(body, (dict, list)) else body
  conn.request(method, path, body=payload, headers=headers)
  r = conn.getresponse()
  data = r.read()
  if r.status>=300:
    raise SystemExit(f"HTTP {r.status} {data[:200]}")
  return json.loads(data.decode("utf-8"))

def q1(index):  # lyrics & vocals AND score > 4
  return {"size":20,"query":{"bool":{"must":[{"match":{"text":"lyrics"}},{"match":{"text":"vocals"}}],"filter":[{"range":{"score":{"gt":4}}}]}}}

def q2(index):  # top10 frequent terms in 1-star reviews
  return {"size":0,"query":{"term":{"score":1.0}},"aggs":{"top_terms":{"terms":{"field":"text","size":10}}}}

def q3(index):  # helpfulness>0.75 and phrase "catchy tunes"
  return {"size":20,"query":{"bool":{"must":[{"match_phrase":{"text":"catchy tunes"}}],"filter":[{"range":{"helpfulness_ratio":{"gt":0.75}}}]}}}

def q4(index):  # most frequent words where text contains "disappointed"
  return {"size":0,"query":{"match":{"text":"disappointed"}},"aggs":{"top_terms":{"terms":{"field":"text","size":10}}}}

def q5(index):  # products with most reviews containing "feel good"
  return {"size":0,"query":{"match_phrase":{"text":"feel good"}},"aggs":{"by_product":{"terms":{"field":"productId","size":10}}}}

def q6(index):  # reviews with "inspiring" in summary and "positive" in text
  return {"size":20,"query":{"bool":{"must":[{"match":{"summary":"inspiring"}},{"match":{"text":"positive"}}]}}}

def q7(index):  # top frequent terms where helpfulness>0.75
  return {"size":0,"query":{"range":{"helpfulness_ratio":{"gt":0.75}}},"aggs":{"top_terms":{"terms":{"field":"text","size":10}}}}

def q8(index):  # reviews that contain "blues" but NOT "sad"
  return {"size":20,"query":{"bool":{"must":[{"match":{"text":"blues"}}],"must_not":[{"match":{"text":"sad"}}]}}}

def q9(index):  # users with most reviews containing "fun" or similar
  return {"size":0,"query":{"match":{"text":"fun funny fun!"}},"aggs":{"by_user":{"terms":{"field":"profileName","size":10}}}}

def q10(index):  # frequent phrases in 5-star reviews with "emotional"
  return {"size":0,"query":{"bool":{"must":[{"match":{"text":"emotional"}}],"filter":[{"term":{"score":5.0}}]}},"aggs":{"phrases":{"terms":{"field":"text.shingles","size":10}}}}

def q11(index):  # users with highest avg helpfulness
  return {"size":0,"aggs":{"by_user":{"terms":{"field":"profileName","size":10},"aggs":{"avg_help":{"avg":{"field":"helpfulness_ratio"}}}}}}

QS = {"q1":q1,"q2":q2,"q3":q3,"q4":q4,"q5":q5,"q6":q6,"q7":q7,"q8":q8,"q9":q9,"q10":q10,"q11":q11}

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--host", default="http://localhost:9200")
  ap.add_argument("--index", default="amazon-music-reviews")
  ap.add_argument("--q", default="all", help="q1..q11 or all")
  args = ap.parse_args()
  to_run = list(QS.keys()) if args.q=="all" else [args.q]
  for name in to_run:
    q = QS[name](args.index)
    res = req(args.host, "POST", f"/{args.index}/_search", q)
    print(f"== {name} ==")
    if "aggs" in q:
      aggname = next(iter(q["aggs"].keys()))
      buckets = res["aggregations"][aggname]["buckets"]
      for b in buckets[:10]:
        key = b.get("key_as_string", b.get("key"))
        print(f"{key:>20}  {b['doc_count']}")
    else:
      hits = res["hits"]["hits"]
      for h in hits[:5]:
        src = h.get("_source",{})
        print("-", src.get("summary","(no summary)"))
    print()
if __name__ == "__main__":
  main()
