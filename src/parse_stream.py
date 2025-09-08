#!/usr/bin/env python3
# parse_stream.py
# Stream-parse Amazon "Music" review format into cleaned JSONL.
# - Robust to odd spacing around keys, slashes, and colons
# - Computes helpfulness_up/down, parses price/score/time
# - Normalizes zero-width & excessive whitespace
# - Outputs to stdout or Kafka (optional)
# - Supports .gz input transparently and rate limiting

import argparse, sys, re, json, io, gzip, time
from typing import Dict, Iterator, Optional

# ---------- Key parsing ----------

LINE_RE = re.compile(r'^\s*([A-Za-z]+)\s*/\s*([A-Za-z]+)\s*:\s*(.*)$')
ZWNJ_RE = re.compile(r'[\u200b-\u200f\u202a-\u202e\u2060]', re.UNICODE)  # common zero-widths
WS_RE   = re.compile(r'\s+')

KEYMAP = {
    ('product','productid'): 'productId',
    ('product','title')    : 'title',
    ('product','price')    : 'price',
    ('review','userid')    : 'userId',
    ('review','profilename'): 'profileName',
    ('review','helpfulness'): 'helpfulness_raw',
    ('review','score')     : 'score',
    ('review','time')      : 'time',
    ('review','summary')   : 'summary',
    ('review','text')      : 'text',
}

def try_lenient_match(line: str):
    """
    Attempt to parse lines with weird spacing like 'review / t e x t : value'
    or extra spaces around '/' and ':'.
    Returns tuple (k1, k2, value) or None.
    """
    # Fast path: normal regex
    m = LINE_RE.match(line)
    if m:
        return m.group(1), m.group(2), m.group(3)

    # Lenient path: split once on ':' then normalize the left part
    parts = re.split(r'\s*:\s*', line, maxsplit=1)
    if len(parts) != 2:
        return None
    left, value = parts[0], parts[1]
    # Collapse spaces and normalize slashes
    left = re.sub(r'\s*/\s*', '/', left.strip())
    # Remove all internal spaces (handles t i t l e, p r o d u c t I d)
    left = left.replace(' ', '')
    if '/' not in left:
        return None
    k1, k2 = left.split('/', 1)
    return k1, k2, value

def normalize_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    # Remove zero-widths, collapse whitespace, trim
    s = ZWNJ_RE.sub(' ', s)
    s = WS_RE.sub(' ', s).strip()
    return s

def parse_helpfulness(h: Optional[str]):
    """
    Parse strings like '3/4' -> (up=3, down=1). On failure returns (None, None).
    """
    if not h or '/' not in h:
        return None, None
    try:
        a, b = h.split('/', 1)
        up = int(a.strip())
        total = int(float(b.strip()))
        down = max(0, total - max(0, up))
        return up, down
    except Exception:
        return None, None

def parse_blocks(fp: io.TextIOBase) -> Iterator[Dict]:
    """
    Read the Amazon format where reviews come as blocks of 'k1 / k2 : value' lines,
    separated by blank lines. Be tolerant to odd spacing.
    """
    block_raw: Dict[str, str] = {}
    last_text_key = None  # remember if we are inside summary/text to append wrapped lines

    for raw in fp:
        line = raw.rstrip('\n')
        if not line.strip():
            if block_raw:
                yield block_raw
                block_raw = {}
                last_text_key = None
            continue

        parsed = try_lenient_match(line)
        if not parsed:
            # If the line doesn't match a key/value, treat it as a continuation of text/summary
            if last_text_key and last_text_key in block_raw:
                block_raw[last_text_key] = f"{block_raw[last_text_key]}\n{line}"
            continue

        k1, k2, val = parsed
        k1, k2 = k1.lower(), k2.lower()
        key = f"{k1}/{k2}"
        block_raw[key] = val.strip()

        if key in ("review/text", "review/summary"):
            last_text_key = key
        else:
            last_text_key = None

    if block_raw:
        yield block_raw

def convert_block(b: Dict[str, str], require_both: bool = True) -> Optional[Dict]:
    """
    Map raw keys to a clean doc with types and derived fields.
    If require_both=True, drop records missing either summary or text.
    """
    get = lambda k: b.get(k)

    pid   = get('product/productid')
    title = get('product/title')
    price = get('product/price')
    user  = get('review/userid')
    pname = get('review/profilename')
    helpf = get('review/helpfulness')
    score = get('review/score')
    rtime = get('review/time')
    summ  = get('review/summary')
    text  = get('review/text')

    # Normalize text fields
    title_n = normalize_text(title)
    summ_n  = normalize_text(summ)
    text_n  = normalize_text(text)

    if require_both:
        if not summ_n or not text_n:
            return None
    else:
        if not (summ_n or text_n):
            return None
    if text_n and len(text_n) < 10:  # discard ultra-short reviews
        return None

    # Helpful votes
    up, down = parse_helpfulness(helpf)

    # Numeric conversions
    price_f = None
    if price:
        try: price_f = float(price)
        except: price_f = None

    score_f = None
    if score:
        try: score_f = float(score)
        except: score_f = None

    time_i = None
    if rtime:
        try: time_i = int(float(rtime))
        except: time_i = None

    doc = {
        "productId": pid,
        "title": title_n,
        "price": price_f,
        "userId": user,
        "profileName": pname,
        "helpfulness_up": up,
        "helpfulness_down": down,
        "score": score_f,
        "time": time_i,
        "summary": summ_n,
        "text": text_n
    }
    return doc

# ---------- IO helpers ----------

def open_text_auto(path: str) -> io.TextIOBase:
    """Open plain text or .gz transparently as text (utf-8, ignore errors)."""
    if path == "-":
        return sys.stdin
    if path.lower().endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")

def get_producer(brokers: Optional[str]):
    """Return a KafkaProducer or None based on brokers string."""
    if not brokers:
        return None
    try:
        from kafka import KafkaProducer  # pip install kafka-python
        return KafkaProducer(
            bootstrap_servers=[s.strip() for s in brokers.split(",") if s.strip()],
            value_serializer=lambda d: json.dumps(d, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        print(f"[fatal] KafkaProducer init failed: {e}", file=sys.stderr)
        sys.exit(2)

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(
        description="Parse Amazon Music format into cleaned JSONL (stdout) and/or Kafka.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    ap.add_argument("--input", default="-", help="Input path (.txt or .txt.gz), or '-' for stdin")
    ap.add_argument("--require-both", action="store_true", help="Drop records unless BOTH summary and text exist")
    ap.add_argument("--min-text-len", type=int, default=10, help="Minimum text length to keep (after normalization)")
    ap.add_argument("--emit", choices=["stdout", "kafka", "both"], default="stdout", help="Where to send cleaned docs")
    ap.add_argument("--kafka-brokers", default=None, help="Kafka brokers (e.g., localhost:9092)")
    ap.add_argument("--kafka-topic", default="reviews_clean", help="Kafka topic name")
    ap.add_argument("--rate", type=int, default=0, help="Max docs per second (0 = no limit)")
    args = ap.parse_args()

    # Open sinks
    producer = get_producer(args.kafka_brokers) if args.emit in ("kafka", "both") else None
    topic = args.kafka_topic

    docs_emitted = 0
    t0 = time.time()
    last_tick = t0
    interval = 1.0 / args.rate if args.rate and args.rate > 0 else 0.0

    with open_text_auto(args.input) as fp:
        for raw_block in parse_blocks(fp):
            doc = convert_block(raw_block, require_both=args.require_both)
            if not doc:
                continue

            # Output
            if args.emit in ("stdout", "both"):
                print(json.dumps(doc, ensure_ascii=False))

            if producer:
                producer.send(topic, doc)

            docs_emitted += 1

            # Rate limiting
            if interval > 0:
                now = time.time()
                elapsed = now - last_tick
                if elapsed < interval:
                    time.sleep(interval - elapsed)
                last_tick = time.time()

    if producer:
        producer.flush()

    dt = max(1e-6, time.time() - t0)
    print(f"[done] Emitted {docs_emitted} docs in {dt:.1f}s  (~{docs_emitted/dt:.1f} docs/s)", file=sys.stderr)

if __name__ == "__main__":
    main()
