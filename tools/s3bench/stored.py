#!/usr/bin/env python3
"""List stored cells/ objects and bytes per key family.

Usage: stored.py <bucket>

Reads S3_ENDPOINT (the real store, not the proxy), AWS_* credentials and
AWS_REGION from the environment, the same variables run.sh exports.
"""

import os
import sys
from collections import Counter

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from count import family  # noqa: E402


def main():
    bucket = sys.argv[1]
    endpoint = os.environ.get("S3_ENDPOINT_REAL") or os.environ.get("S3_ENDPOINT")
    if not endpoint:
        print("stored.py: set S3_ENDPOINT_REAL (or S3_ENDPOINT)", file=sys.stderr)
        sys.exit(2)
    session_kwargs = {}
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        session_kwargs["aws_access_key_id"] = os.environ["AWS_ACCESS_KEY_ID"]
    if os.environ.get("AWS_SECRET_ACCESS_KEY"):
        session_kwargs["aws_secret_access_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]
    session = boto3.session.Session(**session_kwargs)
    s3 = session.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    objects = Counter()
    bytes_ = Counter()
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": "cells/"}
        if token:
            kwargs["ContinuationToken"] = token
        page = s3.list_objects_v2(**kwargs)
        for entry in page.get("Contents", []):
            fam = family("/" + entry["Key"])
            objects[fam] += 1
            bytes_[fam] += entry["Size"]
        if page.get("IsTruncated"):
            token = page["NextContinuationToken"]
        else:
            break
    total_objects = 0
    total_bytes = 0
    for fam in sorted(objects):
        print(f"{fam} objects={objects[fam]} bytes={bytes_[fam]}")
        total_objects += objects[fam]
        total_bytes += bytes_[fam]
    print(f"TOTAL objects={total_objects} bytes={total_bytes}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: stored.py <bucket>", file=sys.stderr)
        sys.exit(2)
    main()
