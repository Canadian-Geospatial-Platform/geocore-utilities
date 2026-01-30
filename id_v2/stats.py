import os
import boto3
import requests
from requests_aws4auth import AWS4Auth
from datetime import datetime, timedelta

def get_stats(os_client, index, target_id):
    """
    Returns a dictionary with the number of hits in the index for a specific id:
    This function returns the accesses for app.geo.ca for the
      - last 30 days
      - all time
    
    Uses the _msearch functionality to reduce latency by running both queries in one request.
    Always returns integers (0 on error or no hits).. which should have been caught earlier
    """
    now = datetime.utcnow()
    thirty_days_ago = now - timedelta(days=30)

    # Query for last 30 days
    body_last_30_days = {
        "size": 0,
        "track_total_hits": True,
        "query": {
            "bool": {
                "filter": [
                    {"match_phrase": {"id": target_id}},
                    {
                        "range": {
                            "timestamp": {
                                "gte": thirty_days_ago.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                "lte": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                "format": "strict_date_optional_time"
                            }
                        }
                    }
                ]
            }
        }
    }

    # Query for all time
    body_all_time = {
        "size": 0,
        "track_total_hits": True,
        "query": {
            "bool": {
                "filter": [
                    {"match_phrase": {"id": target_id}}
                ]
            }
        }
    }

    # _msearch request format: newline-delimited JSON
    msearch_body = [
        {"index": index}, body_last_30_days,
        {"index": index}, body_all_time
    ]

    hits = {"last_30_days": 0, "all_time": 0}
    try:
        res = os_client.msearch(body=msearch_body)
        hits["last_30_days"] = res["responses"][0].get("hits", {}).get("total", {}).get("value", 0)
        hits["all_time"] = res["responses"][1].get("hits", {}).get("total", {}).get("value", 0)
    except Exception as e:
        print("Error in msearch:", e)

    return hits