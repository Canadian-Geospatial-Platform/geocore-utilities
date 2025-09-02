import json
import boto3

from botocore.exceptions import ClientError, NoCredentialsError
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from requests_aws4auth import AWS4Auth
from opensearchpy.exceptions import ConnectionError

def connect_to_opensearch(REGION, AOS_HOST):
    try:        
        credentials = boto3.Session().get_credentials()
        if not credentials:
            raise NoCredentialsError()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, 'es', session_token=credentials.token)

        os_client = OpenSearch(
            hosts=[{'host': AOS_HOST, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        return os_client
    except NoCredentialsError:
        print("Missing AWS credentials.")
        return None
    except ConnectionError as e:
        print(f"Failed to connect to OpenSearch: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def write_to_opensearch(os_client, event, search_index_name):
    ip_address = event.get('ip_address', '') or ''
    ip_address_forward = event.get('ip_address_forward', '') or ''
    if ip_address_forward:
        ip_address = ip_address_forward.split(',')[0].strip() #Use first forwarded IP address if it exists
    timestamp = event.get('timestamp', '') or int(time.time() * 1000) #milliseconds since epoch
    user_agent = event.get('user_agent', '') or ''
    http_method = event.get('http_method', '') or ''
    id = event.get('id', '') or ''
    lang = event.get('lang', '') or ''
    referrer = event.get('referrer', '') or ''
    org = event.get('organization', '') or ''
    cached = event.get('cached', '') or ''
    title_en = event.get('title_en', '') or ''
    title_fr = event.get('title_fr', '') or ''

    create_opensearch_index(os_client, search_index_name)
    ip2geo_data = {}
    ip2geo_data = ip2geo_handler(os_client, ip_address)
    document = [
        {
            "timestamp": timestamp,
            "lang": lang,
            "id": id,
            "referrer": referrer,
            "organization": org,
            "cached": cached,
            "title_en": title_en,
            "title_fr": title_fr,
            "user_agent": user_agent,
            "http_method": http_method,
            "ip2geo": ip2geo_data
        }
    ]    
    print(f"Document to be indexed: {document}")    
    save_to_opensearch(os_client, search_index_name, document)

def parse_geo_point(ip2geo_data):
    if 'location' in ip2geo_data and isinstance(ip2geo_data['location'], str):
        try:
            lat, lon = map(float, ip2geo_data['location'].split(','))
            ip2geo_data['location'] = {"lat": lat, "lon": lon}  # Convert to geo_point format
        except ValueError:
            print("Invalid location format:", ip2geo_data['location'])
            ip2geo_data['location'] = None  # Handle errors gracefully
    return ip2geo_data

def ip2geo_handler(os_client, ip_address):
    
    ip2geo_payload = {
        "docs": [
            {
                "_index": "test",
                "_id": "1",
                "_source": {
                    "ip": ip_address
                }
            }
        ]
    }

    response = os_client.transport.perform_request(
        method="POST",
        url="/_ingest/pipeline/ip-to-geo-pipeline/_simulate",
        body=json.dumps(ip2geo_payload)
    )

    ip2geo_data = {}

    try:
        ip2geo_data = response["docs"][0]["doc"]["_source"].get("ip2geo", {})
        ip2geo_data = parse_geo_point(ip2geo_data) #ensure lat lon is a geo_point
    except (KeyError, json.JSONDecodeError) as e:
        print("Error extracting ip2geo data:", str(e))
    
    return ip2geo_data


def create_opensearch_index(os_client, index_name):
    """Create a new OpenSearch index if it doesn't exist."""
    if not os_client.indices.exists(index=index_name):
        # Define the mapping for the new index
        index_body = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "id": {"type": "keyword"},
                    "lang": {"type": "keyword"},
                    "user_agent": {"type": "keyword"},
                    "http_method": {"type": "keyword"},
                    "ip2geo": {
                        "properties": {
                            "continent_name": {"type": "keyword"},
                            "region_iso_code": {"type": "keyword"},
                            "city_name": {"type": "keyword"},
                            "country_iso_code": {"type": "keyword"},
                            "country_name": {"type": "keyword"},
                            "region_name": {"type": "keyword"},
                            "location": {"type": "geo_point"},
                            "time_zone": {"type": "keyword"}
                        }
                    }
                }
            }
        }

        response = os_client.indices.create(index=index_name, body=index_body)
        print(f"Created new OpenSearch index: {index_name}")
        return response
    else:
        print(f"Index '{index_name}' already exists.")
        return None

def save_to_opensearch(os_client, index, document):
    """
    Loads the transformed log data into OpenSearch.
    """
    for doc in document:
        response = os_client.index(index=index, body=doc)

