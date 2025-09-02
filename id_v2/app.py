import os
import json
import boto3
import logging
import requests
import pandas as pd
import awswrangler as wr

from stats import *
from dashboard import *
from datetime import datetime
from botocore.exceptions import ClientError

PARQUET_BUCKET_NAME = os.environ['PARQUET_BUCKET_NAME']
EXPIRY_DAYS = int(os.environ['CACHE_EXPIRY_IN_DAYS'])
AOS_HOST = os.environ['OS_ENDPOINT']
NEW_INDEX_NAME = os.environ['NEW_INDEX_NAME']
REGION = 'ca-central-1'

cache = {}
cache_date = {}
cache_df = pd.DataFrame()

search_index_name = NEW_INDEX_NAME

def lambda_handler(event, context):
    """
    Parse query string parameters
    """
    
    uuid = event.get('id', False)

    referrer = event.get('referrer', False)

    lang = event.get('lang', False)
    if lang not in ("fr", "en"):
        lang = False
       
    message_en = ""
    message_fr = ""
    response = uuid
    date_time = datetime.utcnow().now()
    cached_datetime_obj = None
    
    if uuid == False or lang == False:
        return {
            'statusCode': 200,
            'message': nonesafe_loads('{ "message_en": "id and language must be provided. Example usage: ?id=XYZ&lang=en", "message_fr": "id et la langue doivent être fournis. Exemple d\'utilisation : ?id=XYZ&lang=fr" }'),
            'body': response
        }

    compound_key = uuid + "_" + lang
    cached_datetime_str = get_from_datetime_cache(compound_key)
    
    # Check to see if there is a cache hit on the datetime cache
    if cached_datetime_str != None:
        format_string = "%Y-%m-%d %H:%M:%S.%f"
        cached_datetime_obj = datetime.strptime(cached_datetime_str, format_string) #convert datetime str to obj
        
        diff = date_time - cached_datetime_obj
        days = diff.days #calculate the difference in days

        # Compare current curr day with cached date_time
        if days < EXPIRY_DAYS:
            # Return the cached result and exit program
            cached_result = get_from_cache(compound_key)
            if cached_result != None:
                ###
                ### Dashboard code for cache hit
                ###
                event['referrer'] = referrer
                event['cached'] = True
                first_item = cached_result['body']['Items'][0]
                event['title_en'] = first_item['title_en']
                event['title_fr'] = first_item['title_fr']
                event['organization'] = extract_org_second_segment(first_item['contact'])
                try:
                    os_client = connect_to_opensearch(REGION, AOS_HOST)
                except:
                    os_client = None
                    print("OpenSearch client is not available. Skipping write.")

                if os_client:
                    write_to_opensearch (os_client, event, search_index_name)
                else:
                    print("OpenSearch is not available. Skipping write.")

                return cached_result
        
    # Cache miss or a need to invalidate the cache
    if uuid != False and lang != False:
        
        #Read the parquet file, and cache dataframe if cold start
        geocore_df = get_df_cache()
        if geocore_df.empty:
            geocore_df = wr.s3.read_parquet(path=PARQUET_BUCKET_NAME, pyarrow_additional_kwargs={"types_mapper": None})
            #print("From S3")
            add_df_to_cache(geocore_df)
        else:
            print("From DF Cache")

        try:
            print(len(geocore_df))
            #Determine if uuid exists - create index on cold start so 'id' is cached subsequently
            geocore_df['features_properties_id'] = geocore_df['features_properties_id'].astype(str)
            geocore_df = geocore_df.set_index('features_properties_id')
            self_df = geocore_df.loc[[uuid]]
            
        except:
            message_en += "uuid not found"
            message_fr += "uuid introuvable"
            return {
                'statusCode': 200,
                'message': nonesafe_loads('{ "message_en": "' + message_en + '", "message_fr": "' + message_fr + '" }'),
                'body': None
            }
        
        try:
            try:
                id                    = uuid
            except:
                return {
                    'statusCode': 200,
                    'message': nonesafe_loads('{ "message_en": "uuid not found", "message_fr": "uuid introuvable" }'),
                    'body': None
                }
            coordinates               = self_df.iloc[0]['features_geometry_coordinates']
            title_en                  = self_df.iloc[0]['features_properties_title_en']
            title_fr                  = self_df.iloc[0]['features_properties_title_fr']
            published                 = self_df.iloc[0]['features_properties_date_published_date']
            options                   = self_df.iloc[0]['features_properties_options']
            contact                   = self_df.iloc[0]['features_properties_contact']
            topicCategory             = self_df.iloc[0]['features_properties_topicCategory']
            created                   = self_df.iloc[0]['features_properties_date_created_date']
            spatialRepresentation     = self_df.iloc[0]['features_properties_spatialRepresentation']
            hnap_type                 = self_df.iloc[0]['features_properties_type']
            
            try:
                begin_temp = self_df.iloc[0]['features_properties_temporalExtent_begin']
                if begin_temp == None:
                    begin_temp = 'None'
            except:
                begin_temp = 'None'
            
            try:
                end_temp = self_df.iloc[0]['features_properties_temporalExtent_end']
                if end_temp == None:
                    end_temp = 'Present'
            except:
                end_temp = 'None'
                
            try:
                temporalExtent        = '{"begin": "' + begin_temp + '", "end": "' + end_temp + '" }'
            except:
                temporalExtent        = '{"begin": "0001-01-01", "end": "0001-01-01"}'
                
            refSys                    = self_df.iloc[0]['features_properties_refSys']
            refSys_version            = self_df.iloc[0]['features_properties_refSys_version']
            status                    = self_df.iloc[0]['features_properties_status']
            maintenance               = self_df.iloc[0]['features_properties_maintenance']
            metadataStandard          = self_df.iloc[0]['features_properties_metadataStandard_en']
            metadataStandardVersion   = self_df.iloc[0]['features_properties_metadataStandardVersion']
            graphicOverview           = self_df.iloc[0]['features_properties_graphicOverview']
            distributionFormat_name   = self_df.iloc[0]['features_properties_distributionFormat_name']
            distributionFormat_format = self_df.iloc[0]['features_properties_distributionFormat_format']
            accessConstraints         = self_df.iloc[0]['features_properties_accessConstraints']
            otherConstraints          = self_df.iloc[0]['features_properties_otherConstraints_en']
            dateStamp                 = self_df.iloc[0]['features_properties_dateStamp']
            dataSetURI                = self_df.iloc[0]['features_properties_dataSetURI']
            try:
                locale                = '{"language": "' + self_df.iloc[0]['features_properties_locale_language'] + '", "country": "' + self_df.iloc[0]['features_properties_locale_country'] +  '", "encoding": "' + self_df.iloc[0]['features_properties_locale_encoding'] +  '" }'
            except:
                locale                = None
            language                  = self_df.iloc[0]['features_properties_language']
            characterSet              = self_df.iloc[0]['features_properties_characterSet']
            environmentDescription    = self_df.iloc[0]['features_properties_environmentDescription']
            distributionFormat_format = self_df.iloc[0]['features_properties_distributionFormat_format']
            supplementalInformation   = self_df.iloc[0]['features_properties_supplementalInformation_en']
            credits                   = self_df.iloc[0]['features_properties_credits']
            cited                     = self_df.iloc[0]['features_properties_cited']
            distributor               = self_df.iloc[0]['features_properties_distributor']
            
            #geocore_extensions
            try:
                plugins                = self_df.iloc[0]['features_properties_plugins']
            except:
                plugins                = None
            
            try:
                sourcesystemname       = self_df.iloc[0]['features_properties_sourceSystemName']
            except:
                sourcesystemname       = None

            #bilingual elements
            if lang == "en":
                description           = self_df.iloc[0]['features_properties_description_en']
                keywords              = self_df.iloc[0]['features_properties_keywords_en']
                useLimits             = self_df.iloc[0]['features_properties_useLimits_en']
            elif lang == "fr":
                description           = self_df.iloc[0]['features_properties_description_fr']
                keywords              = self_df.iloc[0]['features_properties_keywords_fr']
                useLimits             = self_df.iloc[0]['features_properties_useLimits_fr']
            
            #similarity 
            try :
                similarity            = self_df.iloc[0]['features_similarity']
            except:
                similarity            = None 

            #EO collections
            try:
                eoCollection       = self_df.iloc[0]['features_properties_eoCollection']
            except:
                eoCollection       = None
            
            #EO collections
            try:
                eoFilters       = self_df.iloc[0]['features_properties_eoFilters']
            except:
                eoFilters       = None
            #print(eoFilters)
            
            #json elements
            contact = nonesafe_loads(contact)
            distributor = nonesafe_loads(distributor)
            credits = nonesafe_loads(credits)
            cited = nonesafe_loads(cited)
            options = nonesafe_loads(options)
            
            #json elements
            locale = nonesafe_loads(locale)
            temporalExtent = nonesafe_loads(temporalExtent)
            graphicOverview = nonesafe_loads(graphicOverview)
            
            #json elements
            similarity = nonesafe_loads(similarity)
            
            #json elements 
            eoFilters = nonesafe_loads(eoFilters)
            #body response
            response = {"Items": [{ "id": uuid,
                                    "coordinates": coordinates,
                                    "title_en": title_en,
                                    "title_fr": title_fr,
                                    "description": description,
                                    "published": published,
                                    "keywords": keywords,
                                    "topicCategory": topicCategory,
                                    "created": created,
                                    "spatialRepresentation": spatialRepresentation,
                                    "type": hnap_type,
                                    "temporalExtent": temporalExtent,
                                    "refSys": refSys,
                                    "refSys_version": refSys_version,
                                    "status": status,
                                    "maintenance": maintenance,
                                    "metadataStandard": metadataStandard,
                                    "metadataStandardVersion": metadataStandardVersion,
                                    "distributionFormat_name": distributionFormat_name,
                                    "distributionFormat_format": distributionFormat_format,
                                    "useLimits": useLimits,
                                    "accessConstraints": accessConstraints,
                                    "otherConstraints": otherConstraints,
                                    "dateStamp": dateStamp,
                                    "dataSetURI": dataSetURI,
                                    "locale": locale,
                                    "language": language,
                                    "characterSet": characterSet,
                                    "environmentDescription": environmentDescription,
                                    "supplementalInformation": supplementalInformation,
                                    "graphicOverview": graphicOverview,
                                    "contact": contact,
                                    "distributor": distributor,
                                    "credits": credits,
                                    "cited": cited,
                                    "plugins": plugins,
                                    "options": options,
                                    "similarity": similarity,
                                    "sourceSystemName": sourcesystemname,
                                    "eoCollection": eoCollection,
                                    "eoFilters": eoFilters
            }]};
            
        except ClientError as e:
            message_en += "Error parsing parquet with id: " + uuid
            message_fr += "Erreur d'analyse du parquet avec l'id: " + uuid
        
    else:
        message_en += "id and language must be provided. Example usage: ?id=XYZ&lang=en"
        message_fr += "id et la langue doivent être fournis. Exemple d'utilisation : ?id=XYZ&lang=fr"
    
    message = '{ "message_en": "' + message_en + '", "message_fr": "' + message_fr + '" }'
    json_message = nonesafe_loads(message)

    ###
    ### Dashboard code for cache miss
    ###
    event['referrer'] = referrer
    event['cached'] = False
    event['title_en'] = title_en
    event['title_fr'] = title_fr
    event['organization'] = extract_org_second_segment(contact)

    try:
        os_client = connect_to_opensearch(REGION, AOS_HOST)
    except:
        os_client = None
        print("OpenSearch client is not available. Skipping write.")

    if os_client:
        write_to_opensearch (os_client, event, search_index_name)
    else:
        print("OpenSearch is not available. Skipping write.")

    ###
    ### Add statistics to response
    ###

    hits = get_stats(os_client, search_index_name, uuid)
    #print(hits)

    response_clean = clean_na(response)
    json_message_clean = clean_na(json_message)
    
    #Dictionary for the cache
    json_cache = {
        'statusCode': 200,
        'hits': hits,
        'message': nonesafe_loads('{ "message_en": "cached result", "message_fr": "résultat mis en cache" }'),
        'body': response_clean
    }
    
    add_to_cache(compound_key, json_cache)
    add_to_datetime_cache(compound_key, str(date_time))
    
    return {
        'statusCode': 200,
        'hits': hits,
        'message': json_message_clean,
        'body': response_clean
    }

# Wrapper to safely load json objects in case it is null
def nonesafe_loads(obj):
    """Load JSON if string, or return None for pd.NA, np.nan."""
    if isinstance(obj, str):
        try:
            return json.loads(obj)
        except json.JSONDecodeError:
            return obj
    if isinstance(obj, (pd._libs.missing.NAType, float)) and pd.isna(obj):
        return None
    return obj

# Recursively replace pd.NA, np.nan with None for JSON serialization.
def clean_na(obj):
    if isinstance(obj, dict):
        return {k: clean_na(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_na(x) for x in obj]
    elif isinstance(obj, (pd._libs.missing.NAType, float)) and pd.isna(obj):
        return None
    else:
        return obj

# Function to add JSON payload to the cache
def add_to_cache(key, json_payload):
    cache[key] = json_payload

# Function to retrieve JSON payload from the cache
def get_from_cache(key):
    return cache.get(key)
    
# Function to add datetime payload to the cache_date for invalidation
def add_to_datetime_cache(key, datetime):
    cache_date[key] = datetime
    
# Function to retrieve datetime payload from the cache_date for invalidation
def get_from_datetime_cache(key):
    return cache_date.get(key)

# Add dataframe to cache
def add_df_to_cache(dataframe):
    global cache_df
    cache_df = dataframe.copy()

# Get dataframe to cache
def get_df_cache():
    global cache_df
    return cache_df

def extract_org_second_segment(contact_list):
    """
    Extracts the second segment (split by ;) of the 'organisation' field 
    from the first contact in a list.
    
    Returns a dict with 'en' and 'fr' keys. If missing or error, returns None values.
    """
    try:
        # Step 1: If input is a JSON string, parse it
        if isinstance(contact_list, str):
            import json
            contact_list = json.loads(contact_list)

        # Step 2: Ensure it's a list with at least one item
        if not isinstance(contact_list, list) or not contact_list:
            return {"en": None, "fr": None}

        contact = contact_list[0]
        org = contact.get("organisation", {})

        # Step 3: Extract second segment from a semicolon-separated string
        def get_second_segment(s):
            if isinstance(s, str):
                parts = [p.strip() for p in s.split(";")]
                if len(parts) >= 2:
                    return parts[1]
            return None

        return get_second_segment(org.get("en"))

    except Exception as e:
        print("Error in extract_org_second_segment:", e)
        return {"en": None, "fr": None}