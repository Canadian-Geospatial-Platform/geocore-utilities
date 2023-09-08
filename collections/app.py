import os
import io
import json
import boto3
import requests
import logging
import pandas as pd
import awswrangler as wr

from uuid import UUID
from datetime import datetime
from botocore.exceptions import ClientError

PARQUET_BUCKET_NAME = os.environ['PARQUET_BUCKET_NAME']
EXPIRY_DAYS = 2

cache = {}
cache_date = {}

def lambda_handler(event, context):
    
    """ 
    Parse query string parameters 
    """
    message = ""
    
    try:
        uuid = event["id"]
    except:
        uuid = False
    
    try:
        lang = event["lang"]
    except:
        lang = "en"
    
    if uuid == False:
        message += "No id parameter was passed. Usage: ?id=XYZ&lang=en_or_fr"
        return {
            'statusCode': 200,
            'body': message
        }
    
    date_time = datetime.utcnow().now()
    cached_datetime_obj = None
    
    compound_key = uuid + "_" + lang
    cached_datetime_str = get_from_datetime_cache(compound_key)
    
    #Check to see if there is a cache hit on the datetime cache
    if cached_datetime_str != None:
        format_string = "%Y-%m-%d %H:%M:%S.%f"
        cached_datetime_obj = datetime.strptime(cached_datetime_str, format_string) #convert datetime str to obj
        
        diff = date_time - cached_datetime_obj
        days = diff.days #calculate the difference in days

        #Compare current curr day with cached date_time
        if days < EXPIRY_DAYS:
            #Return the cached result and exit program
            if get_from_cache(compound_key) != None:
                return get_from_cache(compound_key)
        
    #Cache miss or a need to invalidate the cache
    if uuid != False:
        try:
            print("Finding the parent and other children for uuid: ", uuid)
            geocore_df = wr.s3.read_parquet(path=PARQUET_BUCKET_NAME)
        except ClientError as e:
            message += "Error accessing " + PARQUET_BUCKET_NAME
            return {
                'statusCode': 200,
                'body': json.dumps(message)
            }
        #self
        self_json = find_self(geocore_df, uuid)
        
        #parent
        parent_json, parent_id  = find_parent(geocore_df, uuid)
        
        #child
        child_json = None
        child_count = 0
        child_json, child_count = find_children(geocore_df, uuid)
        if child_json != None:
            print("child_json ", lang)
            if lang == 'en':
                child_json = sorted(child_json, key=lambda x: x['description_en'], reverse=True)
            elif lang == 'fr':
                child_json = sorted(child_json, key=lambda x: x['description_fr'], reverse=True)
            else:
                child_json = sorted(child_json, key=lambda x: x['description_fr'], reverse=True)
                

        #sibling
        sibling_json = None
        sibling_count = 0
        if parent_json != None and child_json == None:
            sibling_json, sibling_count  = find_siblings(geocore_df, parent_id, uuid)
            print("sibling_json ", lang)
            if lang == 'en':
                sibling_json = sorted(sibling_json, key=lambda x: x['description_en'], reverse=True)
            elif lang == 'fr':
                sibling_json = sorted(sibling_json, key=lambda x: x['description_fr'], reverse=True)
            else:
                sibling_json = sorted(sibling_json, key=lambda x: x['description_fr'], reverse=True)
        
        #Dictionary for the cache
        json_cache = {
            'statusCode': 200,
            'message': nonesafe_loads('{ "message_en": "cached result", "message_fr": "résultat mis en cache" }'),
            'sibling_count': sibling_count,
            'child_count': child_count,
            'self': self_json,
            'parent': parent_json,
            'sibling': sibling_json,
            'child': child_json
        }
        
        add_to_cache(compound_key, json_cache)
        add_to_datetime_cache(compound_key, str(date_time))
        
    else:
        message += "No id parameter was passed. Usage: ?id=XYZ"
        return {
            'statusCode': 200,
            'body': message
        }
    
    return {
        'statusCode': 200,
        'sibling_count': sibling_count,
        'child_count': child_count,
        'self': self_json,
        'parent': parent_json,
        'sibling': sibling_json,
        'child': child_json
    }

def find_self(geocore_df, uuid):
    """
    Find uuid if it exists
    :param geocore_df: dataframe containing all geocore records
    :param uuid: unique id we are looking up 
    :return message: JSON of the uuid and record title in english and french
    """
    
    self_desc_en = ""
    self_desc_fr = ""
    self_df = geocore_df[geocore_df['features_properties_id'] == uuid]

    if len(self_df) == 0:
        self_message = None
    else:
        try:
            self_desc_en = self_df.iloc[0]['features_properties_title_en'].replace('"', '\\"')
            self_desc_fr = self_df.iloc[0]['features_properties_title_fr'].replace('"', '\\"')
            self_message = '{ "id": "' + uuid + '", "description_en": "' + self_desc_en + '", "description_fr": "' + self_desc_fr + '"}'
        except:
            self_message = None

    return nonesafe_loads(self_message)
    
def find_parent(geocore_df, uuid):
    """
    Find parent record of a uuid if it exists
    :param geocore_df: dataframe containing all geocore records
    :param uuid: unique id we are looking up 
    :return message: JSON of the uuid and record title in english and french
    :return parent_id: uuid of the parent record
    """
    parent_id = ""
    parent_desc_en = ""
    parent_desc_fr = ""
    parent_df = geocore_df[geocore_df['features_properties_id'] == uuid]

    if len(parent_df) == 0:
        parent_message = None
    else:
        try:
            parent_id = parent_df.iloc[0]['features_properties_parentIdentifier']
            parent_desc_en = geocore_df[geocore_df['features_properties_id'] == parent_id].iloc[0]['features_properties_title_en'].replace('"', '\\"')
            parent_desc_fr = geocore_df[geocore_df['features_properties_id'] == parent_id].iloc[0]['features_properties_title_fr'].replace('"', '\\"')
            parent_message = '{ "id": "' + parent_id + '", "description_en": "' + parent_desc_en + '", "description_fr": "' + parent_desc_fr + '"}'
        except:
            parent_message = None
            parent_id = uuid

    return nonesafe_loads(parent_message), parent_id

def find_siblings(geocore_df, parent_id, uuid):
    """
    Find sibling records of a uuid if it exists
    :param geocore_df: dataframe containing all geocore records
    :param uuid: unique id we are looking up
    :return message: JSON of the uuid and record title in english and french
    :return parent_id: uuid of the parent record
    """
    child_array_id = []
    child_array_desc_en = []
    child_array_desc_fr = []
    other_children_df = geocore_df[geocore_df['features_properties_parentIdentifier'] == parent_id]
    
    #cannot be its own sibling. remove self from the siblings dataframe
    other_children_df = other_children_df[other_children_df['features_properties_id'] != uuid]
    
    if len(other_children_df) == 0:
        child_message = None
    else:
        child_message = "["
        for i in range(0,len(other_children_df)):
            child_array_id.append(other_children_df.iloc[i]['features_properties_id'])
            child_array_desc_en.append(other_children_df.iloc[i]['features_properties_title_en'])
            child_array_desc_fr.append(other_children_df.iloc[i]['features_properties_title_fr'])

        for i in range(0,len(other_children_df)):
            child_message += '{ "id": "' + child_array_id[i] + '", "description_en": "' + child_array_desc_en[i].replace('"', '\\"') + '", "description_fr": "' + child_array_desc_fr[i].replace('"', '\\"') + '"}'
            if i != len(other_children_df)-1:
                child_message += ', '
        child_message += "]"
    
    return nonesafe_loads(child_message), len(other_children_df)
    
def find_children(geocore_df, uuid):
    """
    Find child records if it exists
    :param geocore_df: dataframe containing all geocore records
    :param uuid: unique id we are looking up
    :return child_json: JSON of the child uuid and record title in english and french
    :return child_count: count of child records
    """
    child_array_id = []
    child_array_desc_en = []
    child_array_desc_fr = []
    other_children_df = geocore_df[geocore_df['features_properties_parentIdentifier'] == uuid]
    
    #cannot be its own sibling. remove self from the siblings dataframe
    other_children_df = other_children_df[other_children_df['features_properties_id'] != uuid]
    
    if len(other_children_df) == 0:
        child_message = None
    else:
        child_message = "["
        for i in range(0,len(other_children_df)):
            child_array_id.append(other_children_df.iloc[i]['features_properties_id'])
            child_array_desc_en.append(other_children_df.iloc[i]['features_properties_title_en'])
            child_array_desc_fr.append(other_children_df.iloc[i]['features_properties_title_fr'])

        for i in range(0,len(other_children_df)):
            child_message += '{ "id": "' + child_array_id[i].replace('"', '\\"') + '", "description_en": "' + child_array_desc_en[i].replace('"', '\\"') + '", "description_fr": "' + child_array_desc_fr[i].replace('"', '\\"') + '"}'
            if i != len(other_children_df)-1:
                child_message += ', '
        child_message += "]"
    
    return nonesafe_loads(child_message), len(other_children_df)
    
def nonesafe_loads(obj):
    if obj is not None:
        return json.loads(obj)

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