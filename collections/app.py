import os
import io
import json
import boto3
import requests
import logging
import pandas as pd
import awswrangler as wr

from uuid import UUID
from botocore.exceptions import ClientError

PARQUET_BUCKET_NAME = os.environ['PARQUET_BUCKET_NAME']
MAX_CHILD_OR_SIBLING_LENGTH = int(os.environ['MAX_CHILD_OR_SIBLING_LENGTH'])

cache_df = pd.DataFrame()

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
    

    if uuid != False:
        
        geocore_df = get_df_cache()
        if geocore_df.empty:
            try:
                #print("Finding the parent and other children for uuid: ", uuid)
                geocore_df = wr.s3.read_parquet(path=PARQUET_BUCKET_NAME)
                #print("From S3")
                add_df_to_cache(geocore_df)
            except ClientError as e:
                message += "Error accessing " + PARQUET_BUCKET_NAME
                return {
                    'statusCode': 200,
                    'body': json.dumps(message)
                }
        else:
            print("From DF Cache")
        
        #self
        self_json = find_self(geocore_df, uuid)
        
        #parent
        parent_json, parent_id  = find_parent(geocore_df, uuid)
        
        #child
        child_json = None
        child_count = 0
        child_json, child_count = find_children(geocore_df, uuid)
        
        if child_json != None:
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
            if lang == 'en':
                sibling_json = sorted(sibling_json, key=lambda x: x['description_en'], reverse=True)
            elif lang == 'fr':
                sibling_json = sorted(sibling_json, key=lambda x: x['description_fr'], reverse=True)
            else:
                sibling_json = sorted(sibling_json, key=lambda x: x['description_fr'], reverse=True)
        
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
            parent_row = geocore_df.loc[geocore_df['features_properties_id'] == parent_id]
            parent_desc_en = parent_row['features_properties_title_en'].values[0].replace('"', '\\"')
            parent_desc_fr = parent_row['features_properties_title_fr'].values[0].replace('"', '\\"')
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

        #vectorized optimized code 
        child_array_id.extend(other_children_df['features_properties_id'].tolist())
        child_array_desc_en.extend(other_children_df['features_properties_title_en'].tolist())
        child_array_desc_fr.extend(other_children_df['features_properties_title_fr'].tolist())
            
        if (len(other_children_df) > MAX_CHILD_OR_SIBLING_LENGTH):
            child_length = MAX_CHILD_OR_SIBLING_LENGTH
        else:
            child_length = len(other_children_df)
            
        for i in range(0,child_length):
            child_message += '{ "id": "' + child_array_id[i] + '", "description_en": "' + child_array_desc_en[i].replace('"', '\\"') + '", "description_fr": "' + child_array_desc_fr[i].replace('"', '\\"')  + '"}'
            if i != child_length-1:
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
        
        #vectorized optimized code 
        child_array_id.extend(other_children_df['features_properties_id'].tolist())
        child_array_desc_en.extend(other_children_df['features_properties_title_en'].tolist())
        child_array_desc_fr.extend(other_children_df['features_properties_title_fr'].tolist())
        
        if (len(other_children_df) > MAX_CHILD_OR_SIBLING_LENGTH):
            child_length = MAX_CHILD_OR_SIBLING_LENGTH
        else:
            child_length = len(other_children_df)
            
        for i in range(0,child_length):
            child_message += '{ "id": "' + child_array_id[i].replace('"', '\\"') + '", "description_en": "' + child_array_desc_en[i].replace('"', '\\"')  + '", "description_fr": "' + child_array_desc_fr[i].replace('"', '\\"') + '"}'
            if i != child_length-1:
                child_message += ', '
        child_message += "]"
    
    return nonesafe_loads(child_message), len(other_children_df)
    
def nonesafe_loads(obj):
    if obj is not None:
        return json.loads(obj)

# Add dataframe to cache
def add_df_to_cache(dataframe):
    global cache_df
    cache_df = dataframe.copy()

# Get dataframe to cache
def get_df_cache():
    global cache_df
    return cache_df