[![Build collections API](https://github.com/Canadian-Geospatial-Platform/geocore_utilities/actions/workflows/collections-zip-deployment.yml/badge.svg)](https://github.com/Canadian-Geospatial-Platform/geocore_utilities/actions/workflows/collections-zip-deployment.yml)

# Python geoCore Utilities

Useful python aws lambda code snippets to perform operations to support various operations on geocore records and databases

## Contents:

```
-collections: Lambda to find parent, child and siblings of records using Pandas
-dynamodb_operations: snipet of codes which can perform CRUB operations on a dynamodb table
-id_v1: original id lambda written in javascript, queries AWS Athena
-id_v2: refactor id lambda written in python, queries parquet file
-popularity_api: Lambda to perform CRUD operations on the popularity dynamodb table
-popularity_proxy: Lambda to proxy request to intranet using a VPC
```
