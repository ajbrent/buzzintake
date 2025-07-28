#!/bin/sh
set -e

# Wait for iceberg-rest to be healthy
for i in $(seq 1 30); do
  if curl -sf http://iceberg-rest:8181/v1/namespaces; then
    echo "Iceberg REST service is healthy."
    break
  fi
  sleep 2
done

# Create desired namespace
curl -X POST http://iceberg-rest:8181/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": ["gdelt"],
    "properties": {
      "location": "/home/iceberg/warehouse/gdelt"
    }
  }'

# Create articles table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "articles",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "collection_time", "type": "string", "required": false},
        {"id": 3, "name": "src_collection_id", "type": "string", "required": false},
        {"id": 4, "name": "src_common_name", "type": "string", "required": false},
        {"id": 5, "name": "doc_id", "type": "string", "required": false},
        {"id": 6, "name": "tone", "type": "float", "required": false},
        {"id": 7, "name": "positive_score", "type": "float", "required": false},
        {"id": 8, "name": "negative_score", "type": "float", "required": false},
        {"id": 9, "name": "polarity", "type": "float", "required": false},
        {"id": 10, "name": "activity_reference_density", "type": "float", "required": false},
        {"id": 11, "name": "self_group_reference_density", "type": "float", "required": false},
        {"id": 12, "name": "word_count", "type": "int", "required": false},
        {"id": 13, "name": "translation_info", "type": "string", "required": false},
        {"id": 14, "name": "extras_xml", "type": "string", "required": false},
        {"id": 15, "name": "gcam", "type": "string", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/articles"
  }'

# Create amounts table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "amounts",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "amount", "type": "float", "required": false},
        {"id": 3, "name": "object", "type": "string", "required": false},
        {"id": 4, "name": "char_offset", "type": "int", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/amounts"
  }'

# Create all_names table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "all_names",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "name", "type": "string", "required": false},
        {"id": 3, "name": "char_offset", "type": "int", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/all_names"
  }'

# Create quotations table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "quotations",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "char_offset", "type": "int", "required": false},
        {"id": 3, "name": "length", "type": "int", "required": false},
        {"id": 4, "name": "verb", "type": "string", "required": false},
        {"id": 5, "name": "quote", "type": "string", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/quotations"
  }'

# Create social_video_embeds table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "social_video_embeds",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "url", "type": "string", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/social_video_embeds"
  }'

# Create social_image_embeds table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "social_image_embeds",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "url", "type": "string", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/social_image_embeds"
  }'

# Create sharing_images table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sharing_images",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "url", "type": "string", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/sharing_images"
  }'

# Create related_images table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "related_images",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "url", "type": "string", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/related_images"
  }'

# Create dates table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "dates",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "date_resolution", "type": "string", "required": false},
        {"id": 3, "name": "month", "type": "int", "required": false},
        {"id": 4, "name": "day", "type": "int", "required": false},
        {"id": 5, "name": "year", "type": "int", "required": false},
        {"id": 6, "name": "char_offset", "type": "int", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/dates"
  }'

# Create organizations table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "organizations",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "organization_name", "type": "string", "required": false},
        {"id": 3, "name": "char_offset", "type": "int", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/organizations"
  }'

# Create persons table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "persons",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "person_name", "type": "string", "required": false},
        {"id": 3, "name": "char_offset", "type": "int", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/persons"
  }'

# Create locations table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "locations",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "location_type", "type": "int", "required": false},
        {"id": 3, "name": "location_full_name", "type": "string", "required": false},
        {"id": 4, "name": "location_country_code", "type": "string", "required": false},
        {"id": 5, "name": "location_adm1_code", "type": "string", "required": false},
        {"id": 6, "name": "location_adm2_code", "type": "string", "required": false},
        {"id": 7, "name": "location_latitude", "type": "float", "required": false},
        {"id": 8, "name": "location_longitude", "type": "float", "required": false},
        {"id": 9, "name": "location_feature_id", "type": "string", "required": false},
        {"id": 10, "name": "char_offset", "type": "int", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/locations"
  }'

# Create themes table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "themes",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "gkg_theme", "type": "string", "required": false},
        {"id": 3, "name": "char_offset", "type": "int", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/themes"
  }'

# Create counts table
curl -X POST http://iceberg-rest:8181/v1/namespaces/gdelt/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "counts",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "record_id", "type": "string", "required": true},
        {"id": 2, "name": "count_type", "type": "string", "required": false},
        {"id": 3, "name": "count", "type": "int", "required": false},
        {"id": 4, "name": "object_type", "type": "string", "required": false},
        {"id": 5, "name": "location_type", "type": "int", "required": false},
        {"id": 6, "name": "location_full_name", "type": "string", "required": false},
        {"id": 7, "name": "location_country_code", "type": "string", "required": false},
        {"id": 8, "name": "location_adm1_code", "type": "string", "required": false},
        {"id": 9, "name": "location_latitude", "type": "float", "required": false},
        {"id": 10, "name": "location_longitude", "type": "float", "required": false},
        {"id": 11, "name": "location_feature_id", "type": "string", "required": false},
        {"id": 12, "name": "char_offset", "type": "int", "required": false}
      ]
    },
    "location": "/home/iceberg/warehouse/gdelt/counts"
  }'
