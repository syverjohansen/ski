#!/bin/bash

# Define your JSON string
JSON_INPUT=$(cat <<EOF
{
	"sex" : "M",
	"relay": null,
	"date1" : null,
	"date2" : null,
	"city" : null,
	"country" : null,
	"event" : null,
	"distance": null,
	"ms" : null,
	"technique" : null,
	"place1": null,
	"place2" : null,
	"name" : null,
	"season1" : null,
	"season2" : null,
	"nation" : null,
	"race1" : null,
	"race2" : null
}
EOF
)

# Call the Python script with the JSON string
python3 EloDemo2.py "$JSON_INPUT"