import unicodedata
import re
import requests
import pandas as pd

def slugify(value, allow_unicode=False):
	# Taken from https://github.com/django/django/blob/master/django/utils/text.py
	value = str(value)
	if allow_unicode:
		value = unicodedata.normalize("NFKC", value)
	else:
		value = unicodedata.normalize("NFKD", value).encode(
			"ascii", "ignore").decode("ascii")
	value = re.sub(r"[^\w\s-]", "", value.lower())
	return re.sub(r"[-\s]+", "-", value).strip("-_")

# Federal Railroad Administration (FRA) Office of Safety Analysis page is here: https://safetydata.fra.dot.gov/OfficeofSafety/default.aspx
railroads = [
	{
		"code": "LI",
		"name": "Long Island Rail Road"
	}, {
		"code": "NYA",
		"name": "New York & Atlantic Railway Company"
	}, {
		"code": "BHR",
		"name": "Brookhaven Rail, LLC"
	}
]

railroad_ids = [ railroad["code"] for railroad in railroads ]
railroad_where_query_string = "$where=railroadcode='" + "' OR railroadcode='".join(railroad_ids) + "'"
county_where_query_string = "$where=statename='NEW YORK' AND (countyname='NASSAU' OR countyname='SUFFOLK')"

# We are defining the Socrata datasets we want to scrape here.
datasets = [
	{
		"identifier": "m2f8-22s6",
		# "title": "Crossing Inventory Data - Current",
		# "dataset_link": "https://data.transportation.gov/Railroads/Crossing-Inventory-Data-Current/m2f8-22s6/data",
		# "api_documentation_link": "https://dev.socrata.com/foundry/data.transportation.gov/m2f8-22s6",
		"description": "Railroad crossings (current)", 
		"where_query_string": county_where_query_string,
		"dictionary_values": []
	}, {
		"identifier": "vhwz-raag",
		# "title": "Crossing Inventory Data - Historical",
		# "dataset_link": "https://data.transportation.gov/Railroads/Crossing-Inventory-Data-Historical/vhwz-raag/data",
		# "api_documentation_link": "https://dev.socrata.com/foundry/data.transportation.gov/vhwz-raag",
		"description": "Railroad crossings (historical)", 
		"where_query_string": county_where_query_string,
		"dictionary_values": []
	}, {
		"identifier": "85tf-25kj",
		# "title": "Rail Equipment Accident/Incident Data",
		# "dataset_link": "https://data.transportation.gov/Railroads/Rail-Equipment-Accident-Incident-Data/85tf-25kj/data",
		# "api_documentation_link": "https://dev.socrata.com/foundry/data.transportation.gov/85tf-25kj",
		"description": "Railroad equipment accidents", 
		"where_query_string": county_where_query_string,
		"dictionary_values": []
	}, {
		"identifier": "7wn6-i5b9",
		# "title": "Highway-Rail Grade Crossing Accident Data",
		# "dataset_link": "https://data.transportation.gov/Railroads/Highway-Rail-Grade-Crossing-Accident-Data/7wn6-i5b9/data",
		# "api_documentation_link": "https://dev.socrata.com/foundry/data.transportation.gov/7wn6-i5b9",
		"description": "Railroad crossing accidents", 
		"where_query_string": county_where_query_string,
		"dictionary_values": []
	}, {
		"identifier": "rash-pd2d",
		# "title": "Injury/Illness Summary - Casualty Data",
		# "dataset_link": "https://data.transportation.gov/Railroads/Injury-Illness-Summary-Casualty-Data/rash-pd2d/data",
		# "api_documentation_link": "https://dev.socrata.com/foundry/data.transportation.gov/rash-pd2d",
		"description": "Railroad injuries and illnesses", 
		"where_query_string": county_where_query_string,
		"dictionary_values": ["pdf_report"]
	}, {
		"identifier": "m8i6-zdsy",
		# "title": "Injury/Illness Summary - Operational Data",
		# "dataset_link": "https://data.transportation.gov/Railroads/Injury-Illness-Summary-Operational-Data/m8i6-zdsy/data",
		# "api_documentation_link": "https://dev.socrata.com/foundry/data.transportation.gov/m8i6-zdsy",
		"description": "Railroad monthly operational data", 
		"where_query_string": railroad_where_query_string,
		"dictionary_values": []
	}
]

# We are going to call every item within "datasets" a "dataset". As we go through each dataset, we are going to scrape the dataset.
for dataset in datasets:
	try:
		# We are creating an empty list called "results".
		results = []
		url = "https://data.transportation.gov/resource/" + dataset["identifier"] + ".json"
		# The limit can be up to 50000.
		limit = 1000
		count_payload = "$select=count(*)&" + dataset["where_query_string"]
		# "requests" documentation page is here: https://docs.python-requests.org/en/master/user/quickstart/
		count_request = requests.get(url, params=count_payload)
		# As we go through each page of the dataset, we are going to scrape that page of the dataset.
		count = int(count_request.json()[0]["count"])
		i = 0
		while i < count / limit:
			offset = i * limit
			loop_payload = "$limit=" + str(limit) + "&$offset=" + str(offset) + "&" + dataset["where_query_string"]
			loop_request = requests.get(url, params=loop_payload)
			for result in loop_request.json():
				for dictionary_value in dataset["dictionary_values"]:
					result.pop(dictionary_value)
				results.append(result)
			i += 1
		# "pandas" documentation page is here: https://pandas.pydata.org/docs/index.html
		df = pd.DataFrame(results)
		file_name = slugify(dataset["description"])
		df.to_csv("csv/" + file_name + ".csv", index=False)
	except:
		pass