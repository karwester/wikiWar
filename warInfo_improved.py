from SPARQLWrapper import SPARQLWrapper, JSON
import csv
import time
import requests
from urllib.error import HTTPError

#########################################
'''
This script collects war information from wikidata via a sparql query.
It goes through a list of URIs of instances of war, collected with another script
and for each one it gets its properties like, start date, end date, participating countries.
It has a try/except logic for timeouts due to request rate limits imposed by Wikidate.
It can be run a few times to try to complete all the URIs. It will first check whether the data for a URI from 
a loist already exits and will execute if it doesn't. For some queries, it will still time out (for
example the war between Russia and Ukraine) has too much data to download within the limits. For these the query will 
need to be split into smaller queries and ran separately.

Queries time out for:
wd:Q110999040
wd:Q110999040

#run separately without some of the properties



'''
#########################################
# Function to check if URI exists in a file
def uri_exists(uri, file_path):
    #wd:Q10859
    uri_clean = uri.split(':')[-1]
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            uri_from_list = row[0].rsplit('/', 1)[-1]
            if row and uri_clean == uri_from_list:  # Assuming URI is in the first column
                return True
    return False

#example uri wd:Q10859
def fetch_war_information(uri):
######################### SPARQL QUERY #######################################
    query_get_wars_info = """
    SELECT 
    ?event 
    ?eventLabel 
    ?date 
    ?start_date 
    ?end_date 
    ?countryLabel 
    ?locationLabel 
    ?participantLabel 
    ?biggerWarInstanceLabel 
    ?smallerWarInstanceLabel 
    ?casualties 
    ?isReligious 
    ?isCivil
        WHERE {
        BIND(uri as ?event)
        ?event wdt:P31 ?instanceOf.
        BIND(IF(?instanceOf = wd:Q8465, "yes", "no") AS ?isCivil)
        BIND(IF(?instanceOf = wd:Q1827102, "yes", "no") AS ?isReligious)
        
       
        OPTIONAL {
            ?event wdt:P276 ?location.
            ?location rdfs:label ?locationLabel FILTER (lang(?locationLabel) = "en").
        }
        OPTIONAL {
            ?event wdt:P17 ?country.
            ?country rdfs:label ?countryLabel FILTER (lang(?countryLabel) = "en").
        }
        OPTIONAL { ?event wdt:P585 ?date. }
        OPTIONAL { ?event wdt:P580 ?start_date. }
        OPTIONAL { ?event wdt:P582 ?end_date. }
        OPTIONAL {
            ?event wdt:P710 ?participant.
            ?participant rdfs:label ?participantLabel FILTER (lang(?participantLabel) = "en").
        }
        OPTIONAL {
            ?event wdt:P361 ?biggerWarInstance.
            ?biggerWarInstance rdfs:label ?biggerWarInstanceLabel FILTER (lang(?biggerWarInstanceLabel) = "en").
        }
        OPTIONAL {
            ?smallerWarInstance wdt:P361 ?event.
            ?smallerWarInstance rdfs:label ?smallerWarInstanceLabel FILTER (lang(?smallerWarInstanceLabel) = "en").
        }
        OPTIONAL { ?event wdt:P1120 ?casualties. }  # Casualties (optional)
        
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}

"""
##LIMIT 100
    query_get_wars_info = query_get_wars_info.replace('uri', uri)  # Replace 'uri' with the actual URI value

#######################################################################
# don't abuse rate limits

    #print(uri)
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setQuery(query_get_wars_info)
    sparql.setReturnFormat(JSON)

    retries = 5 
    for attempt in range(retries):
        try:
            # Execute the query
            sparql_results = sparql.query().convert()
            return sparql_results
        except HTTPError as e:
            if e.code == 429:  # Rate limit exceeded
                if 'Retry-After' in e.headers:
                    delay = int(e.headers['Retry-After'])
                    time.sleep(delay)
                else:
                    time.sleep(60)  # Fallback to a default delay
            else:
                raise e  # Raise the HTTPError for other status codes
        except Exception as e:
            print(f"Request failed: {e}")
            print("This uri produced an error, possibly times out, investigate separately"+ uri)
            

        return None  # Failed after all retries

####################################################################


file_path = r"C:\Users\karol\projects\wikiWar\war_uris.csv"
file_path_data = r"C:\Users\karol\projects\wikiWar\war_data_all1234.csv"


with open(file_path, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row
    
    for row in reader:
        war_uri = row[0]
        war_label = row[1]
        war_uri = war_uri.rsplit('/', 1)[-1]  # Split at the last '/' and get the last part
        war_uri = 'wd:' + war_uri
        

        
        if not uri_exists(war_uri, file_path_data):  # Check if URI exists in the file
            retries = 3  # Number of retries
            
            for attempt in range(retries):
                jsonInput = fetch_war_information(war_uri)
                
                if jsonInput is not None:
                    try:
                        keys = jsonInput['head']['vars']  # Define 'keys' within this scope

                        with open(file_path_data, 'a', newline='', encoding='utf-8') as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=keys)
                            is_empty = csvfile.tell() == 0

                            if is_empty:
                                writer.writeheader()

                            row_data = {}
                            for key in keys:
                                values = list(set(entry.get(key, {}).get('value', None) for entry in jsonInput['results']['bindings']))
                                if len(values) > 1:
                                    row_data[key] = ', '.join(values)
                                elif len(values) == 1:
                                    row_data[key] = values[0]
                                else:
                                    row_data[key] = None

                            writer.writerow(row_data)
                        
                        break  # Break the retry loop if successful
                    except TypeError as te:
                        print(f"TypeError occurred: {te} ")
                        print("An error occurred at this URI print. Retrying..." + war_uri)
                        time.sleep(10)  # Wait before retrying
                else:
                    print(f"Fetching information for URI {war_uri} failed.")
                    break  # Break the retry loop if fetching failed after retries