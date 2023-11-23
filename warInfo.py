from SPARQLWrapper import SPARQLWrapper, JSON
import csv
#example uri wd:Q10859
def fetch_war_information(uri):
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    # Construct your query for additional war information using the passed URI

    # Execute the query
    # Process the query results and save them to a CSV file for this specific war
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
        BIND({{uri}} as ?event)
        ?event wdt:P31 ?instanceOf.
        ?instanceOf rdfs:label ?isCivilLabel.
        FILTER(LANG(?isCivilLabel) = "en")
        BIND(IF(?instanceOf = wd:Q8465, "yes", "no") AS ?isCivil)
        BIND(IF(?instanceOf = wd:Q1827102, "yes", "no") AS ?isReligious)
        
        optional{
        ?event wdt:P580 ?start_time.}
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
    #query_get_wars_info = query_get_wars_info.replace('{{uri}}', uri)
    query_get_wars_info = query_get_wars_info.format(uri=uri)
    sparql.setQuery(query_get_wars_info)
    
    sparql.setReturnFormat(JSON)


# Fetching the list of wars
    results = sparql.query().convert()

    return results

# Read the previously saved CSV file with war URIs and labels
file_path = r"C:\Users\karol\projects\wikiWar\war_uris.csv"
# with open(file_path, mode='r', encoding='utf-8') as file:
#     reader = csv.reader(file)
#     next(reader)  # Skip header row

#     for row in reader:
#         war_uri = row[0]
#         war_label = row[1]

#         # Call the function to fetch information for each war
#         fetch_war_information(war_uri)

from itertools import islice


with open(file_path, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row

    # Read the first two rows using islice
    for row in islice(reader, 2):
        war_uri, war_label = row[:2]

        # Call the function to fetch information for each war
        print(fetch_war_information(war_uri))