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
        BIND(uri as ?event)
        ?event wdt:P31 ?instanceOf.
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
    query_get_wars_info = query_get_wars_info.replace('uri', uri)  # Replace 'uri' with the actual URI value
    sparql.setQuery(query_get_wars_info)
    sparql.setReturnFormat(JSON)


# Fetching the list of wars
    results = sparql.query().convert()
    print(type(results))
    return results

####################################################################


file_path = r"C:\Users\karol\projects\wikiWar\war_uris.csv"
file_path_data = r"C:\Users\karol\projects\wikiWar\war_data_all12245.csv"
# 1 open csv with uris and read them one by one
with open(file_path, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row
    count = 0  # Initialize a counter
    
    for row in reader:
        if count >= 10:  # Check if 10 rows have been processed
            break  # If 10 rows have been processed, exit the loop
        
        war_uri = row[0]
        war_label = row[1]
        war_uri = war_uri.rsplit('/', 1)[-1]  # Split at the last '/' and get the last part
        war_uri = 'wd:'+war_uri
        
        jsonInput = fetch_war_information(war_uri)
        
        keys = jsonInput['head']['vars']  # Define 'keys' within this scope
        
        with open(file_path_data, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            
            # Check if the file is empty
            is_empty = csvfile.tell() == 0
            
            # If the file is empty, write the header row
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
        
        count += 1  # Increment the counter after processing each row