from SPARQLWrapper import SPARQLWrapper, JSON
import csv

def fetch_war_information(uri):
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    # Construct your query for additional war information using the passed URI

    # Execute the query
    # Process the query results and save them to a CSV file for this specific war
    query_get_wars_info = """
    SELECT ?war ?warLabel WHERE {
    {
  ?war wdt:P31/wdt:P279* wd:Q198.
  }
  union
  {
  ?war wdt:P31/wdt:P279* wd:Q103495.
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}

"""
##LIMIT 100
    sparql.setQuery(query_get_wars)
    sparql.setReturnFormat(JSON)


# Fetching the list of wars
    results = sparql.query().convert()

    return results

# Read the previously saved CSV file with war URIs and labels
with open('war_uris.csv', mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row

    for row in reader:
        war_uri = row[0]
        war_label = row[1]

        # Call the function to fetch information for each war
        fetch_war_information(war_uri)