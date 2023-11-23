from SPARQLWrapper import SPARQLWrapper, JSON
import csv
# Wikidata SPARQL endpoint
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

# Query to fetch a list of wars
# include instance of war and world war, 
#there are also military conflicts but these include revolutions and strike,battles, riots and disputes not included here
query_get_wars = """
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

#print(results)
with open('war_uris.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['War URI', 'War Label'])

    for result in results["results"]["bindings"]:
        war_id = result['war']['value']
        war_label = result['warLabel']['value']
        writer.writerow([war_id, war_label])

# Run the function to save war URIs and labels to CSV
