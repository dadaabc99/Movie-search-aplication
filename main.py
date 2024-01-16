import csv
from flask import Flask, render_template, request
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
from flasgger import Swagger

app = Flask(__name__)
Swagger(app)

es = Elasticsearch(['https://localhost:9200'], verify_certs=False, http_auth=('Mihai', '123456'))

# Define nested mapping with 'keyword' for specific nested fields
mapping = {
    "properties": {
        "Rank": {"type": "integer"},
        "Title": {"type": "text"},
        "Genre": {"type": "keyword"},
        "Description": {"type": "text"},
        "Director": {"type": "text"},
        "Actors": {
            "type": "nested",
            "properties": {
                "name": {"type": "text"},
                "role": {"type": "keyword"}
            }
        },
        "Year": {"type": "integer"},
        "Runtime": {"type": "integer"},
        "Rating": {"type": "float"},
        "Votes": {"type": "integer"},
        "Revenue": {"type": "float"},
        "Metascore": {"type": "integer"}
    }
}

# Create a new index with the defined mapping
# es.indices.create(index='movies_index', body={"mappings": mapping})

def index_data():
    data = []
    with open('dataset.csv', 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            actors_list = [{"name": actor.strip(), "role": "actor"} for actor in row['Actors'].split(',')]

            # Check if 'Revenue (Millions)' is not an empty string before converting to float
            revenue = float(row['Revenue (Millions)']) if row['Revenue (Millions)'].strip() else None

            # Check if 'Metascore' is not an empty string before converting to int
            metascore = int(row['Metascore']) if row['Metascore'].strip() else None

            data.append({
                "Rank": int(row['Rank']),
                "Title": row['Title'],
                "Genre": row['Genre'],
                "Description": row['Description'],
                "Director": row['Director'],
                "Actors": actors_list,
                "Year": int(row['Year']),
                "Runtime": int(row['Runtime (Minutes)']),
                "Rating": float(row['Rating']),
                "Votes": int(row['Votes']),
                "Revenue": revenue,
                "Metascore": metascore
            })

    # Bulk index documents in Elasticsearch
    bulk_data = [{"_op_type": "index", "_index": 'movies_index', "_source": item} for item in data]
    bulk(es, bulk_data)

# Index data when the application starts
index_data()

@app.route('/', methods=['GET', 'POST'])
def search_movies():
    """
    Movie Search API
    ---
    parameters:
      - name: query
        in: formData
        type: string
        description: Enter the search query
    responses:
      200:
        description: A list of movies matching the search query
    """
    if request.method == 'POST':
        query = request.form.get('query', '')
        search_query = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["Title", "Description", "Director", "Actors.name"]
                }
            }
        }
        res = es.search(index='movies_index', body=search_query)
        search_results = [hit['_source'] for hit in res['hits']['hits']]
        return render_template('index.html', query=query, search_results=search_results)
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
