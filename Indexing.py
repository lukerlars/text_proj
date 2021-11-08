import csv
import json
from typing import Callable, Dict, List, Set, Tuple
import math
import numpy as np
from elasticsearch import Elasticsearch
from tqdm import tqdm



INDEX_SETTINGS = {
    "mappings": {
        "properties":{
            "body": {"type": "text", "term_vector": "yes", "analyzer": "english"},
        }      
    } 
}

def reset_index(es: Elasticsearch) -> None:
    """Clears index"""
    if es.indices.exists(index =index_name):
        es.indices.delete(index=index_name)

    es.indices.create(index=index_name, body=INDEX_SETTINGS)



def index_documents(filepath: str, es: Elasticsearch, index: str) -> None:
    """Indexes documents from file."""
    bulk_data = []
   
    file = open(filepath, encoding="utf8")
    read_tsv=csv.reader(file, delimiter="\t")
    print("Indexing as began...")
    count =1
    for passage in tqdm(read_tsv):
        bulk_data.append(
            {"index": {"_index": index, "_id": passage[0]}}
        )
        bulk_data.append({"body":passage[1]})

        if count % 1001 == 0:
            es.bulk(index=index, body=bulk_data)
            bulk_data =[]
        count += 1
    print("Indexing Finished.")

def analyze_query(
    es: Elasticsearch, query: str, index: str = "_myindex"
) -> List[str]:
    """Analyzes a query with respect to the relevant index.

    Args:
        es: Elasticsearch object instance.
        query: String of query terms.
        index: Name of the index with respect to which the query is analyzed.

    Returns:
        A list of query terms that exist in the specified field among the
        documents in the index.
    """
    tokens = es.indices.analyze(index=index, body={"text": query})["tokens"]
    query_terms = []
    for t in sorted(tokens, key=lambda x: x["position"]):
        # Use a boolean query to find at least one document that contains the
        # term.
        hits = (
            es.search(
                index=index,
                query={"match": {"body": t["token"]}},
                _source=False,
                size=1,
            )
            .get("hits", {})
            .get("hits", {})
        )
        doc_id = hits[0]["_id"] if len(hits) > 0 else None
        if doc_id is None:
            continue
        query_terms.append(t["token"])
    return query_terms

def load_queries(filepath: str) -> Dict[str, str]:
    """Given a filepath, returns a dictionary with query IDs and corresponding
    query strings.


    Take as query ID the value (on the same line) after `<num> Number: `, 
    and take as the query string the rest of the line after `<title> `. Omit
    newline characters.

    Args:
        filepath: String (constructed using os.path) of the filepath to a
        file with queries.

    Returns:
        A dictionary with query IDs and corresponding query strings.
    """
    # TODO
    d={}
    key=""
    with open(filepath,'r', encoding="utf-8") as file :
        for line in file :
            line = json.loads(line)
            ln=line.strip().split(" ")
            if ln[0]=="<num>" :
                key=ln[-1]
            elif ln[0]=="<title>" :
                d[key]=" ".join(ln[1:])
            line = file.readline()
    file.close()
    
    return d

if __name__ == "__main__":
    index_name = "a_index"
    es = Elasticsearch(timeout=120)

    # reset_index(es)
    # index_documents("data/collection.tsv", es,index=index_name)

    tv = es.termvectors(index=index_name, id = '2000')
    print(tv['term_vectors']['body']['terms'].keys())
    
