curl -s -XPUT "http://localhost:9200/_template/malicious-attempt/" -H 'Content-Type: application/json' -d'
    {
      "template": "*",
      "mappings": { "dynamic_templates": [ { "dates": { "match": "time", "mapping": { "type": "date" } } } ]  }
    }'
