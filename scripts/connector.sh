curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
 "name": "malicious-connector",
 "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "connection.url": "http://elasticsearch:9200",
    "tasks.max": "1",
    "topics": "malicious-attempt",
    "type.name": "_doc",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.ignore": "true",
    "schema.ignore": "true",
    "value.converter.schemas.enable": "false"
 }
}'
