## Kassette is product data platform built for engineers and data professionals

Kassette provides data pipelines that make it easy to collect data from every application, website and Saas, then activate it in your warehouse and business tools.

With Kassette, you can build product data pipelines that connect your whole data stack and then make them smarter by triggering enrichment and transformations based on analysis in your data warehouse. It's easy-to-use SDKs and event source integrations, Cloud Extract integrations, transformations, data extraction tools and expansive library of destination and warehouse integrations makes building customer data pipelines for both event streaming and cloud-to-warehouse ELT simple.

## Getting Started

Setup a postgres DB with an Admin user and add to .env properties. Then simply run:

```  go run main.go ```

### Installation

Kassette is available as a helm chart. You can run it using the following command:

```bash

helm repo add metaops https://metaops-solutions.github.io/helm-charts

helm install kassette-server metaops/kassette-server 

```

### Run locally using docker-compose
This will create 2 Postgres Databaases: kassette and warehouse + builds and runs kassette-server
kassette-server will run and listen on port 8088
```
docker-compose up
```


### Configuration
Configuration is done in app but the application can be scaled

Configure destionations in the kassette database:

```
INSERT into source_config ( id, source, write_key ) VALUES ( 1, '{ "id": "1", "config": {}, "enabled": true, "writeKey": "write_key", "sourceName": "Camunda-test", "destinations": [ { "ID": "1", "Name": "Camunda-test", "Config": null, "Enabled": true, "RevisionID": "", "WorkspaceID": "", "Transformations": { "ID": "1", "Config": {}, "VersionID": "1" }, "IsProcessorEnabled": true, "DestinationDefinition": { "ID": "1", "Name": "power_bi", "Config": { "endpoint": "https://api.powerbi.com/beta/c4ae8b92-c69e-4f24-a16b-9a034ffa7e79/datasets/5998ed2b-b43c-4281-bdd5-1bc851b2697a/rows?experience=power-bi&key=kQ3YW5PG7ws1fy%2FUU3aau2P8wdeYxhB6yzBfbg6EtJ5WUrix6xgJl%2BRwnzTqooDAncwnc9poWOUijC1CnBue8g%3D%3D", "requestFormat": "ARRAY", "requestMethod": "WAREHOUSE" }, "DisplayName": "Power BI", "ResponseRules": null } } ], "sourceDefinition": { "category": "Workflow", "sourceId": "1", "sourceName": "Camunda" } }', 'write_key');
```

### Releasing

We will use git tags to track the versions 
```
git tag -a v1.0.0 -m "First tagged release of the kassette-server"
git push --tags
```


### Usage

### Documentation

![alt text](https://github.com/kassette-ai/kassette-server/blob/main/Kassette-architecture.png)

The Kassette architecture is composed of 2 main components:
Kassette Server
Contains a processor which accepts data, persists to Postgres in a dataset. This is a job queue which is then picked up by the processor. The processor figures out which transformations and destinations are relevant for the data, performs these actions and maintains a job status table.
A transformer which accepts data from the gateway, collects context and then returns to the gateway which places the

Kassette trasformer
This is manages the sources, destinations and reports on health of the feeds. It's a web portal into the data pipeline.


