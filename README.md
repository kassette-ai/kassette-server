## Kassette is product data platform built for engineers and data professionals

Kassette provides data pipelines that make it easy to collect data from every application, website and Saas, then activate it in your warehouse and business tools.

With Kassette, you can build product data pipelines that connect your whole data stack and then make them smarter by triggering enrichment and transformations based on analysis in your data warehouse. It's easy-to-use SDKs and event source integrations, Cloud Extract integrations, transformations, data extraction tools and expansive library of destination and warehouse integrations makes building customer data pipelines for both event streaming and cloud-to-warehouse ELT simple.

## Getting Started



### Installation

Kassette is available as a helm chart. You can run it using the following command:

```bash
helm repo add kassette https://kassette-io.github.io/helm-charts

helm install kassette kassette/kassette
```

### Configuration
Configuration is done in app but the application can be scaled

### Usage

### Documentation

![alt text](https://github.com/kassette-ai/kassette-server/blob/main/Kassette-architecture.png)

The Kassette architecture is composed of 2 main components:
Kassette Server
Contains a processor which accepts data, persists to kafka and then forwards to the transformer, get's a response and forwards to the destination.
A transformer which accepts data from the gateway, collects context and then returns to the gateway which places the

Kassette trasformer
This is manages the sources, destinations and reports on health of the feeds


