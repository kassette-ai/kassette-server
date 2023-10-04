### camunda2postgres docker-compose example combines the following:

## camunda-diy 
Sample java App for simulating fast-food restaurant ordering process. It uses Camunda Workflow which includes 3 process definitions
When application starts Camunda Admin page can be accessed in a web browser via http://localhost:8090 with username/password set to default demo/demo

## camunda-poker
shell script simulating restaurant customer flow and triggering various events via Camunda Rest API

## postgres
Postgres DataBase instance which has multiple schemas:
kassette - Kassete configuration and operational Database
workflow - Database used by the Camunda application "camunda-diy" for storing camunda internals and history
warehouse - Database used for simulating a destination Postgres DB used for exporting Camunda history. 

## kassette-server
Kassette main component responsible for processing, transformation and submittion of the data pulled from Source. API runs on localhost:8088

## kassette-transformer
UI Admin interface allowing to configure kassette-server logic. Can be accessed via http://localhost:3000

## kassette-camunda-agent
Kassette Camunda Agent which polls Camunda Database waiting for new historical events and submits them to the kassette-server.

## grafana
Configured instance of grafana which will show simple graph analysing the Data captured from Camunda events

