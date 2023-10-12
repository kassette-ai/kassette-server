### postgres2postgres docker-compose example combines the following:

## postgres
Postgres DataBase instance which has multiple schemas:
kassette - Kassete configuration and operational Database
warehouse - Database used for simulating a destination Postgres DB used for exporting Camunda history. 

## kassette-server
Kassette main component responsible for processing, transformation and submittion of the data pulled from Source. API runs on localhost:8088

## kassette-transformer
UI Admin interface allowing to configure kassette-server logic. Can be accessed via http://localhost:3000

## kassette-postgres-agent
Kassette Postgres Agent which runs SQL query provided in configfile on schedule and send the data to kassette-server.
As an example it send the content of service_catalogue table and picks several fields from it dropping everything else. 

## grafana
Configured instance of grafana which will show simple table displaying the content of a Postgres Table captured from the SRC Database. Can be accessed via http://localhost:3001

