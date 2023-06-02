FROM ubuntu:18.04


RUN apt-get update
RUN apt-get install ca-certificates curl -y
RUN update-ca-certificates

RUN groupadd -r kassette -g 435
RUN useradd -u 435 -r -g kassette -s /sbin/nologin -c "Docker image user" kassette


RUN mkdir -p /opt/kassette-ai
COPY kassette-server /opt/kassette-ai/

RUN chmod 0755 /opt/kassette-ai/kassette-server
RUN chown -R kassette:kassette /opt/kassette-ai/


USER kassette

WORKDIR /opt/kassette-ai/

ENTRYPOINT ["/opt/kassette-ai/kassette-server"]


