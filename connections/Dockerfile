FROM ubuntu:18.04


RUN groupadd -r kassette -g 435
RUN useradd -u 435 -r -g kassette -s /sbin/nologin -c "Docker image user" kassette


RUN mkdir -p /opt/kassette-ai
COPY kassette-agent /opt/kassette-ai/

RUN chmod 0755 /opt/kassette-ai/kassette-agent
RUN chown -R kassette:kassette /opt/kassette-ai/


USER kassette

WORKDIR /opt/kassette-ai/

ENTRYPOINT ["/opt/kassette-ai/kassette-agent"]


