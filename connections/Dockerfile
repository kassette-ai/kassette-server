FROM scratch
ARG COMPILED_ARTEFACT=kassete-agent

RUN mkdir -p /opt/metaops/kassette-agent
WORKDIR /opt/metaops/kassette-agent
COPY $COMPILED_ARTEFACT /opt/metaops/kassette-agent/kassette-agent
RUN chmod 0755 /opt/metaops/kassette-agent/kassette-agent
RUN chown 1001 /opt/metaops/kassette-agent/kassette-agent
USER 1001
ENTRYPOINT ["/opt/metaops/kassette-agent/kassette-agent"]


