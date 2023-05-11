FROM scratch
ARG COMPILED_ARTEFACT=kassete-agent
RUN groupadd -r kassette -g 435 && \
    useradd -u 435 -r -g kassette -s /sbin/nologin -c "Docker image user" kassette


RUN mkdir -p /opt/metaops/kassette-agent
WORKDIR /opt/metaops/kassette-agent
COPY $COMPILED_ARTEFACT /opt/metaops/kassette-agent/kassette-agent
RUN chmod 0755 /opt/metaops/kassette-agent/kassette-agent
RUN chown -R kassette:kassette /opt/metaops/kassette-agent/kassette-agent
USER kassette
ENTRYPOINT ["/opt/metaops/kassette-agent/kassette-agent"]


