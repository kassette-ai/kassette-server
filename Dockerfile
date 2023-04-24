FROM scratch
ARG COMPILED_ARTEFACT=main
RUN groupadd -r kassette -g 435 && \
    useradd -u 435 -r -g kassette -s /sbin/nologin -c "Docker image user" kassette


RUN mkdir -p /opt/metaops/kassette-server
WORKDIR /opt/metaops/kassette-server
COPY $COMPILED_ARTEFACT /opt/metaops/kassette-server/kassette-server
RUN chmod 0755 /opt/metaops/kassette-server/kassette-server
RUN chown -R kassette:kassette /opt/metaops/kassette-server/kassette-server
USER kassette
ENTRYPOINT ["/opt/metaops/kassette-server/kassette-server"]


