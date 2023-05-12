FROM alpine:3.18.0
ARG COMPILED_ARTEFACT=kassette-server

COPY $COMPILED_ARTEFACT /
chmod 0755 /$COMPILED_ARTEFACT
USER 1001
ENTRYPOINT ["/$COMPILED_ARTEFACT"]


