FROM scratch
ARG COMPILED_ARTEFACT=kassette-server

COPY $COMPILED_ARTEFACT /
USER 1001
ENTRYPOINT ["/kassette-server"]


