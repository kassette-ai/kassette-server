FROM scratch
ARG COMPILED_ARTEFACT=kassete-server

COPY $COMPILED_ARTEFACT /
USER 1001
ENTRYPOINT ["/kassette-server"]


