FROM alpine:3.18.0
ARG COMPILED_ARTEFACT=kassette-server
ENV EXEC_FILE="/${COMPILED_ARTEFACT}"
COPY ${COMPILED_ARTEFACT} /
RUN chmod 0755 /${COMPILED_ARTEFACT}

ENTRYPOINT ["${EXEC_FILE}"]


