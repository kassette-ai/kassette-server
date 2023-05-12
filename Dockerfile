FROM alpine:3.18.0

COPY kassette-server /
RUN chmod 0755 /kassette-server
USER 1001
ENTRYPOINT ["/kassette-server"]


