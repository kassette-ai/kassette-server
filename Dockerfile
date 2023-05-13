FROM ubuntu:18.04

COPY kassette-server /
RUN chmod 0755 /kassette-server

USER 1001
ENTRYPOINT ["/kassette-server"]


