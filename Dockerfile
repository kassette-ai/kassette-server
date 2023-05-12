FROM alpine:3.18.0

COPY kassette-server /
RUN chmod 0755 /kassette-server
RUN mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2

USER 1001
ENTRYPOINT ["/kassette-server"]


