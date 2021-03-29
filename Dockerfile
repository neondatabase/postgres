FROM alpine:3.13

RUN apk add --update gcc libc-dev bison flex readline-dev zlib-dev \
        make diffutils gdb musl-dbg libxml2-dev rust perl curl-dev \
        linux-headers cargo

RUN addgroup zenith && adduser -h /zenith -D -G zenith zenith


COPY ./ /zenith

# XXX: place postgres layer first as it would be changed less frequently.
# Now this is blocked on our repo-layout discussion.


RUN chown -R zenith:zenith /zenith/pageserver
USER zenith
RUN ls -la  /zenith/pageserver/
RUN /zenith/pageserver/pgbuild.sh

RUN cd /zenith/pageserver/ && cargo build
#test --test test_pageserver -- --nocapture
