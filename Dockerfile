#
# Image with pre-built tools
#
FROM zenithdb/compute-tools:latest AS compute-deps
# Only to get ready apply_conf binary as a dep

#
# Image with Postgres build deps
#
FROM debian:buster-slim AS build-deps

RUN apt-get update && apt-get -yq install automake libtool build-essential bison flex libreadline-dev zlib1g-dev libxml2-dev \
                                          libcurl4-openssl-dev

#
# Image with built Postgres
#
FROM build-deps AS pg-build

# Add user postgres
RUN adduser postgres
RUN mkdir /pg && chown postgres:postgres /pg

# Copy source files
COPY . /pg/

# Build and install Postgres locally
RUN mkdir /pg/compute_build && cd /pg/compute_build && \
    ../configure CFLAGS='-O0 -g3' --prefix=$(pwd)/postgres_bin --enable-debug --enable-cassert --enable-depend && \
    # Install main binaries and contribs
    make MAKELEVEL=0 -j $(getconf _NPROCESSORS_ONLN) -s install && \
    make MAKELEVEL=0 -j $(getconf _NPROCESSORS_ONLN) -s -C contrib/ install && \
    make MAKELEVEL=0 -j $(getconf _NPROCESSORS_ONLN) -s -C contrib/zenith install && \
    # Install headers
    make MAKELEVEL=0 -j $(getconf _NPROCESSORS_ONLN) -s -C src/include install

USER postgres
WORKDIR /pg

#
# Final compute node image to be exported
#
FROM debian:buster-slim

# libreadline-dev is required to run psql
RUN apt-get update && apt-get -yq install openssh-server libreadline-dev && \
    # This will prepare everything needed by sshd
    # like generation host keys with ssh-keygen -A
    service ssh start

# Add user postgres
RUN mkdir /var/db && useradd -m -d /var/db/postgres postgres && \
    echo "postgres:test_console_pass" | chpasswd && \
    mkdir /var/db/postgres/compute && mkdir /var/db/postgres/specs && \
    chown -R postgres:postgres /var/db/postgres/compute && \
    chown -R postgres:postgres /var/db/postgres/specs && \
    chmod 0750 /var/db/postgres/compute

# Copy ready Postgres binaries
COPY --from=pg-build /pg/compute_build/postgres_bin /var/db/postgres/install

# Copy apply_conf binary
COPY --from=compute-deps /usr/local/bin/apply_conf /usr/local/bin/apply_conf

# Copy postgres binaries to the common location
RUN cp /var/db/postgres/install/bin/* /usr/local/bin/ && \
    cp -r /var/db/postgres/install/share/* /usr/local/share/ && \
    # Add postgres shared objects to the search path
    echo '/var/db/postgres/install/lib' >> /etc/ld.so.conf && /sbin/ldconfig

# To be able to run sshd (seems to be default)
# USER root

ENTRYPOINT ["/bin/sh"]
