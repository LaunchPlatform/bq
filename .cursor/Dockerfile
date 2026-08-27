FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# System toolchain: git/curl for checkout & downloads, build-essential for any
# native wheels (e.g. psycopg2), sudo so the ubuntu user can manage the local
# PostgreSQL service, and PostgreSQL 16 which BeanQueue depends on.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        build-essential \
        sudo \
        postgresql-16 \
        postgresql-contrib \
    && rm -rf /var/lib/apt/lists/*

# The ubuntu user (uid 1000) ships with the base image. Grant passwordless sudo
# so install/start scripts can start the PostgreSQL cluster.
RUN echo "ubuntu ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/ubuntu \
    && chmod 0440 /etc/sudoers.d/ubuntu

USER ubuntu
WORKDIR /home/ubuntu

# uv manages the Python toolchain and project dependencies.
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/home/ubuntu/.local/bin:${PATH}"
