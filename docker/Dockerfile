FROM ubuntu:20.04

ARG PG_VERSION
ARG DEBIAN_FRONTEND=noninteractive

# Install PostgreSQL
RUN apt-get update
RUN apt-get install -y wget ca-certificates gnupg2
RUN echo "deb http://apt.postgresql.org/pub/repos/apt focal-pgdg main" >> /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update && apt-get --yes remove postgresql\*
RUN apt-get -y install postgresql-${PG_VERSION} postgresql-client-${PG_VERSION}

# Configure locale
RUN apt-get install -y locales
RUN locale-gen en_US.UTF-8

# Install Python modules
RUN apt -y install software-properties-common
RUN apt -y install python3-pip
RUN pip3 install sqlparse && \
	pip3 install requests && \
	pip3 install pyzipper && \
	pip3 install coverage && \
	pip3 install coveralls

# Add db_converter
ADD ./db_converter /usr/share/db_converter

# Reduce images size
RUN rm -rf /tmp/*
RUN apt-get purge -y --auto-remove
RUN apt-get clean -y autoclean
RUN rm -rf /var/lib/apt/lists/*

EXPOSE 5432

ENV PG_VERSION=${PG_VERSION}

ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ADD motd /etc/motd

WORKDIR /usr/share/db_converter

ENTRYPOINT exec /entrypoint.sh
