FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install GDAL + SpatiaLite + SQLite
RUN apt-get update && \
    apt-get install -y \
        gdal-bin \
        sqlite3 \
        libsqlite3-mod-spatialite \
        libgdal-dev \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create working directory
WORKDIR /data

# Copy build script
COPY build_spatial_db.sh /usr/local/bin/build_spatial_db.sh
RUN chmod +x /usr/local/bin/build_spatial_db.sh

ENTRYPOINT ["/usr/local/bin/build_spatial_db.sh"]
