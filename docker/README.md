# db_converter Dockerfile

## Usage

Make image:

```bash
cd db_converter/docker
make PG_VERSION=13
```

Push image:

```bash
docker tag $(docker images -q | head -n 1) USER/db_converter:dbc_pg13 && \
docker push USER/db_converter:dbc_pg13
```

## Run container

```bash
docker pull masterlee998/db_converter:dbc_pg13
docker run --name dbc -d masterlee998/db_converter:dbc_pg13
docker exec -it dbc bash
```