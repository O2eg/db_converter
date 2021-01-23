.DEFAULT_GOAL := build
.PHONY: build

check-env:
ifndef PG_VERSION
	$(error PG_VERSION is undefined)
endif

default: build

build:
	if test -d db_converter; \
	then echo "db_converter exists"; \
	else git clone https://github.com/masterlee998/db_converter.git; \
	fi

	docker build -t db_converter:dbc_pg${PG_VERSION} --build-arg PG_VERSION=${PG_VERSION} .

.PHONY: clean
clean:
	rm -rf db_converter

.PHONY: prune
prune:
	docker images prune -a
	docker system prune -a -f
