SHELL := /bin/bash

dbt-console: 
	docker-compose run dbt_console

.PHONY: dbt-console