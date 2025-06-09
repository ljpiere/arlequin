GEN ?= data-sim/generate.py
CONFIG ?= data-sim/config.yaml

up:                 ## Arranca todo el stack incluido Kafka
	docker compose up -d

data-sim:           ## Genera dataset y lo envía al topic
	python $(GEN) --config $(CONFIG)

down:               ## Apaga y limpia volúmenes
	docker compose down -v
