
# Set the variables
OUTPUT_DIR 	= $(PWD)/output
OPENDATA_LIST	= $(PWD)/output/opencanada.list
# OPENDATA_LIST	= $(PWD)/output/debug.list
OPENDATA_DIR 	= /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only
YAGO_DB 	= /home/kenpu/clones/table-union/sandbox/kenpu/yago.sqlite3

step0:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	python python/setup.py

step1:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run go/src/cmd/build-domain-values.go

step2:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run go/src/cmd/classify-domain-values.go

output/opendata.list:
	cd python; python build-opendata-index.py

clean:
	rm -rf $(PWD)/output/domains
	rm -rf $(PWD)/output/logs
