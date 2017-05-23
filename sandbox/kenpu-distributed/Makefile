
# Set the variables
OUTPUT_DIR 	= $(PWD)/output
OPENDATA_LIST	= $(PWD)/output/opencanada-en.list
# OPENDATA_LIST	= $(PWD)/output/debug.list
OPENDATA_DIR 	= /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only
YAGO_DB 	= /home/kenpu/clones/table-union/sandbox/kenpu-build-yago/yago.sqlite3.0

all:
	@echo "make clean rmtypes step0 step1 step2 ..."
	@echo $(OUTPUT_DIR)
	@echo $(OPENDATA_LIST)

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

step2: rmtypes
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run go/src/cmd/classify-domain-values.go

step3: rmentities
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run go/src/cmd/annotate-domains.go

count_domains:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run go/src/cmd/count_domain_segments.go



output/opendata.list:
	cd python; python build-opendata-index.py

rmtypes:
	rm -f `find $(PWD)/output/domains -name "types"`

rmentities:
	rm -f `find $(PWD)/output/domains -name "*.entities"`

clean:
	rm -rf $(PWD)/output/domains
	rm -rf $(PWD)/output/logs
