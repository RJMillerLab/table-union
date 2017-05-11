
# Set the variables
OUTPUT_DIR 	= $(PWD)/output
OPENDATA_LIST	= $(PWD)/output/opencanada.list
OPENDATA_LIST	= $(PWD)/output/debug.list
OPENDATA_DIR 	= /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only
YAGO_DB 	= /home/kenpu/clones/table-union/sandbox/kenpu-build-yago/yago.sqlite3

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

step2:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run go/src/cmd/classify-domain-values.go

step3:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run go/src/cmd/annotate-domains.go


output/opendata.list:
	cd python; python build-opendata-index.py

rmtypes:
	rm `find $(PWD)/output/domains -name "types"`

clean:
	rm -rf $(PWD)/output/domains
	rm -rf $(PWD)/output/logs
