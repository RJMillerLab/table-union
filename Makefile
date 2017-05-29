
# Set the variables
OUTPUT_DIR 	= /home/fnargesian/TABLE_UNION_OUTPUT
OPENDATA_LIST	= $(OUTPUT_DIR)/opencanada-en.list
#OPENDATA_LIST	= $(OUTPUT_DIR)/debug.list
OPENDATA_DIR 	= /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only
YAGO_DB 	= $(OUTPUT_DIR)/yago.sqlite3.0

now: step4

all:
	@echo "make clean rmtypes step0 step1 step2 ..."
	@echo "OUTPUT_DIR=" $(OUTPUT_DIR)
	@echo "OPENDATA_LIST=" $(OPENDATA_LIST)

build:
	go build ./...

install:
	go install ./...


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
	go run cmd/build_domain_values/main.go

step2: rmtypes
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/classify_domain_values/main.go

step3: rmentities
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/annotate_domains/main.go

step4: rmftsum
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/build_domain_embeddings/main.go

count_domains:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/count_domain_segments/main.go

odserver:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/opendataserver/main.go

odclient:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/opendataclient/main.go -query /home/ekzhu/WIKI_TABLE/q1/query.csv -result-dir result 


output/opendata.list:
	cd python; python build-opendata-index.py

rmtypes:
	rm -f `find $(OUTPUT_DIR)/domains -name "types"`

rmftsum:
	rm -f `find $(OUTPUT_DIR)/domains -name "*.ft-sum"`

rmentities:
	rm -f `find $(OUTPUT_DIR)/domains -name "*.entities"`

rmsamples:
	rm -f `find $(OUTPUT_DIR)/domains -name "*.samples"`

clean:
	rm -rf $(OUTPUT_DIR)/domains
	rm -rf $(OUTPUT_DIR)/logs
