
# Set the variables
OUTPUT_DIR 	= $(PWD)/output
OPENDATA_LIST	= $(PWD)/output/opencanada.list
OPENDATA_DIR 	= /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only
YAGO_DB 	= /home/kenpu/clones/table-union/sandbox/kenpu/yago.sqlite3

# sets the environment, and start the go annotation
all:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(PWD)/output/debug.list \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run go/src/cmd/main.go 

output/opendata.list:
	cd python; python build-opendata-index.py

clean:
	rm -rf $(PWD)/output/domains

