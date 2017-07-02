# Set the variables
OUTPUT_DIR = /home/fnargesian/TABLE_UNION_OUTPUT
OPENDATA_LIST = $(OUTPUT_DIR)/opencanada-en.list
QUERY_LIST = $(OUTPUT_DIR)/cod_queries2.list
QUERY_LIST = $(OUTPUT_DIR)/debug.list
#OPENDATA_LIST = $(OUTPUT_DIR)/debug.list
#QUERY_LIST = $(OUTPUT_DIR)/small.query
QUERY_LIST = $(OUTPUT_DIR)/active_queries.list
OPENDATA_DIR = /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only
COLUMN_UNIONABILITY_TABLE = col_scores
DATASET_UNIONABILITY_TABLE = dataset_scores
MEASURE = sum_cosine
COLUMN_UNIONABILITY_DB = $(OUTPUT_DIR)/column_unionability.sqlite
DATASET_UNIONABILITY_DB = $(OUTPUT_DIR)/dataset_unionability.sqlite
THRESHOLD=0
YAGO_DB = /home/kenpu/TABLE_UNION_OUTPUT/yago.sqlite3.0
EXPERIMENT_DB = /home/fnargesian/TABLE_UNION_OUTPUT/experiments-fixedn.sqlite
EXPERIMENT_DB2 = /home/fnargesian/TABLE_UNION_OUTPUT/jaccard-experiments-fixedn.sqlite
EXPERIMENT_TABLE = fixedn_scores
EXPERIMENT_TABLE2 = fixedn_scores
EXPANSION_DB = /home/fnargesian/TABLE_UNION_OUTPUT/emb-expansion.sqlite
EXPANSION_DB2 = /home/fnargesian/TABLE_UNION_OUTPUT/jaccard-expansion.sqlite 
EXPANSION_TABLE = emb_scores
EXPANSION_TABLE2 = jaccard_scores
SHARED_DB = /home/fnargesian/TABLE_UNION_OUTPUT/union.db
SHARED_TABLE = all_scores
ANNOTATION_DB = /home/fnargesian/TABLE_UNION_OUTPUT/annotation.sqlite
SUBJECT_ANNOTATION_TABLE = subject_labels
ALL_ANNOTATION_TABLE = all_labels
SARMA_TABLE = sarma_results
SARMA_DB = /home/fnargesian/TABLE_UNION_OUTPUT/sarma.sqlite

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
	python3.6 python/setup.py

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

step3:  rmentities 
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/annotate_domains/main.go

step4: 
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/build_domain_embeddings/main.go

step5: 
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/build_domain_minhash/main.go

step6:  
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	JACCARD_SCORES=$(JACCARD_SCORES) \
	CONTAINMENT_SCORES=$(CONTAINMENT_SCORES) \
	COSINE_SCORES=$(COSINE_SCORES) \
	COLUMN_UNIONABILITY_TABLE=$(COLUMN_UNIONABILITY_TABLE) \
	COLUMN_UNIONABILITY_DB=$(COLUMN_UNIONABILITY_DB) \
	QUERY_LIST=$(QUERY_LIST) \
	go run cmd/generate_scores/main.go

od_emb_benchmark_server:  
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	QUERY_LIST=$(QUERY_LIST) \
	go run cmd/opendata_emb_benchmark_server/main.go

od_emb_benchmark_client:  
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	QUERY_LIST=$(QUERY_LIST) \
	go run cmd/opendata_emb_benchmark_client/main.go

od_jaccard_benchmark_server:  
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	QUERY_LIST=$(QUERY_LIST) \
	go run cmd/opendata_jaccard_benchmark_server/main.go

od_jaccard_benchmark_client:  
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	QUERY_LIST=$(QUERY_LIST) \
	go run cmd/opendata_jaccard_benchmark_client/main.go

step7:  
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	COLUMN_UNIONABILITY_TABLE=$(COLUMN_UNIONABILITY_TABLE) \
	DATASET_UNIONABILITY_TABLE=$(DATASET_UNIONABILITY_TABLE) \
	MEASURE=$(MEASURE) \
	COLUMN_UNIONABILITY_DB=$(COLUMN_UNIONABILITY_DB) \
	DATASET_UNIONABILITY_DB=$(DATASET_UNIONABILITY_DB) \
	THRESHOLD=$(THRESHOLD) \
	QUERY_LIST=$(QUERY_LIST) \
	go run cmd/benchmark_unionability/main.go

step8:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	QUERY_LIST=$(QUERY_LIST) \
	ANNOTATION_DB=$(ANNOTATION_DB) \
	SUBJECT_ANNOTATION_TABLE=$(SUBJECT_ANNOTATION_TABLE) \
	ALL_ANNOTATION_TABLE=$(ALL_ANNOTATION_TABLE) \
	go run cmd/annotate_subject_columns/main.go

benchmark_sarma:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	QUERY_LIST=$(QUERY_LIST) \
	ANNOTATION_DB=$(ANNOTATION_DB) \
	SUBJECT_ANNOTATION_TABLE=$(SUBJECT_ANNOTATION_TABLE) \
	ALL_ANNOTATION_TABLE=$(ALL_ANNOTATION_TABLE) \
	SARMA_DB=$(SARMA_DB) \
	SARMA_TABLE=$(SARMA_TABLE) \
	go run cmd/benchmark_sarma/main.go

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
	#go run cmd/opendataclient/main.go -query /home/fnargesian/TABLE_UNION_OUTPUT/domains/open.canada.ca_data_en.jsonl/49fbab13-1a5a-4fed-8ca5-ce6e4d92576d/440423b9-d4ee-427f-ad47-af7a1a630cbe/2.values -result-dir result

union_server:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/unionserver/main.go

jaccard_server:
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	go run cmd/jaccardserver/main.go

union_client:         
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	#go run cmd/unionclient/main.go -query /home/ekzhu/WIKI_TABLE/q1/query.csv -result-dir result 
	#go run cmd/unionclient/main.go -query /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/open.canada.ca_data_en.jsonl/c702301b-235c-4e7c-8513-11a2aaa6d226/a8e225fb-5c71-497e-a8a6-77046200dde6____0/03270006-eng.csv -result-dir result
	#go run cmd/unionclient/main.go -query /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/open.canada.ca_data_en.jsonl/a9d4039f-0ef8-47a6-85ba-a19127765ce5/d57d2990-98ae-48c1-b89e-c5467fb76bfc____0/01530147-eng.csv
	#go run cmd/unionclient/main.go -query /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/open.canada.ca_data_en.jsonl/b75af755-b08b-4235-b259-41856217c652/8f369ac9-ce02-43bf-bc57-2a67c3402e1f____2/Fiche_Technique-20161026.txt -result-dir result
	#go run cmd/unionclient/main.go -query /home/fnargesian/OPENDATA/queries/NSERC_GRT_FYR2011_AWARD.csv -result-dir result
	go run cmd/unionclient/main.go -query /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/open.canada.ca_data_en.jsonl/c1b0f627-8c29-427c-ab73-33968ad9176e/252f152f-a97a-435d-a15d-14c70de139fc -result-dir result

jaccard_client:         
	OPENDATA_DIR=$(OPENDATA_DIR) \
	OPENDATA_LIST=$(OPENDATA_LIST) \
	YAGO_DB=$(YAGO_DB) \
	OUTPUT_DIR=$(OUTPUT_DIR) \
	#go run cmd/unionclient/main.go -query /home/ekzhu/WIKI_TABLE/q1/query.csv -result-dir result 
	#go run cmd/unionclient/main.go -query /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/open.canada.ca_data_en.jsonl/c702301b-235c-4e7c-8513-11a2aaa6d226/a8e225fb-5c71-497e-a8a6-77046200dde6____0/03270006-eng.csv -result-dir result
	#go run cmd/unionclient/main.go -query /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/open.canada.ca_data_en.jsonl/a9d4039f-0ef8-47a6-85ba-a19127765ce5/d57d2990-98ae-48c1-b89e-c5467fb76bfc____0/01530147-eng.csv
	go run cmd/unionclient/main.go -query /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/open.canada.ca_data_en.jsonl/b75af755-b08b-4235-b259-41856217c652/8f369ac9-ce02-43bf-bc57-2a67c3402e1f____2/Fiche_Technique-20161026.txt -result-dir result
	#go run cmd/unionclient/main.go -query /home/fnargesian/OPENDATA/queries/NSERC_GRT_FYR2011_AWARD.csv -result-dir result
	#go run cmd/jaccardclient/main.go -query /home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/open.canada.ca_data_en.jsonl/c1b0f627-8c29-427c-ab73-33968ad9176e/252f152f-a97a-435d-a15d-14c70de139fc -result-dir result

expansion:
	EXPERIMENT_DB=$(EXPERIMENT_DB) \
	EXPERIMENT_TABLE=$(EXPERIMENT_TABLE) \
	EXPERIMENT_DB2=$(EXPERIMENT_DB2) \
	EXPERIMENT_TABLE2=$(EXPERIMENT_TABLE2) \
	EXPANSION_DB=$(EXPANSION_DB) \
	EXPANSION_TABLE=$(EXPANSION_TABLE) \
	EXPANSION_DB2=$(EXPANSION_DB2) \
	EXPANSION_TABLE2=$(EXPANSION_TABLE2) \
	SHARED_DB=$(SHARED_DB) \
	SHARED_TABLE=$(SHARED_TABLE) \
	OPENDATA_DIR=$(OPENDATA_DIR) \
	go run cmd/compute_union_expansion/main.go

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

rmminhashes:
	rm -f `find $(OUTPUT_DIR)/domains -name "*.minhash"`

clean:
	rm -rf $(OUTPUT_DIR)/logs
