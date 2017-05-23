all:
	sqlite3 yago.sqlite3.0 < word-entity.sql > word-entity.txt
	mv word-entity.txt ../kenpu-distributed/output

build-word-index:
	python build-word-index.py yago.sqlite3.0
	sqlite3 yago.sqlite3.0 < word-count.sql

all0:
	@python summarize_domains.py 466352 1 925424 1
	@python summarize_domains.py 1005971 1 1004428 1
	@python summarize_domains.py 1005971 1 466352 1

debug1:
	ipython -i summarize_domains.py 466352 1 925424 1

debug2:
	ipython -i summarize_domains.py 1005971 1 1004428 1


sarma:
	@python test_sarma.py 466352 1 925424 1
	@python test_sarma.py 1005971 1 1004428 1
	@python test_sarma.py 1005971 1 466352 1
