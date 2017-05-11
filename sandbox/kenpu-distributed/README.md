# Generating the data file list

```
export OPENDATA_DIR=...
export OUTPUT_DIR=...

python python/build-opendata-index.py
```

This will save the output to `$OUTPUT_DIR/opendata.list`


## Generating the canadian open data list

```
cd $OUTPUT_DIR
grep open.canada opendata.list > opencanada.list
```

# Step 0: Create the holding directories

```
export OPENDATA_LIST=...
export OUTPUT_DIR=...
python python/setup.py
```

# Step 1: Generate the domain values


