import os

opendata_dir = os.environ["OPENDATA_DIR"]
output_dir = os.environ["OUTPUT_DIR"]

def list_datasets(opendata_dir):
    for repo in os.listdir(opendata_dir):
        if repo.startswith("."):
            continue
        else:
            repo_dir = os.path.join(opendata_dir, repo)
            if os.path.isdir(repo_dir):
                for dataset in os.listdir(repo_dir):
                    dataset_dir = os.path.join(repo_dir, dataset)
                    if os.path.isdir(dataset_dir):
                        meta_filename = os.path.join(dataset_dir, "metadata.json")
                        if os.path.exists(meta_filename):
                            for f in os.listdir(dataset_dir):
                                if not f == "metadata.json":
                                    f_path = os.path.join(dataset_dir, f)
                                    if os.path.isfile(f_path):
                                        # looks like f is a file
                                        yield dict(repo=repo, ds=dataset, f=f)
                                    else:
                                        # assume that f contains csv files
                                        for ff in os.listdir(f_path):
                                            ff_path = os.path.join(f_path, ff)
                                            if os.path.isfile(ff_path):
                                                yield dict(repo=repo, 
                                                        ds=dataset, 
                                                        f=os.path.join(f, ff))

if __name__ == '__main__':
    from time import time
    start = time()
    count = 0
    with open(os.path.join(output_dir, 'opendata.list'), "w") as output:
        for x in list_datasets(opendata_dir):
            print("%s %s %s" % (x['repo'], x['ds'], x['f']), file=output)
            count += 1
            if count % 1000 == 0:
                dur = time() - start
                print("Found %d data sets in %.2f seconds." % (count, dur))

    dur = time() - start
    print("Found %d data sets in %.2f seconds." % (count, dur))
