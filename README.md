# pySpark for Data Science Workshop
## Local setup
NOTE: If you are using jupyter available in the cluster you can skip this setup. It is useful for people wanting to run workshop exercises locally.

1. Install anaconda  https://conda.io/projects/conda/en/latest/user-guide/install/index.html#id2

2. Create conda environment with packages from requirements file
```bash
> conda create -y --name pyspark_env --file environment/requirements.txt
```

3. Activate newly created conda environment
```bash
> source activate pyspark_env
```

4. Run jupyter notebook
```bash
> jupyter notebook
```

5. Open notebook with exercises
```
pySpark SQL exercises.ipynb
```

## Authors
Mikołaj Kromka
Grzegorz Gawron
