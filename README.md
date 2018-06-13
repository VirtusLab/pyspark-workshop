# Distributes Big Data Processing workshop

## Local setup
If you are using jupyter available in the cluster then you can skip this setup. It is useful for people wanting to run workshop exercises locally.

1. Install anaconda https://conda.io/docs/user-guide/install/index.html

2. Create conda environment with packages from requirements file
```bash
> Conda create --name pyspark_env --file environment/requirements.txt python=3.5
```
When prompted to install lots of pacakges click Enter to accept.

3. Activate newly created conda environment
```bash
> source activate pyspark_env
```

4. Run jupyter notebook and open workshop exercises
```bash
> jupyter notebook
```


