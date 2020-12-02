
## Using this code

This code is written for Python 3.7, using Jupyter Notebooks to interact with the outputs of FTOT runs, calculate evenness as a proxy of resiliency, and (eventually) carry out sequential link removal and recalculation of network performance.

The Python dependences are detailed in `environment.yml`. This assumes you have an installation of Python 3.7 and conda. These steps follow [this reference](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file).

From your terminal or Anaconda Prompt, navigate to the location where you cloned this repository and run the following:

```
conda env create -f environment.yml
conda info --envs
```

You should see `geoenv` show up as an available environment. To make this environment usable with Jupyter, you **may** have to run the following:

```
python -m ipykernel install --user --name=geoenv
```
