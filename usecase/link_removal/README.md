


## Using this code

This code is written for Python 3.x, using Jupyter Notebooks to interact with the outputs of FTOT runs, calculate evenness as a proxy of resiliency, and carry out sequential link removal and recalculation of network performance.

The Python dependences are detailed in `environment.yml`. This assumes you have an installation of Python 3.x and conda. These steps follow [this reference](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file).

From your terminal or Anaconda Prompt, navigate to the location where you cloned this repository and run the following:

```
conda env create -f environment.yml
```

You should see `FTOTenv` show up as an available environment, when checking your environments with:

```
conda info --envs
```

You can then launch Jupyter Notebook by the following steps:

```
conda activate FTOTenv
jupyter notebook
```

## Modify FTOT code

Two files in the FTOT code need to be modified for this use case. They are as follows:

- `ftot_networkx.py`
	+ Modified to keep temporary shapefiles. This adds size to the output of each scenario, but allows us to make modifications to the shapefiles
- `ftot_routing.py`
	+ Modified to not add back in interstates to the road network. Without this step, an analysis of the road network would always include the national interstate system.
	+ Also modified to use only a 50 mile buffer around the selected area. This makes the scenario analysis more specific to the local network.

Copy these files from the location of this file to the location of your FTOT installation, which likely is `C:\FTOT\program`.

## Run quick start scenario

Run quick start 7 now, using the modified FTOT code.
Browse to `C:\FTOT\scenarios\quick_start\qs7_rmp_proc_dest_multi_inputs` and double-click `run_v5_1.bat`.

## Conduct evenness calculation and link removal disruptions

Browse to Calcualte Evenness.ipynb to begin this module.
