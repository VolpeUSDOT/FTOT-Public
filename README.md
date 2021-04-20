# Forest Residuals-to-Jet Fuel Supply Chain Documentation (2021.1 Version)


Created by WSU, 
Jie Zhao (Graduate Research Assistant, Washington State University, Pullman, WA 99164, USA, E-mail: jie.zhao2@wsu.edu);
Ji Yun Lee (Assistant Professor, Washington State University, Pullman, WA 99164, USA, E-mail: jiyun.lee@wsu.edu)

## Overview: 
Supply chain resilience assessment includes two parts: integrated risk assessment to capture the combined effects of multiple risk factors on supply chain performance, and resilience assessment to calculate the long-term supply chain resilience in planning horizon. 
The purpose of this documentation is to illustrate how to run the Forest Residuals-to-Jet Fuel supply chain resilience assessment based on FTOT optimization and several outputs from risk assessment. 

## Getting Started:
(a) Forest Residuals-to-Jet Fuel supply chain scenario is stored C:\FTOT_SCR\scenarios\ForestResiduals_SCR folder. 
(b) Download the common_data folder from the FTOT dataset and save it into C:\FTOT_SCR/scenarios folder.
(c) ArcGIS 64‐bit background geoprocessing is required to run this scenario due to the large computational burden.
(d) The batch script file (run_v5_ 1.bat) is included in the ForestResiduals_SCR folder to automate each step required in FTOT optimization.

## ForestResiduals_SCR folder includes:
(a) scenario configuration file (scenario.XML): to define the locations of the files and parameter values used in the run.
(b) the batch script file (run_v5_ 1.bat): to execute FTOT run in a sequence of steps.
(c) input_data folder: to record facility-related data and each facility type has a separate CSV file.
(d) input_GISdata folder: to contain a facilities.gdb file.
(e) 1- to 6- Python files: to simulate the effects of risk factors on supply chain performance (risk assessment).
(f) CSV and TXT files: to present all inputs data into 1- to 6- Python files for risk assessment. 

## Run Step:
(a) Go to FTOT_SCR\scenarios\ForestResiduals_SCR folder.
(b) Risk assessment: run 1- to 6- Python files in sequence, and then the outputs can be used into modified FTOT optimization run.
(c) Resilience assessment: run the batch script by double-clicking it or manually executing it in the Command Prompt.

## Run Scenario:
(a) Informational logging is available in the ForestResiduals_SCR\logs folder.
(b) Optimization steps would be iterated for each scenario and time period. 
(c) Each optimization should run take about 10 minutes, the total simulation will take about 20 days.

## Results:
(a) Final result (supply chain resilience for every scenario) is save into a NPY file, called Resilience.npy.
(b) Related results, such as resilience components, weight factors for each component, are saved in associated NPY files.
