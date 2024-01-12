# Evaluating Resilience to Heat Stress among Dairy Cows in Sweden 🐄🌡️

**This GitHub repository houses the codebase for a study investigating the impact of Swedish weather conditions, particularly heat, on dairy cows' milk production on Swedish farms. Using weather data sourced from Sveriges meteorologiska och hydrologiska institut (SMHI) and extensive dairy data from the Gigacow project at Sveriges lantbruksuniversitet (SLU).**

## Introduction

This project sutdies the relationship between weather conditions and dairy cow milk production in Swedish farms. The motivation stems from the critical importance of understanding how varying temperatures, specifically heat, influence this aspect of agriculture. By combining data from weather and dairy sources, the study employs a diverse set of mathematical and machine learning techniques. These methods, ranging from normalization techniques to modeling and statistical frameworks, enables a exploration of the dynamics. This GitHub repository serves as a hub for the codebase, providing a foundation for future studies.

## Contributors
### Authors
- [Joakim Svensson](https://www.linkedin.com/in/joakim-svensson1998/)
- [Axel Englund](www.linkedin.com/in/axel-englund-826714183)

### Supervisors
- Lena-Mari Tamminen
- Tomas Klingström
- Martin Johnsson


## Features

- Data preprocessing of dairy and weather data
- Employment of several statistical methods
- ...

## Repo structure
```
HeatStressEvaluation (project-root)/
|-- Data/
|   |-- TheData.csv
|   |
|   |-- CowData/
|   |    |-- CowData_README.md
|   |    |-- GIGACOW/
|   |    |   |-- Cow_filtered.csv
|   |    |   |-- DiagnosisTreatment_filtered.csv
|   |    |   |-- Lactation_filtered.csv
|   |    |   |-- MilkYield_filtered.csv
|   |    |   |-- Robot_filtered.csv
|   |    |
|   |    |-- RawGIGACOW/
|   |        |-- Cow.csv
|   |        |-- DiagnosisTreatment.csv
|   |        |-- Lactation.csv
|   |        |-- MilkYield.csv
|   |        |-- Robot.csv
|   |
|   |-- WeatherData/
|       |-- WeatherData_README.md
|       |-- Coordinates/
|       |   |-- Coordinates.csv
|       |
|       |-- MESAN/
|       |   |-- processed_data_XXXX.csv
|       |   |   ...
|       |   |-- ...
|       |
|       |-- RawMESAN/
|           |-- XXXX_2022-2023.csv
|           |   ...
|           |-- ...
|       
|-- DataPreprocessing/
|   |-- Preprocesses.py
|   |-- DataPreprocessing.ipynb
|
|-- Modeling/
|   |-- Bayesian.py
|   |-- BayesianGAM.ipynb
|   |-- BayesianLinear.ipnyb
|   |-- XXXX
|   |-- XXXX
|   |-- XXXX
|
|-- README.md
|-- requirements.txt
```
## Prerequisites
Before running the scripts, make sure to fulfill the following prerequisites:
### 0. Git Clone
```bash
git clone https://github.com/axeUUeng/HeatStressEvaluation.git
```
And then change into the project directory:
```bash
cd /path/to/HeatStressEvaluation
```
Replace `/path/to/HeatStressEvaluation` with the actual path to the `HeatStressEvaluation` project directory.
### 1. Installation
Follow one of the installation guides for conda
- [Anaconda Installation](https://docs.anaconda.com/free/anaconda/install/index.html)
- [Miniconda Installation](https://docs.conda.io/projects/miniconda/en/latest/)

Then to get the proper enviroment:
```bash
# Conda env installation command
conda create --name your_enviroment_name --file requirements.txt
```
### 2. Datasets

Some datasets are necessary and should be placed in the "Data" folder according to the structure provided above. Ensure the availability of the following datasets and their correct placement:

- `Cow.csv`
- `DiagnosisTreatment.csv`
- `Lactation.csv`
- `MilkYield.csv`
- `Robot_filtered.csv`
- `Coordinates.csv`
- The MESAN files from SMHI
    - `XXXX_2022-2023.csv`
### 2. Preproccess the data
Run the two cells in `DataPreprocessing/DataPreprocessing.ipynb`
### 3. Run the models in `/Modeling/`