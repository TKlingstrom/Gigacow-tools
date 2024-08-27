# Gigacow Experiments. 
## These experiments are based on the four hypothesis discussed in the article.

To run these experiments, first contact Gigacow's project team to get the latest raw data files. The dataset will be based on the following four files:

- gigacow_<date>.csv
- lactation_<date>.csv
- traffic_<date>.csv

Run the following command to initiate a Docker container. Here we assumes you have a running Docker environment. If you do not have Docker installed and want to install it, follow the official guide: https://docs.docker.com/engine/

```bash
docker run -p 10000:8888 -v <path_to_directory>/Gigacow-tools/projects:<path_to_the_local_directory>/work jupyter/scipy-notebook
```
First run the following notebooks. These notebooks will prepare the datasets based on the raw data files. 

- ts_problematic_detection_L1.ipynb
- ts_problematic_detection_L2.ipynb

For experiments, follow the notebook's naming convention. For example, the code related to experiments 1 and 2 is in the notebook named ``Cow_Detection_L1_Exp_1_2.ipynb``. To set up the time series duration in the code, the instructions are available in the notebooks.
