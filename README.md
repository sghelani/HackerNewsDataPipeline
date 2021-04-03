# HackerNewsDataPipeline

## Description
* The Data Pipeline was implemented from scratch using the Directed Acyclic Graph data structure.
* The pipeline extracts, transforms and analyzes stories obtained from the Hacker News [API](https://hn.algolia.com/api).
* The goal of this project was to find out 100 most talked about topics on the Hacker News Platform in year 2014

## Artifacts

* pipeline.py: 
This python file contains the Pipeline class which is implemented as a DAG.

* HackerNewsPipeline.ipynb:
This jupyter notebook contains the code for adding tasks to the pipeline and executing them in the intended order for data analysis.

* stop_words.py:
This file contains the collection of stop words that we want to exclude from the analysis.

* Data:
This folder contains the input data that is processed by the pipeline and the output files created by the individual tasks.
