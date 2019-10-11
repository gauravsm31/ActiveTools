# ActiveTools
A data pipeline tool for programmers to explore trends in actively used and collocated libraries in various application categories.

# Motivation
One of the most important initial decisions to make when developing in a new software application is which programming tools to use. With new tools being developed everyday and classical tools continuously improving, it can be overwhelming to choose a tool since many of them perform the same task. It can cost a company $15K to $700K to remediate a problem that occurs when a company chooses the wrong software. To make this decision easy, I have built ActiveTools. 

In general, tools that have a large user base have good community support, are easy to use and compatible with other tools. ActiveTools is a dashboard that shows trends on actively used and collocated libraries for different tasks so that you chose the right tools for your project. 

# Dataset

In July 2017, a team in Design Lab at UC San Diego queried, downloaded, and analyzed approximately 1.25 million Jupyter Notebooks in public repositories on GitHub. By their calculation this was about 95% of all Jupyter Notebooks publicly available on GitHub at the time. This dataset includes:
~1.25 million Jupyter Notebooks
Metadata about each notebook
Metadata about each of the nearly 200,000 public repositories that contained a Jupyter Notebook
Top level README files for nearly 150,000 repositories containing a Jupyter Notebook

In addition to this core data, these data include:
A smaller, starter dataset with 1000 randomly selected repositories containing ~6000 notebooks
CSV files summarizing and indexing the notebooks, repositories, and READMEs
Log files documenting when each file was downloaded
Scripts for our initial analysis of the dataset 

More Information on the [dataset](https://library.ucsd.edu/dc/object/bb2733859v) can be obtained in the following publication:
Rule, Adam; Tabard, Aurélien; Hollan, James D. (2018). Data from: Exploration and Explanation in Computational Notebooks. UC San Diego Library Digital Collections.


# Tech Stack
![Tech stack used in ActeveTools](https://github.com/gauravsm31/ActiveTools/blob/master/Image/TechStack.png)
I used the following tools for building the pipeline. I used an Amazon S3 bucket to host the Jupyter Notebooks that have a “notebook (.ipynb)” format, mapping of Notebooks to it’s repository in a “csv” format and metadata on repositories in “json” format. I process this data in parallel using an Apache Spark cluster having four nodes. I store the processed data for each library in a postgres database and display trends on actively used tools using Dash. If there is a new library that whose trends you want to analyse, you can add the name of the library in a csv file on Amazon S3 and the pipeline will generate a new table for that library in the database.

# Dashboard
Please look at the [ActeveTools website](www.activetools.xyz) to view the processed data. 
