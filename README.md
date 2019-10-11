# ActiveTools
A data pipeline tool for software developers to explore trends in actively and collectively used programming tools for different tasks.

# Motivation
One of the most first important decisions to make when developing in a new software application is which programming tools to use. With new tools being developed everyday and classical tools continuously improving, it can be overwhelming to choose a set of tools since many of them perform the same task. It can cost a company $15K to $700K to remediate a problem that occurs when a company chooses the wrong tool. To make this decision easy, I have built ActiveTools. 

ActiveTools is a dashboard that shows trends on actively and collectively used programming tools for different tasks so that you chose the right tools for your project. In general, tools that have a large user base have good community support, are easy to use and tools often used together are compatible with each other. 

# Dataset

In July 2017, a team in the Design Lab at UC San Diego queried, downloaded, and analyzed approximately 1.25 million Jupyter Notebooks in public repositories on GitHub. By their calculation this was about 95% of all Jupyter Notebooks publicly available on GitHub at the time. This dataset includes: \
~1.25 million Jupyter Notebooks \
Metadata about each notebook \
Metadata about each of the nearly 200,000 public repositories that contained a Jupyter Notebook \
Top level README files for nearly 150,000 repositories containing a Jupyter Notebook

In addition to this core data, these data include: \
A smaller, starter dataset with 1000 randomly selected repositories containing ~6000 notebooks \
CSV files summarizing and indexing the notebooks, repositories, and READMEs \
Log files documenting when each file was downloaded \
Scripts for our initial analysis of the dataset 

More Information on the [dataset](https://library.ucsd.edu/dc/object/bb2733859v) can be obtained in the following publication:
Rule, Adam; Tabard, Aurélien; Hollan, James D. (2018). Data from: Exploration and Explanation in Computational Notebooks. UC San Diego Library Digital Collections.


# Tech Stack
![Tech stack used in ActeveTools](https://github.com/gauravsm31/ActiveTools/blob/master/Image/TechStack.png)
The following tools were used for building the pipeline: \
An Amazon S3 bucket was used to host the Jupyter Notebooks that have a “notebook (.ipynb)” format, mapping of Notebooks to it’s repository in a “csv” format and metadata on repositories in “json” format. \
This data was processed in parallel using an Apache Spark cluster having four nodes. \
The processed data for each library was stored in a postgres database and display trends on actively used tools using Dash. \
If there is a new library whose trends need to be analysed, it can be added the list of libraries in a csv file on Amazon S3 and the pipeline will generate a new table for that library in the database.

# Dashboard
Please look at the [ActiveTools website](http://www.activetools.xyz) to view the processed data. 
