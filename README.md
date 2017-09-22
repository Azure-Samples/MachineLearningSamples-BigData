
# Server Workload Forecasting on Terabytes Data

## Link to the Microsoft DOCS site

The detailed documentation for this real world scenario includes the step-by-step walkthrough:
[https://review.docs.microsoft.com/en-us/azure/machine-learning/preview/scenario-big-data](https://review.docs.microsoft.com/en-us/azure/machine-learning/preview/scenario-aerial-big-data)

## Link to the Gallery GitHub repository

The public GitHub repository for this real world scenario contains all the code samples:
[https://github.com/Azure/MachineLearningSamples-BigData](https://github.com/Azure/MachineLearningSamples-BigData)

## Overview

Forecasting workload on servers is a common business need for technology companies that manage their own infrastructure. A key challenge in forecasting the workload on servers is the huge amount of data. In this scenario, we use 1-TB synthesized data to demonstrate how data scientists can use Azure ML Workbench to develop solutions that require use of big data. We show how a user by using Azure ML Workbench can follow a happy path of starting from a sample of a large dataset, iterating through data preparation, feature engineering and machine learning, and then eventually extending the process to the entire large dataset.


## Key components needed to run this scenario

* An [Azure account](https://azure.microsoft.com/free/) (free trials are available)
* An installed copy of [Azure Machine Learning Workbench] and a workspace.
* A Data Science Virtual Machine (DSVM) for Linux (Ubuntu).  We recommend using a virtual machine with at least 8 cores and 32 GB of memory.  You need the DSVM IP address, user name, and password to try out this example. You can choose to use any virtual machine (VM) with [Docker Engine](https://docs.docker.com/engine/) installed.
* A HDInsight Spark Cluster with HDP version 3.6 and Spark version 2.1.x.  We recommend using a three-worker cluster with each worker having 16 cores and 112 GB of memory. Or you can just choose VM type "`D12 V2`" for head node and "`D14 V2`" for the worker node. The deployment of the cluster takes around 20 minutes. You need the cluster name, SSH user name, and password to try out this example.
* An Azure Storage account. You can follow the [instructions](https://docs.microsoft.com/azure/storage/common/storage-create-storage-account) to create an Azure storage account. Also, create two private Blob containers with name "`fullmodel`" and "`onemonthmodel`" in this storage account. The storage account is used to save intermediate compute results and machine learning models. You need the storage account name and access key to try out this example. 


## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (for example, label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information, see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.





