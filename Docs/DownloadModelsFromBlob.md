# Download Model Parquet Files to Local Machine

## Use Local Python to Download
If you have a local version of python,
* Run "pip install azure-storage" to install `azure-storage` package.
* Go the project directory,  run 

```python ./Code/download_model.py ./Config/fulldata_storageconfig.json ./Model```.

Then you can see the model files are downloaded to `Model` folder.

## Use Azure Machine Learning (ML) Workbench to Download

Use the following steps to download the model files to `Model` folder

* Navigate to aml_config and open local.runconfig and add "azure-storage" to "pip" section
* Start the commandline and run "az ml experiment submit -t local -c local ./Code/download_model.py"
* By default, the model file are downloaded to "os.environ['AZUREML_NATIVE_SHARE_DIRECTORY']". Use the following to find where the files are:
```
    # on Windows
    C:\users\<username>\.azureml\share\<exp_acct_name>\<workspace_name>\<proj_name>\

    # on macOS
    /Users/<username>/.azureml/share/<exp_acct_name>/<workspace_name>/<proj_name>/
```

* Copy the files from the directory identifed from the previous step to  the Model directory of your project.