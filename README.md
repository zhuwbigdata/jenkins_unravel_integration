# Unravel CI/CD integration with Jenkins on Databricks

## Reference
https://learn.microsoft.com/en-us/azure/databricks/dev-tools/ci-cd/ci-cd-jenkins

The example uses Runtime 7.3 LTS expiring on 09/2023. Modified for Runtime 10.4 LTS in this case.
## Databricks Runtime 
10.4 LTS with Python 3.8
## Configurations on Jenkins CI server
### OS
Unbutu 20.04.6 LTS
### Jenkins Version
2.401.3
### MINICONDA Version
conda 23.5.2
### Databricks CLI
$ wget https://github.com/databricks/cli/releases/download/v0.202.0/databricks_cli_0.202.0_linux_amd64.zip
### COND ENV CONFIGURATION
Run as user jenkins.
$ conda create -n py3810 python=3.8.10

$ conda activate py3810

$ conda install requests

$ conda install markdown

$ conda install pytest

$ pip3 install --upgrade "databricks-connect==10.4.*"

### Jenkins Pipeline
See pipeline groovy file.

