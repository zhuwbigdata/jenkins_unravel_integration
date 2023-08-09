// Jenkinsfile
node {

  def GITREPO         = "/var/lib/jenkins/workspace/${env.JOB_NAME}"
  def GITREPOREMOTE   = "https://github.com/zhuwbigdata/jenkins_unravel_integration"
  def GITHUBCREDID    = "github-token"
  def CURRENTRELEASE  = "main"
  def DBTOKEN         = "databricks-token"
  def DBURL           = "https://adb-7575549084929882.2.azuredatabricks.net"
  def SCRIPTPATH      = "${GITREPO}/cicd_scripts"
  def NOTEBOOKPATH    = "${GITREPO}/notebooks"
  def LIBRARYPATH     = "${GITREPO}/libraries"
  def BUILDPATH       = "${GITREPO}/Builds/${env.JOB_NAME}-${env.BUILD_NUMBER}"
  def OUTFILEPATH     = "${BUILDPATH}/Validation/Output"
  def TESTRESULTPATH  = "${BUILDPATH}/Validation/reports/junit"
  def WORKSPACEPATH   = "/Shared"
  def DBFSPATH        = "dbfs:/libraries"
  def CLUSTERID       = "0713-162045-22697mg4"
  def CONDAPATH       = "/opt/miniconda"
  def CONDAENV        = "py3810"
  def DBRKSCLI        = "/opt/databricks_cli"

  stage('Setup') {
      withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
        sh """#!/bin/bash
            # Configure Conda environment for deployment & testing
            source ${CONDAPATH}/bin/activate ${CONDAENV}

            # Configure Databricks CLI for deployment
            echo "Configure Databricks CLI"
            rm ~/.databrickscfg*
            echo "$TOKEN" | ${DBRKSCLI}/databricks configure --host "${DBURL}" --token
            # Test
            # ${DBRKSCLI}/databricks fs ls dbfs://

            # Configure Databricks Connect for testing
            echo "Configure Databricks Connect"
            rm ~/.databricks-connect
            echo "y
            ${DBURL}
            $TOKEN
            ${CLUSTERID}
            0
            15001" | databricks-connect configure
            # Test 
            # databricks-connect test
           """
      }
  }
  stage('Checkout') { // for display purposes
    echo "Pulling ${CURRENTRELEASE} Branch from Github at ${GITREPOREMOTE} "
    git branch: CURRENTRELEASE, credentialsId: GITHUBCREDID, url: GITREPOREMOTE
  }
  stage('Run Unit Tests') {
    try {
        sh """#!/bin/bash

              # Enable Conda environment for tests
              source ${CONDAPATH}/bin/activate ${CONDAENV}

              # Python tests for libs
              python3 -m pytest --junit-xml=${TESTRESULTPATH}/TEST-libout.xml ${LIBRARYPATH}/python/dbxdemo/test*.py || true
           """
    } catch(err) {
      step([$class: 'JUnitResultArchiver', testResults: '--junit-xml=${TESTRESULTPATH}/TEST-*.xml'])
      if (currentBuild.result == 'UNSTABLE')
        currentBuild.result = 'FAILURE'
      throw err
    }
  }
  stage('Package') {
    sh """#!/bin/bash

          # Enable Conda environment for tests
          source ${CONDAPATH}/bin/activate ${CONDAENV}

          # Package Python library to wheel
          cd ${LIBRARYPATH}/python/dbxdemo
          python3 setup.py sdist bdist_wheel
       """
  }
  stage('Build Artifact') {
    sh """mkdir -p ${BUILDPATH}/notebooks
          mkdir -p ${BUILDPATH}/libraries/python
          mkdir -p ${BUILDPATH}/Validation/Output
          #Get modified files
          git diff --name-only --diff-filter=AMR HEAD^1 HEAD | xargs -I '{}' cp --parents -r '{}' ${BUILDPATH}

          # Get packaged libs
          find ${LIBRARYPATH} -name '*.whl' | xargs -I '{}' cp '{}' ${BUILDPATH}/libraries/python/

          # Generate artifact
          tar -czvf ${GITREPO}/Builds/latest_build.tar.gz ${BUILDPATH}
       """
    archiveArtifacts artifacts: 'Builds/latest_build.tar.gz'
  }
  stage('Deploy') {
    sh """#!/bin/bash
          # Enable Conda environment for tests
          source ${CONDAPATH}/bin/activate ${CONDAENV}

          # Use Databricks CLI to deploy notebooks
          echo databricks workspace import-dir ${BUILDPATH}/notebooks ${WORKSPACEPATH}
          ${DBRKSCLI}/databricks workspace import-dir ${BUILDPATH}/notebooks ${WORKSPACEPATH}
          
          echo databricks fs cp -r ${BUILDPATH}/libraries/python ${DBFSPATH}
          ${DBRKSCLI}/databricks fs cp -r ${BUILDPATH}/libraries/python ${DBFSPATH}
       """
    withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
        sh """#!/bin/bash
                
              # Enable Conda environment for tests
              source ${CONDAPATH}/bin/activate ${CONDAENV}
              
              #Get space delimited list of libraries
              #MacOS
              LIBS=\$(find ${BUILDPATH}/libraries/python/ -name '*.whl' | sed 's#.*/##' | paste -sd " " -)

              #Script to uninstall, reboot if needed & instsall library
              python3 ${SCRIPTPATH}/installWhlLibrary.py --workspace=${DBURL}\
                        --token=$TOKEN\
                        --clusterid=${CLUSTERID}\
                        --libs=\$LIBS\
                        --dbfspath=${DBFSPATH}
           """
    }
  }
  stage('Run Integration Tests') {
    withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
        sh """#!/bin/bash
                
              
              python3 ${SCRIPTPATH}/executenotebook.py --workspace=${DBURL}\
                        --token=$TOKEN\
                        --clusterid=${CLUSTERID}\
                        --localpath=${NOTEBOOKPATH}\
                        --workspacepath=${WORKSPACEPATH}\
                        --outfilepath=${OUTFILEPATH}
           """
    }
    sh """#!/bin/bash
    
          # Enable Conda environment for tests
          source ${CONDAPATH}/bin/activate ${CONDAENV}
          
          # Replace output path
          sed -i -e 's #ENV# ${OUTFILEPATH} g' ${SCRIPTPATH}/evaluatenotebookruns.py
          python3 -m pytest --junit-xml=${TESTRESULTPATH}/TEST-notebookout.xml ${SCRIPTPATH}/evaluatenotebookruns.py || true
       """
  }
  stage('Report Test Results') {
    sh """find ${OUTFILEPATH} -name '*.json' -exec gzip --verbose {} \\;
          touch ${TESTRESULTPATH}/TEST-*.xml
       """
    junit "**/reports/junit/*.xml"
  }
}
