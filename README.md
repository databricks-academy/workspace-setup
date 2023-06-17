# [Universal] Workspace Setup

## Purpose
This repository contains the configuration files and Notebooks that make up the Universal Workspace Setup. 

## Getting Started
While intended to be used in both notebook/online/remote and "traditional"/offline/local development, this module is specifically structured for local development and as such requires a Conda environment. Instructions to setup the Conda environment are as folllows:
```
conda create -n workspace-setup python=3.9
conda activate workspace-setup
pip install -r requirements.txt
```
Once setup, unit tests can be run by executing
```pytest```