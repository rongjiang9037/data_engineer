#!/bin/bash


## create conda environment
export EVN_NAME=immigration_demographics_env
echo "Creating conda environment: $ENV_NAME"
conda env create -f /home/ec2-user/data_engineer/env.yml

## activate environment
echo "Activating environment: $EVN_NAME"
conda activate immigration_demographics_env
echo "Conda environment $EVN_NAME is ready"

## install airflow
echo "Installing Apache Airflow..."
pip install -r requirments.txt


