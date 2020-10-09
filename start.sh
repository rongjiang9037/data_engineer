#!/bin/bash


echo "Installing gcc..."
sudo yum -y install gcc

## create conda environment
echo "Updating conda..."
conda update -n base -c defaults conda

export EVN_NAME=immigration_demographics_env
echo "Creating conda environment: $ENV_NAME"
conda env create -f /home/ec2-user/data_engineer/env.yml

## activate environment
echo "Activating environment: $EVN_NAME"
## export conda functions
export CONDAPATH=$(conda info --base)
source $CONDAPATH/etc/profile.d/conda.sh

conda activate immigration_demographics_env
echo "Conda environment $EVN_NAME is ready"

exit 0
