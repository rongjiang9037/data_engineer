#!/bin/bash

echo "Welcome to the data engineer project!"
echo "Setting up AWS architecture needed for this project..."
## upload data to S3
python aws/S3.py

## create VPC, subnets, EC2 instance
python aws/vpc_ec2.py



