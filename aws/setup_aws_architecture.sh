#!/bin/bash

echo "Welcome to the data engineer project!"
echo "Setting up AWS architecture needed for this project..."

## upload data to S3
python S3.py

## create VPC, subnets, EC2 instance
python vpc_ec2.py
