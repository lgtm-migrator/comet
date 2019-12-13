#!/bin/sh

# exit when any command fails
set -e

# TODO: switch back to branch develop
git clone https://github.com/kotekan/kotekan.git --branch rn/fixtests --single-branch
cd kotekan/build
cmake -DBOOST_TESTS=ON ..
make dataset_broker_producer dataset_broker_producer2 dataset_broker_consumer

more /proc/cpuinfo | grep flags

cd ../tests
export PYTHONPATH=../python:$PYTHONPATH
pytest -xs test_dataset_broker.py
