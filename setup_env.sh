#!/bin/sh

echo 'Installing python-pip'
sudo apt-get install python-pip

echo 'Installing virtualenv'
sudo pip install virtualenv

virtualenv venv

source venv/bin/activate

echo 'Installing libraries'
pip install -r requirements.txt

deactivate