#!/bin/sh

echo 'Installing python-pip'
sudo apt-get install python-pip

echo 'Installing virtualenv'
sudo pip install virtualenv

virtualenv venv

echo 'Installing libraries'
venv/bin/python venv/bin/pip install -r requirements.txt
