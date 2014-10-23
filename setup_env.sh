#!/bin/sh

echo 'Installing python-pip'
sudo apt-get install python-pip

echo 'Installing virtualenv'
sudo pip install virtualenv

virtualenv env

echo 'Installing libraries'
env/bin/python env/bin/pip install -r requirements.txt
