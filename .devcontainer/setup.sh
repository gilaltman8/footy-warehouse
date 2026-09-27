#!/usr/bin/env bash
set -e
pip install -r requirements.txt || true
curl -sSL https://install.astronomer.io | sudo bash -s
