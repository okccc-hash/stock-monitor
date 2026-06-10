name: Daily Market Monitor

on:
  schedule:
    - cron: '0 13 * * *'
  workflow_dispatch:

jobs:
  run-monitor:
    runs-on: ubuntu-latest

    steps:
    - name: Check out repository code
      uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'

    - name: Install dependencies
      run: |
        pip install yfinance schedule requests

    - name: Run script instantly
      run: |
        python stock_monitor.py --now
