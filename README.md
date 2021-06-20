# Nginx log parser

<h5>
Log parser is a simple application to parse nginx log files.
By default script is trying to find last log file with max date in ./log directory and write report to ./report directory.
If report exists it will not process log file.
Result of the script will be report.html where you can find statistic about queries with maximum time_sum values.
</h5>

##### Supported log file formats are *plain-text* and *gz*.
##### You can configure script parameters through log_parser.cfg or cli. Parameters from cli are prioritized.
##### Log file format described in ```parsers.py```

#### To get help run ```python2 log_parser.py -h```

#### To run tests ```python2 test_file_utils.py```

### Query statistic fields for top urls:
* **count** - absolute query counter
* **count_perc** - query counter compare to summary parsed queries counter
* **time_sum** - summary query *$request_time*
* **time_perc** - summary query *$request_time* compare to summary parsed queries time
* **time_avg** - average query *$request_time*
* **time_max** - max query *$request_time* value
* **time_med** - median query *$request_time*

##### requirements: *python2.7*