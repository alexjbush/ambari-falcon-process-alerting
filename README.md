# ambari-falcon_process-alerting

Ambari Alerts triggered from Falcon process statuses

Works in both Kerberised and non-Kerberised environments.

Queries the Oozie server in the Falcon Process and Cluster definitions to check Oozie workflow statuses over time.

## Alert definition

The alert takes three variables for configuration.

1. The whitelist pattern to filter Falcon Process names. Only Falcon Processes that contain this pattern will be alerted on.
2. The timeframe in minutes in the past from now in which workflows should be monitored. At least one successful workflow must complete in this window.
3. The number of workflow failures to tolerate in the monitoring window. Give `-1` to ignore failures.

```javascript
      {
        "name": "process_pattern",
        "display_name": "Process Name pattern",
        "value": "-dr-",
        "type": "STRING",
        "description": "Process name pattern to filter on"
      },
      {
        "name": "monitor_window",
        "display_name": "Monitoring Window",
        "value": 30,
        "type": "STRING",
        "units": "minutes",
        "description": "Time window in which at least one successful run should complete",
      },
      {
        "name": "failure_tolerance",
        "display_name": "Failure Tolerance",
        "value": -1,
        "type": "STRING",
        "description": "Number of failed workflows to tolerate in the monitoring window (-1 to ignore failures)"
      }
```

## Alert installation

> Taken from [https://github.com/monolive/ambari-custom-alerts](https://github.com/monolive/ambari-custom-alerts)

Push the new alert via Ambari REST API.

```sh
curl -u admin:admin -i -H 'X-Requested-By: ambari' -X POST -d @alerts.json http://ambari.cloudapp.net:8080/api/v1/clusters/hdptest/alert_definitions
```
You will also need to copy the python script in /var/lib/ambari-server/resources/host_scripts and restart the ambari-server. After restart the script will be pushed in /var/lib/ambari-agent/cache/host_scripts on the different hosts.

You can find the ID of your alerts by running
```sh
curl -u admin:admin -i -H 'X-Requested-By: ambari' -X GET http://ambari.cloudapp.net:8080/api/v1/clusters/hdptest/alert_definitions
```

If we assume, that your alert is id 103. You can force the alert to run by
```sh
curl -u admin:admin -i -H 'X-Requested-By: ambari' -X PUT  http://ambari.cloudapp.net:8080/api/v1/clusters/hdptest/alert_definitions/103?run_now=true
```

## [License](LICENSE)

Copyright (c) 2016 Alex Bush.
Licensed under the [Apache License](LICENSE).
