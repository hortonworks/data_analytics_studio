## Build Hive Studio mpack:

```bash
mvn clean package -P mpack
```

Output file can be found at 

```bash
target/data-analytics-studio-mpack-XXX.tar.gz
```

## Install Hive Studio mpack:

Stop Ambari Server:
```bash
ambari-server stop
```

Install Hive Studio mpack:
```bash
ambari-server install-mpack --mpack=/my-path/data-analytics-studio-mpack-XXX.tar.gz --verbose
```

Start Ambari Server
```bash
ambari-server start
```
