# Stream processing with Azure Databricks

This reference architecture shows an end-to-end [stream processing](https://docs.microsoft.com/azure/architecture/data-guide/big-data/real-time-processing) pipeline. This type of pipeline has four stages: ingest, process, store, and analysis and reporting. For this reference architecture, the pipeline ingests data from two sources, performs a join on related records from each stream, enriches the result, and calculates an average in real time. The results are stored for further analysis. 

![](https://github.com/mspnp/architecture-center/blob/master/docs/reference-architectures/data/images/stream-processing-databricks.png)

**Scenario**: A taxi company collects data about each taxi trip. For this scenario, we assume there are two separate devices sending data. The taxi has a meter that sends information about each ride &mdash; the duration, distance, and pickup and dropoff locations. A separate device accepts payments from customers and sends data about fares. To spot ridership trends, the taxi company wants to calculate the average tip per mile driven, in real time, for each neighborhood.

## Deploy the solution

A deployment for this reference architecture is available on [GitHub](https://github.com/mspnp/azure-databricks-streaming-analytics). 

### Prerequisites

1. Clone, fork, or download this GitHub repository.

2. Install [Docker](https://www.docker.com/) to run the data generator.

3. Install [Azure CLI 2.0](https://docs.microsoft.com/cli/azure/install-azure-cli?view=azure-cli-latest).

4. Install [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html).

5. From a command prompt, bash prompt, or PowerShell prompt, sign into your Azure account as follows:
    ```bash
    az login
    ```
6. Optional - Install a Java IDE, with the following resources:
    - JDK 1.8
    - Scala SDK 2.11
    - Maven 3.6.3
    > Note: Instructions are included for building via a docker container if you do not want to install a Java IDE.

### Download the New York City taxi and neighborhood data files

1. Create a directory named `DataFile` in the root of the cloned Github repository in your local file system.

2. Open a web browser and navigate to https://uofi.app.box.com/v/NYCtaxidata/folder/2332219935.

3. Click the **Download** button on this page to download a zip file of all the taxi data for that year.

4. Extract the zip file to the `DataFile` directory.

    > Note: This zip file contains other zip files. Don't extract the child zip files.

    The directory structure should look like the following:

    ```shell
    /DataFile
        /FOIL2013
            trip_data_1.zip
            trip_data_2.zip
            trip_data_3.zip
            ...
    ```

5. Open a web browser and navigate to <https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html#ti1400387013>. 

6. Under the section **County Subdivisions** click the dropdown an select **New York**.

7. Copy the **cb_2019_36_cousub_500k.zip** file from your browser's **downloads** directory to the `DataFile` directory.

### Deploy the Azure resources

1. From a shell or Windows Command Prompt, run the following command and follow the sign-in prompt:

    ```bash
    az login
    ```

2. Navigate to the folder named `azure` in the GitHub repository:

    ```bash
    cd azure
    ```

3. Run the following commands to deploy the Azure resources:

    ```bash
    export resourceGroup='[Resource group name]'
    export resourceLocation='[Region]'
    export eventHubNamespace='[Event Hubs namespace name]'
    export databricksWorkspaceName='[Azure Databricks workspace name]'
    export cosmosDatabaseAccount='[Cosmos DB database name]'
    export logAnalyticsWorkspaceName='[Log Analytics workspace name]'
    export logAnalyticsWorkspaceRegion='[Log Analytics region]'

    # Create a resource group
    az group create --name $resourceGroup --location $resourceLocation

    # Deploy resources
    az deployment group create --resource-group $resourceGroup \
	    --template-file ./deployresources.json --parameters \
	    eventHubNamespace=$eventHubNamespace \
        databricksWorkspaceName=$databricksWorkspaceName \
	    cosmosDatabaseAccount=$cosmosDatabaseAccount \
	    logAnalyticsWorkspaceName=$logAnalyticsWorkspaceName \
	    logAnalyticsWorkspaceRegion=$logAnalyticsWorkspaceRegion
    ```

4. The output of the deployment is written to the console once complete. Search the output for the following JSON:

```JSON
"outputs": {
        "cosmosDb": {
          "type": "Object",
          "value": {
            "hostName": <value>,
            "secret": <value>,
            "username": <value>
          }
        },
        "eventHubs": {
          "type": "Object",
          "value": {
            "taxi-fare-eh": <value>,
            "taxi-ride-eh": <value>
          }
        },
        "logAnalytics": {
          "type": "Object",
          "value": {
            "secret": <value>,
            "workspaceId": <value>
          }
        }
},
```
These values are the secrets that will be added to Databricks secrets in upcoming sections. Keep them secure until you add them in those sections.

### Add a Cassandra table to the Cosmos DB Account

1. In the Azure portal, navigate to the resource group created in the **deploy the Azure resources** section above. Click on **Azure Cosmos DB Account**. Create a table with the Cassandra API.

2. In the **overview** blade, click **add table**.

3. When the **add table** blade opens, enter `newyorktaxi` in the **Keyspace name** text box. 

4. In the **enter CQL command to create the table** section, enter `neighborhoodstats` in the text box beside `newyorktaxi`.

5. In the text box below, enter the following:
```shell
(neighborhood text, window_end timestamp, number_of_rides bigint,total_fare_amount double, primary key(neighborhood, window_end))
```
6. In the **Throughput (1,000 - 1,000,000 RU/s)** text box enter the value `4000`.

7. Click **OK**.

### Add the Databricks secrets using the Databricks CLI

First, enter the secrets for EventHub:

1. Using the **Azure Databricks CLI** installed in step 2 of the prerequisites, create the Azure Databricks secret scope:
    ```
    databricks secrets create-scope --scope "azure-databricks-job"
    ```
2. Add the secret for the taxi ride EventHub:
    ```
    databricks secrets put --scope "azure-databricks-job" --key "taxi-ride"
    ```
    Once executed, this command opens the vi editor. Enter the **taxi-ride-eh** value from the **eventHubs** output section in step 4 of the *deploy the Azure resources* section. Save and exit vi.

3. Add the secret for the taxi fare EventHub:
    ```
    databricks secrets put --scope "azure-databricks-job" --key "taxi-fare"
    ```
    Once executed, this command opens the vi editor. Enter the **taxi-fare-eh** value from the **eventHubs** output section in step 4 of the *deploy the Azure resources* section. Save and exit vi.

Next, enter the secrets for Cosmos DB:

1. Open the Azure portal, and navigate to the resource group specified in step 3 of the **deploy the Azure resources** section. Click on the Azure Cosmos DB Account.

2. Using the **Azure Databricks CLI**, add the secret for the Cosmos DB user name:
    ```
    databricks secrets put --scope azure-databricks-job --key "cassandra-username"
    ```
Once executed, this command opens the vi editor. Enter the **username** value from the **CosmosDb** output section in step 4 of the *deploy the Azure resources* section. Save and exit vi.

3. Next, add the secret for the Cosmos DB password:
    ```
    databricks secrets put --scope azure-databricks-job --key "cassandra-password"
    ```

Once executed, this command opens the vi editor. Enter the **secret** value from the **CosmosDb** output section in step 4 of the *deploy the Azure resources* section. Save and exit vi.

> Note: If using an [Azure Key Vault-backed secret scope](https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html#azure-key-vault-backed-scopes), the scope must be named **azure-databricks-job** and the secrets must have the exact same names as those above.

### Add the Zillow Neighborhoods data file to the Databricks file system

1. Create a directory in the Databricks file system:
    ```bash
    dbfs mkdirs dbfs:/azure-databricks-jobs
    ```

2. Navigate to the DataFile folder and enter the following:
    ```bash
    dbfs cp cb_2019_36_cousub_500k.zip dbfs:/azure-databricks-jobs
    ```

### Add the Azure Log Analytics workspace ID and primary key to configuration files

For this section, you require the Log Analytics workspace ID and primary key. The workspace ID is the **workspaceId** value from the **logAnalytics** output section in step 4 of the *deploy the Azure resources* section. The primary key is the **secret** from the output section. 

1. To configure log4j logging, open azure\AzureDataBricksJob\src\main\resources\com\microsoft\pnp\azuredatabricksjob\log4j.properties. Edit the following two values:
    ```
    log4j.appender.A1.workspaceId=<Log Analytics workspace ID>
    log4j.appender.A1.secret=<Log Analytics primary key>
    ```

2. To configure custom logging, open azure\azure-databricks-monitoring\scripts\metrics.properties. Edit the following two values:
    ``` 
    *.sink.loganalytics.workspaceId=<Log Analytics workspace ID>
    *.sink.loganalytics.secret=<Log Analytics primary key>
    ```

### Build the .jar files for the Databricks job and Databricks monitoring

1. To build the jars using a docker container from a bash prompt change to the **azure** directory and run:

    ```bash
    docker run -it --rm -v `pwd`:/streaming_azuredatabricks_azure -v ~/.m2:/root/.m2 maven:3.6.3-jdk-8 mvn -f /streaming_azuredatabricks_azure/pom.xml package
    ```

    > Note: Alternately, use your Java IDE to import the Maven project file named **pom.xml** located in the **data/streaming_azuredatabricks/azure** directory. Perform a clean build.

1. The outputs of the build are files named **azure-databricks-monitoring-0.9.jar** in the **./azure-databricks-monitoring/target** directory and **azure-databricks-job-1.0-SNAPSHOT.jar** in the **./AzureDataBricksJob/target** directory.

### Configure custom logging for the Databricks job

1. Copy the **azure-databricks-monitoring-0.9.jar** file to the Databricks file system by entering the following command in the **Databricks CLI**:
    ```
    databricks fs cp --overwrite azure-databricks-monitoring-0.9.jar dbfs:/azure-databricks-job/azure-databricks-monitoring-0.9.jar
    ```

2. Copy the custom logging properties from \azure\azure-databricks-monitoring\scripts\metrics.properties to the Databricks file system by entering the following command:
    ```
    databricks fs cp --overwrite metrics.properties dbfs:/azure-databricks-job/metrics.properties
    ```

3. While you haven't yet decided on a name for your Databricks cluster, select one now. You'll enter the name below in the Databricks file system path for your cluster. Copy the initialization script from azure\azure-databricks-monitoring\scripts\spark.metrics to the Databricks file system by entering the following command:
    ```
    databricks fs cp --overwrite spark-metrics.sh dbfs:/databricks/init/<cluster-name>/spark-metrics.sh
    ```

### Create a Databricks cluster

1. In the Databricks workspace, click "Clusters", then click "create cluster". Enter the cluster name you created in step 3 of the **configure custom logging for the Databricks job** section above. 

2. Select a **standard** cluster mode.

3. Set **Databricks runtime version** to **6.6 (includes Apache Spark 2.4.5, Scala 2.11)**

4. Set **Python version** to **3**.

5. Set **Driver Type** to **Same as worker**

6. Set **Worker Type** to **Standard_DS3_v2**.

7. Deselect **Enable autoscaling**.

8. Set **Workers** to **2**.

9. Below the **Auto Termination** dialog box, click on **Advanced Options** then **Init Scripts**.

10. Enter **dbfs:/databricks/init/\<cluster-name\>/spark-metrics.sh**, substituting the cluster name created in step 1 for **\<cluster-name\>**.

11. Click the **Add** button.

12. Click the **Create Cluster** button.

### Install dependent libraries on cluster

1. In the Databricks user interface, click on the **home** button.

2. Click on **Clusters** then click on the cluster you created in the **Create a Databricks cluster** step.

3. Click on **Libraries**, then click **Install New**.

4. In the **Library Source** control, select **Maven**.

5. Under the **Maven Coordinates** text box, enter `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.5`.

6. Select **Install**.

7. Repeat steps 3 - 6 for the `com.microsoft.azure.cosmosdb:azure-cosmos-cassandra-spark-helper:1.0.0` Maven coordinate.

8. Repeat steps 3 - 6 for the `com.datastax.spark:spark-cassandra-connector_2.11:2.3.1` Maven coordinate.

9. Repeat steps 3 - 5 for the `org.geotools:gt-shapefile:19.2` Maven coordinate.

10. Enter `https://repo.osgeo.org/repository/release/` in the **Repository** text box.

11. Click **Install**.

### Create a Databricks job

1. In the Databricks workspace, click "Jobs", "create job".

2. Enter a job name.

3. Click "set jar", this opens the "Upload JAR to Run" dialog box.

4. Drag the **azure-databricks-job-1.0-SNAPSHOT.jar** file you created in the **build the .jar for the Databricks job** section to the **Drop JAR here to upload** box.

5. Enter **com.microsoft.pnp.TaxiCabReader** in the **Main Class** field.

6. In the arguments field, enter the following (replace **\<Cosmos DB Cassandra host name\>** with a value from above):

    ```
    -n jar:file:/dbfs/azure-databricks-jobs/cb_2019_36_cousub_500k.zip!/cb_2019_36_cousub_500k.shp --taxi-ride-consumer-group taxi-ride-eh-cg --taxi-fare-consumer-group taxi-fare-eh-cg --window-interval "1 minute" --cassandra-host <Cosmos DB Cassandra host name>
    ```

7. Click **OK**.

8. Beside **Cluster:**, click on **Edit**. This opens the **Configure Cluster** dialog. In the **Cluster Type** drop-down, select **Existing Interactive Cluster**. In the **Select Cluster** drop-down, select the cluster created the **Create a Databricks cluster** section. Click **Confirm**.

10. Click **run now**.

### Run the data generator

1. Navigate to the directory `data/streaming_azuredatabricks/onprem` in the GitHub repository.

2. Update the values in the file **main.env** as follows:

    ```shell
    RIDE_EVENT_HUB=[Connection string for the taxi-ride event hub]
    FARE_EVENT_HUB=[Connection string for the taxi-fare event hub]
    RIDE_DATA_FILE_PATH=/DataFile/FOIL2013
    MINUTES_TO_LEAD=0
    PUSH_RIDE_DATA_FIRST=false
    ```
    The connection string for the taxi-ride event hub is the **taxi-ride-eh** value from the **eventHubs** output section in step 4 of the *deploy the Azure resources* section. The connection string for the taxi-fare event hub the **taxi-fare-eh** value from the **eventHubs** output section in step 4 of the *deploy the Azure resources* section.

3. Run the following command to build the Docker image.

    ```bash
    docker build --no-cache -t dataloader .
    ```

4. Navigate back to the parent directory.

    ```bash
    cd ..
    ```

5. Run the following command to run the Docker image.

    ```bash
    docker run -v `pwd`/DataFile:/DataFile --env-file=onprem/main.env dataloader:latest
    ```

The output should look like the following:

```
Created 10000 records for TaxiFare
Created 10000 records for TaxiRide
Created 20000 records for TaxiFare
Created 20000 records for TaxiRide
Created 30000 records for TaxiFare
...
```

To verify the Databricks job is running correctly, open the Azure portal and navigate to the Cosmos DB database. Open the **Data Explorer** blade and examine the data in the **taxi records** table.

[1] <span id="note1">Donovan, Brian; Work, Dan (2016): New York City Taxi Trip Data (2010-2013). University of Illinois at Urbana-Champaign. https://doi.org/10.13012/J8PN93H8