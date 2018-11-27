# Stream processing with Azure Databricks

This reference architecture shows an end-to-end [stream processing](/azure/architecture/data-guide/big-data/real-time-processing) pipeline. This type of pipeline has four stages: ingest, process, store, and analysis and reporting. For this reference architecture, the pipeline ingests data from two sources, performs a join on related records from each stream, enriches the result, and calculates an average in real time. The results are stored for further analysis. 

![](https://github.com/mspnp/architecture-center/blob/master/docs/reference-architectures/data/images/stream-processing-databricks.png)

**Scenario**: A taxi company collects data about each taxi trip. For this scenario, we assume there are two separate devices sending data. The taxi has a meter that sends information about each ride &mdash; the duration, distance, and pickup and dropoff locations. A separate device accepts payments from customers and sends data about fares. To spot ridership trends, the taxi company wants to calculate the average tip per mile driven, in real time, for each neighborhood.

## Deploy the solution

A deployment for this reference architecture is available on [GitHub](https://github.com/mspnp/reference-architectures/tree/master/data). 

### Prerequisites

1. Clone, fork, or download the zip file for the [reference architectures](https://github.com/mspnp/reference-architectures) GitHub repository.

2. Install [Docker](https://www.docker.com/) to run the data generator.

3. Install [Azure CLI 2.0](/cli/azure/install-azure-cli?view=azure-cli-latest).

4. Install [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html).

5. From a command prompt, bash prompt, or PowerShell prompt, sign into your Azure account as follows:
    ```
    az login
    ```
6. Install a Java IDE, with the following resources:
    - JDK 1.8
    - Scala SDK 2.11
    - Maven 3.5.4

### Download the New York City taxi and neighborhood data files

1. Create a directory named `DataFile` under the `data/streaming_azuredatabricks` directory in your local file system.

2. Open a web browser and navigate to https://uofi.app.box.com/v/NYCtaxidata/folder/2332219935.

3. Click the **Download** button on this page to download a zip file of all the taxi data for that year.

4. Extract the zip file to the `DataFile` directory.

    > [!NOTE]
    > This zip file contains other zip files. Don't extract the child zip files.

    The directory structure should look like the following:

    ```
    /data
        /streaming_azuredatabricks
            /DataFile
                /FOIL2013
                    trip_data_1.zip
                    trip_data_2.zip
                    trip_data_3.zip
                    ...
    ```

5. Open a web browser and navigate to https://www.zillow.com/howto/api/neighborhood-boundaries.htm. 

6. Click on **New York Neighborhood Boundaries** to download the file.

7. Copy the **ZillowNeighborhoods-NY.zip** file from your browser's **downloads** directory to the `DataFile` directory.

### Deploy the Azure resources

1. From a shell or Windows Command Prompt, run the following command and follow the sign-in prompt:

    ```bash
    az login
    ```

2. Navigate to the folder `data/streaming_azuredatabricks` in the GitHub repository

    ```bash
    cd data/streaming_azuredatabricks
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
    az group deployment create --resource-group $resourceGroup \
	    --template-file ./azure/deployresources.json --parameters \
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

5. In the text box below, enter the following::
```
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

> [!NOTE]
> If using an [Azure Key Vault-backed secret scope](https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html#azure-key-vault-backed-scopes), the scope must be named **azure-databricks-job** and the secrets must have the exact same names as those above.

### Add the Zillow Neighborhoods data file to the Databricks file system

1. Create a directory in the Databricks file system:
    ```bash
    dbfs mkdirs dbfs:/azure-databricks-jobs
    ```

2. Navigate to data/streaming_azuredatabricks/DataFile and enter the following:
    ```bash
    dbfs cp ZillowNeighborhoods-NY.zip dbfs:/azure-databricks-jobs
    ```

### Add the Azure Log Analytics workspace ID and primary key to configuration files

For this section, you require the Log Analytics workspace ID and primary key. The workspace ID is the **workspaceId** value from the **logAnalytics** output section in step 4 of the *deploy the Azure resources* section. The primary key is the **secret** from the output section. 

1. To configure log4j logging, open data\streaming_azuredatabricks\azure\AzureDataBricksJob\src\main\resources\com\microsoft\pnp\azuredatabricksjob\log4j.properties. Edit the following two values:
    ```
    log4j.appender.A1.workspaceId=<Log Analytics workspace ID>
    log4j.appender.A1.secret=<Log Analytics primary key>
    ```

2. To configure custom logging, open data\streaming_azuredatabricks\azure\azure-databricks-monitoring\scripts\metrics.properties. Edit the following two values:
    ``` 
    *.sink.loganalytics.workspaceId=<Log Analytics workspace ID>
    *.sink.loganalytics.secret=<Log Analytics primary key>
    ```

### Build the .jar files for the Databricks job and Databricks monitoring

1. Use your Java IDE to import the Maven project file named **pom.xml** located in the root of the **data/streaming_azuredatabricks** directory. 

2. Perform a clean build. The output of this build is files named **azure-databricks-job-1.0-SNAPSHOT.jar** and **azure-databricks-monitoring-0.9.jar**. 

### Configure custom logging for the Databricks job

1. Copy the **azure-databricks-monitoring-0.9.jar** file to the Databricks file system by entering the following command in the **Databricks CLI**:
    ```
    databricks fs cp --overwrite azure-databricks-monitoring-0.9.jar dbfs:/azure-databricks-job/azure-databricks-monitoring-0.9.jar
    ```

2. Copy the custom logging properties from data\streaming_azuredatabricks\azure\azure-databricks-monitoring\scripts\metrics.properties to the Databricks file system by entering the following command:
    ```
    databricks fs cp --overwrite metrics.properties dbfs:/azure-databricks-job/metrics.properties
    ```

3. While you haven't yet decided on a name for your Databricks cluster, select one now. You'll enter the name below in the Databricks file system path for your cluster. Copy the initialization script from data\streaming_azuredatabricks\azure\azure-databricks-monitoring\scripts\spark.metrics to the Databricks file system by entering the following command:
    ```
    databricks fs cp --overwrite spark-metrics.sh dbfs:/databricks/init/<cluster-name>/spark-metrics.sh
    ```

### Create a Databricks cluster

1. In the Databricks workspace, click "Clusters", then click "create cluster". Enter the cluster name you created in step 3 of the **configure custom logging for the Databricks job** section above. 

2. Select a **standard** cluster mode.

3. Set **Databricks runtime version** to **4.3 (includes Apache Spark 2.3.1, Scala 2.11)**

4. Set **Python version** to **2**.

5. Set **Driver Type** to **Same as worker**

6. Set **Worker Type** to **Standard_DS3_v2**.

7. Set **Min Workers** to **2**.

8. Deselect **Enable autoscaling**. 

9. Below the **Auto Termination** dialog box, click on **Init Scripts**. 

10. Enter **dbfs:/databricks/init/<cluster-name>/spark-metrics.sh**, substituting the cluster name created in step 1 for <cluster-name>.

11. Click the **Add** button.

12. Click the **Create Cluster** button.

### Create a Databricks job

1. In the Databricks workspace, click "Jobs", "create job".

2. Enter a job name.

3. Click "set jar", this opens the "Upload JAR to Run" dialog box.

4. Drag the **azure-databricks-job-1.0-SNAPSHOT.jar** file you created in the **build the .jar for the Databricks job** section to the **Drop JAR here to upload** box.

5. Enter **com.microsoft.pnp.TaxiCabReader** in the **Main Class** field.

6. In the arguments field, enter the following:
    ```
    -n jar:file:/dbfs/azure-databricks-jobs/ZillowNeighborhoods-NY.zip!/ZillowNeighborhoods-NY.shp --taxi-ride-consumer-group taxi-ride-eh-cg --taxi-fare-consumer-group taxi-fare-eh-cg --window-interval "1 minute" --cassandra-host <Cosmos DB Cassandra host name from above> 
    ``` 

7. Install the dependent libraries by following these steps:
    
    1. In the Databricks user interface, click on the **home** button.
    
    2. In the **Users** drop-down, click on your user account name to open your account workspace settings.
    
    3. Click on the drop-down arrow beside your account name, click on **create**, and click on **Library** to open the **New Library** dialog.
    
    4. In the **Source** drop-down control, select **Maven Coordinate**.
    
    5. Under the **Install Maven Artifacts** heading, enter `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.5` in the **Coordinate** text box. 
    
    6. Click on **Create Library** to open the **Artifacts** window.
    
    7. Under **Status on running clusters** check the **Attach automatically to all clusters** checkbox.
    
    8. Repeat steps 1 - 7 for the `com.microsoft.azure.cosmosdb:azure-cosmos-cassandra-spark-helper:1.0.0` Maven coordinate.
    
    9. Repeat steps 1 - 6 for the `org.geotools:gt-shapefile:19.2` Maven coordinate.
    
    10. Click on **Advanced Options**.
    
    11. Enter `http://download.osgeo.org/webdav/geotools/` in the **Repository** text box. 
    
    12. Click **Create Library** to open the **Artifacts** window. 
    
    13. Under **Status on running clusters** check the **Attach automatically to all clusters** checkbox.

8. Add the dependent libraries added in step 7 to the job created at the end of step 6:
    1. In the Azure Databricks workspace, click on **Jobs**.

    2. Click on the job name created in step 2 of the **create a Databricks job** section. 
    
    3. Beside the **Dependent Libraries** section, click on **Add** to open the **Add Dependent Library** dialog. 
    
    4. Under **Library From** select **Workspace**.
    
    5. Click on **users**, then your username, then click on `azure-eventhubs-spark_2.11:2.3.5`. 
    
    6. Click **OK**.
    
    7. Repeat steps 1 - 6 for `spark-cassandra-connector_2.11:2.3.1` and `gt-shapefile:19.2`.

9. Beside **Cluster:**, click on **Edit**. This opens the **Configure Cluster** dialog. In the **Cluster Type** drop-down, select **Existing Cluster**. In the **Select Cluster** drop-down, select the cluster created the **create a Databricks cluster** section. Click **confirm**.

10. Click **run now**.

### Run the data generator

1. Navigate to the directory `data/streaming_azuredatabricks/onprem` in the GitHub repository.

2. Update the values in the file **main.env** as follows:

    ```
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

4. Navigate back to the parent directory, `data/stream_azuredatabricks`.

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