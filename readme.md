# Replication API
## What is the API?
This API was built to ease replicate data between SQL Servers using Bulk Inserts. It uses a PowerShell Tools based called DBATools and allow run SQL commands through cmdlets and easily make bulk inserts only informing some parameters.

## How does it work?
The API is divided into two sides:

Server-Side: that receives the requisitions and run the functionalities based on PowerShell tool;
Client-Side: that requests the execution on the Server-Side based on the parameters set on the configuration.

![Image of Workflow](https://github.com/NicholasGoes/replication_api/blob/master/imgs/Workflow.png)

On the Client-Side, to know which tables will be replicated a CSV archive is used. On this archive, 7 columns indicate the operations to be executed on the server-side. There are:

* database: indicate which database the origin objects are; 
* schema: indicate which schema the origin objects are;
* origin_table:indicate which table will be used to gather the data;
* target_table: indicate which table will be used to insert the data;
* origin_server: indicate the orgin  server;
* target_server: indicate the target server;
* selectQuery: indicate the query to be used to collect the origin data; **(If empty, will select all records on the table)**
* deleteQuery: indicate the sql command to be used to delete records on the target server. **(If empty, will truncate the table)**

On the Server-Side, the API receives the request and executes the respective command to Delete or Bulk Insert data on the table. So, it gets the data from the to origin server and inserts it on the target server.

![Image of Data Replication Fluxogram](https://github.com/NicholasGoes/replication_api/blob/master/imgs/Data%20Replication%20Fluxogram.png)
