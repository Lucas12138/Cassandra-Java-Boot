# Cassandra-Java-Boot
Quick start with a single node Cassandra on Mac as an example, emphasizing on the Java backend


## Quick start

To run on a stand-alone local node, if you're using Mac and haven't installed Cassandra:
- brew install cassandra
- brew services start cassandra
- brew info cassandra
  
After Cassandra has been installed, you will need to prepare the database schema.
You can simply go to resource/schema directory and run:
- cqlsh -u cassandra -p cassandra -f user_activity.cql  
  
You may want to use intellij to inspect this repo.
The Maven pom.xml file has been well written. You can find the dependencies in that file.
It should not be hard to figure out how to run/debug it.

If you want to deploy it, you can build it as a war file and deploy it on Tomcat. 

## Main structure
It's using a MVC pattern.

You can find the views(JSPs) in the src/main/webapp/WEB-INF/view directory.

The controller is applying the similar pattern in this [repo](https://github.com/Lucas12138/JavaEE-Web-Application).

Interesting things happen in the model part.
It's using singleton pattern for CassandraConnector and its DAOs.
Callback style DB query is applied to CqlRequest.
Besides, the CqlRequestFactory is using factory pattern to generate different kinds of CRUD requests.
To make it more adaptable and avoid duplication code, classes are designed with layers.
GenericDAO is the root class, supposed to handle basic CRUD methods only. Table specific DAOs can extend it and make more complex and specific queries.
Data are encapsulated as beans. Java reflection is intensively used in the beans to achieve ORM-like queries. 

## Demo
1. Existing activity logs

![alt text](https://github.com/Lucas12138/Cassandra-Java-Boot/blob/master/resource/demoScreenshots/demo1.png)

2. Add a new log

![alt text](https://github.com/Lucas12138/Cassandra-Java-Boot/blob/master/resource/demoScreenshots/demo2.png)
![alt text](https://github.com/Lucas12138/Cassandra-Java-Boot/blob/master/resource/demoScreenshots/demo3.png)

3. Delete a previous log

![alt text](https://github.com/Lucas12138/Cassandra-Java-Boot/blob/master/resource/demoScreenshots/demo4.png)
![alt text](https://github.com/Lucas12138/Cassandra-Java-Boot/blob/master/resource/demoScreenshots/demo5.png)

## TODO
- Discuss about the compaction strategy
- Consider more about the consistency level
- Implement stream query features
- Make a blog 
- Consider if Action pattern, synchronizing on a hashMap, will lead to performance issue
- How to achieve atomicity across difference tables, currently it's hard to achieve because different daos are used in controller layer 