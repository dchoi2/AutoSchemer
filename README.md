SVAutoSchemer

### Setup and configurations
##### Python libraries
To install all dependencies, run
`pip install -r requirements.txt`
##### Postgres Configuration
You may find it useful to create a separate database for testing. To do this, 
```shell
user$ sudo su - postgres
postgres$ psql
psql# CREATE DATABASE db_name
```
Now you have created a db called _db_name_
You can also choose to create a new postgres user.
``` shell
psql# CREATE USER user_name WITH PASSWORD 'password';
psql# GRANT ALL PRIVILEGES ON DATABASE db_name TO user_name;
```

##### Environmental Variables
You'll need to set up environmental variables for your postgres db credentials. This is so that pgdb can connect to your specified db and create the schema. The required variables are 
* **dbname**: the name of the db you're connecting to, as created above in the postgres configuration step
* **dbuser**: name of the user with the sufficient privilege to access the above dbname
* **dbhost**: this will be localhost for our purposes
* **dbpwd**: depending on the method of authentication you configured at '/etc/postgresql/x.x/main/pg_hba.conf', the program will require a password corresponding to the dbuser. If the method is 'trust', then dbpwd is not required
An example script would look like:

```bash
## filename: db_credentials.sh
#!/bin/bash

export dbname=testdb # custom db name
export dbuser=postgres # typically, postgres, or custom username if created
export dbhost=localhost
export dbpwd=textpwd # password, if method of authentication is not trust, then need to pass in password too

```
**_Do not push this file to github._**

You can then call `. db_credentials.sh` to load these variables into your environment
### Running AutoSchemer
To run the autoschemer, you can simply run 
```bash
$ python AutoSchemer.py csv-file
```



### Components Overview (TODO: Incomplete):
#### Parser
parses CSV data file, Returns:
* distinct rows => (col_num, num_distinct_rows)
* column numbers ordered by most distinct rows
* list of type guessers

#### TypeGuesser:
aggregate histogram heuristic to guess what the type of a column is. Used in Parser while iterating through each row/column.


#### AutoSchemer
the wrapper for running the program

#### Table
tables have ids, a list of columns, a list of foreign keys which point to different tables if there is a relation between them, and a list of primary_keys which is not necessarily distinct from the list of columns.

#### Schema
for simplicity, currently schema contains two objects: 1) a list of tables, and 2) a list of types corresponding to the columns of the dataset. This is computed with the TypeGuesser class while parsing, as mentioned above

