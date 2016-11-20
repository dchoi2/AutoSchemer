# CSVQuerier
TODO: README

Components Overview:
Parser: parses data file, Returns:
    - distinct rows => (col_num, num_distinct_rows)
    - column numbers ordered by most distinct rows
    - list of type guessers

TypeGuesser: aggregate histogram heuristic to guess what the type of a column is. Used in Parser while iterating through each row/column.


AutoSchemer: the wrapper for running the program

Table: tables have ids, a list of columns, a list of foreign keys which point to different tables if there is a relation between them, and a list of primary_keys which is not necessarily distinct from the list of columns.

Schema: for simplicity, currently schema contains two objects: 1) a list of tables, and 2) a list of types corresponding to the columns of the dataset. This is computed with the TypeGuesser class while parsing, as mentioned above


#####
Steps for psql setup (INCOMPLETE)
1) createdb db_name

# Use postgres user 
sudo su - postgres

# OR create psql user
psql# CREATE USER user_name WITH PASSWORD 'password';
psql# GRANT ALL PRIVILEGES ON DATABASE db_name TO user_name;

# input schema into db (schema.psql will contain the schema creation logic)
postgress$ psql -f schema.psql -d db_name 

# testing csv input code
$PROJECT_DIR='/home/dmchoi/Projects/MIT/CSVQuerier';

psql -c 'COPY tb_name FROM '{$PROJECT_DIR}/data.csv' delimiter ',' csv' db_name;

