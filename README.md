# CSVQuerier
TODO: README


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

