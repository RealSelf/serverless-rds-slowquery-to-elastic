# What is this
This lambda copies a table backed RDS mysql slow query log into Elasticsearch.

It does so by:

      1. Calling a stored procedure in RDS to copy the current slow_log table to slow_log_backup ( called mysql.rds_rotate_slow_log )
      2. Querying mysql.slow_log_backup
      3. Dumping all of that into Elasticsearch

It runs inside a VPC, so you'll need to know a little bit about your VPC, like subnet-ids and vpc-ids 

## How does it run? 
1. First you must setup a user in mysql:

```
GRANT SELECT ON mysql.slow_log to `username`@`%` IDENTIFIED BY 'supersecretpassword'; 
GRANT SELECT ON mysql.slow_log_backup to `username`@`%`;
GRANT EXECUTE ON PROCEDURE mysql.rds_rotate_slow_log to `username`@`%`;
FLUSH PRIVILEGES;
```

2. Now create a secrets file in json format, and encrypt it.
    1. assuming this as the secrets file, `/var/tmp/secret.txt`

```json
{
 "db_host": "database.example.com",
 "username": "scott",
 "password": "tiger",
 "database": "mysql"
}
```

1. KMS encrypt that . There's a 🐔/🥚 thing here. you must `serverless deploy` once to create the KMS key and alias

```
aws kms encrypt --key-id alias/lambda-mysql-slowquerylog-prod --plaintext fileb:///path/to/secret.txt > prod.txt
```


2. Allow this lambda's SecurityGroup to connect to RDS via the RDS SecurityGroup

3. Deploy. Below was my last production deployment
   
```
ELASTICSEARCH_URL=http://elasticsearch.example.com:9200  SECRET_FILE="./prod.txt" serverless deploy --stage prod --vpc  vpc-XXXXX  --subnet1 subnet-AAAAAA --subnet2 subnet-BBBBB
```

## More info?
* Logs are in CloudWatchLogs
* The ES indices are named `slowquery-YYYY.mm.dd`
* The _type of the document is `database.example.com`
