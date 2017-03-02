import sys
sys.path.append("./vendored")
import json
import boto3
import os
import logging
import pymysql
import elasticsearch
import datetime
import base64
import hashlib

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_config(file=None):
    """ Returns the dict() of the decrypted ciphertext

      KWArgs:
        file = The file of ciphertext
    """
    with open(file) as data_file:
        ciphertext = json.load(data_file)['CiphertextBlob']
    decrypted_text = _kms_decrypt(cipher_text=ciphertext)
    return json.loads(decrypted_text)

def _kms_decrypt(cipher_text=None):
    """ Returns the plain-text STRING which is the database config json dict

        KWArgs:
        client_key_cipher_text = the KMS crypted cipher text
    """
    kms = boto3.client('kms')
    return kms.decrypt(CiphertextBlob=base64.b64decode(cipher_text))['Plaintext']

def json_datetime_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
        return serial
    if isinstance(obj, datetime.timedelta):
        return obj.total_seconds()
    raise TypeError ("Type %s not serializable" % (type(obj).__name__))


class JSONSerializerES(elasticsearch.serializer.JSONSerializer):
    """ Override the elasticsearch library serializer, BECAUSE THAT'S THE KIND
    of crazy I am. 
    or because the default serializer can't take datetime.timedelta objects
    """
    def dumps(self, data):
        if isinstance(data, elasticsearch.compat.string_types):
            return data
        try:
            return json.dumps(data, default=json_datetime_serial)
        except (ValueError, TypeError) as e:
            raise elasticsearch.exceptions.SerializationError(data, e)

# http://docs.aws.amazon.com/lambda/latest/dg/vpc-rds-deployment-pkg.html
# :point_up: connect() outside of the handler for performance.
#  I'm totally cargoculting that
config_data = parse_config(os.environ.get('SECRET_FILE'))
try:
    conn = pymysql.connect(host=config_data['db_host'],
                           user=config_data['username'],
                           passwd=config_data['password'],
                           db=config_data['database'],
                           connect_timeout=5)
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

es = None
if os.environ.get('ELASTICSEARCH_URL'):
    try:
        es = elasticsearch.Elasticsearch(os.environ.get('ELASTICSEARCH_URL'),
                                         serializer=JSONSerializerES())
        es_logger = logging.getLogger('elasticsearch')
        es_logger.setLevel(logging.WARNING)
        es.info()
    except:
        raise


def es_and_cloudwatch(event, context):
    with conn.cursor() as cursor:
        # "SELECT * FROM slow_query_log ORDER "
        cols = [ 'start_time', 'user_host', 'query_time', 'lock_time', 'rows_sent', 'rows_examined', 'db', 'last_insert_id', 'insert_id', 'server_id', 'sql_text', 'thread_id', 'start_time_epoch_seconds' ]
        ####
        ####
        sql = "SELECT start_time, user_host, query_time, lock_time, rows_sent, rows_examined, db, last_insert_id, insert_id, server_id, sql_text, thread_id, UNIX_TIMESTAMP(start_time) AS epoch_seconds FROM slow_log WHERE start_time >= DATE_SUB(NOW(), INTERVAL 6 MINUTE) ORDER BY epoch_seconds"
        ####
        ####
        cursor.execute(sql)
        res = cursor.fetchall()

        counter = 0
        for row in res:
            if (counter % 50) == 0:
                 logger.info("Processed %i rows from mysql" % (counter))
            if es:
                 doc = dict(zip(cols, row))
                 tmpid = hashlib.md5()
                 tmpid.update("%s" % (doc['start_time_epoch_seconds']))
                 tmpid.update(doc['sql_text'])
                 tmpid.update("%s" % (doc['thread_id']))
                 date_index_name = doc['start_time'].strftime('slowquery-%Y.%m.%d')
                 try:
                     esres = es.index(index=date_index_name,
                                      doc_type=config_data['db_host'],
                                      id=tmpid.hexdigest(),
                                      body=doc)
                 except Exception as e:
                     logger.warning("FAILURE Indexing in Elasticsearch, %s" % (e))
                 if esres['_shards']['failed'] > 0:
                     logger.warning("FAILURE TO LOG TO ELASTICSEARCH")
                     logger.warning(json.dumps(doc))
                     logger.warning(json.dumps(esres))
            else:
                logger.info(json.dumps(row, default=json_datetime_serial))
            counter += 1
        return {'status': 'ended'}
