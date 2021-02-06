# importing System dependencies and required flask modules
import sys
import flask
from flask import request, jsonify
from flask import abort
import mysql.connector
import datetime

class FeedbackHandler:
    def __init__(self,database_host,Kafka_topic,database_username,database_password,database_name):
        # Initialize MySQL database connection
        self.db = mysql.connector.connect(
            host=database_host,user=database_username,password=database_password,database=database_name
            )

    def fetchServerAdDetails(self):
        # Initialize DB cursor
        db_cursor = self.db.cursor()

        # Sql fetch served ads details based on Request ID
        sql = "select * from served_ads where requestID ='" + self.ad_request_id +"'"
        
        # Retrieve auction ad details
        db_cursor.execute(sql)

        # Fetching all the details and assigning to the object
        self.servedDetails =  db_cursor.fetchall()


    def handlerProcess(self):
        self.fetchServerAdDetails()
        self.
        self.SendDatatoKafkaProducer()

    def __del__(self):
        # Cleanup database connection before termination
        self.db.close()

if __name__ == "__main__":

    # Validate Command line arguments
    if len(sys.argv) != 6:
        print("Usage: <database_host> <Kafka_topic> <database_username> <database_password> <database_name>")
        exit(-1)

    # Assiging arguments with meaning full names
    database_host = sys.argv[1]
    kafka_Topic = sys.argv[2]
    database_username = sys.argv[3]
    database_password = sys.argv[4]
    database_name = sys.argv[5]

    try:
        feedback_handler = FeedbackHandler(database_host,Kafka_topic,database_username,database_password,database_name)

        # Basic Flask Configuration
        app = flask.Flask(__name__)
        app.config["DEBUG"] = True;
        app.config["RESTFUL_JSON"] = {"ensure_ascii":False}

        # Http Get request  processing
        @app.route('/ad/<ad_request_id>/feedback', methods=['POST'])
        def request_handler(ad_request_id):

            # Ad server process initiation
            feedback_handler.ad_request_id = ad_request_id
            feedback_handler.handlerProcess()

        # Hosting web service at localhost and port 8000 
        app.run(host="0.0.0.0", port=8000)

    except KeyboardInterrupt:
        print("press control-c again to quit")

    finally:
        if feedback_handler is not None:
            del feedback_handler