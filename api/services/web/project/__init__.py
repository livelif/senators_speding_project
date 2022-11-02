import logging

from flask import Flask, jsonify, request

from .base_code.base_gold import BaseGold
from .base_code.base_silver import BaseSilver
from .extract.senators.senators_extract import SenatorsExtractor
from .extract.votation_by_senator.votation_by_senator_extract import \
    VotationExtractor

app = Flask(__name__)

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()


@app.route('/')
def hello():
    return 'Welcome.'


@app.route("/extract/senators")
def get_senators():
    response = SenatorsExtractor().execute()
    logging.info(f"RESPONSE: {response}")
    return jsonify(response)

@app.route("/extract/votation_by_senator")
def get_votation_by_senator():
    response = VotationExtractor().execute()
    logging.info(f"RESPONSE: {response}")
    return jsonify(response)

@app.route("/silver/<className>")
def get_silver(className):
    response = BaseSilver(spark, className).execute()
    logging.info(f"RESPONSE: {response}")
    return jsonify(response)

@app.route("/gold/<className>")
def get_gold(className):
    response = BaseGold(spark, className).execute()
    logging.info(f"RESPONSE: {response}")
    return jsonify(response)