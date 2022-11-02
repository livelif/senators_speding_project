import logging

from ..silver.benefits.benefits_silver import Benefits
from ..silver.remuneration.remuneration_silver import Remuneration
from ..silver.senators.senators_silver import SenatorsSilver
from ..silver.votation_by_senator.votation_by_senator_silver import \
    VotationBySenatorSilver
from ..utils.reponse_generator import generateResponse


class BaseSilver():
    def __init__(self, spark, className):
        self.spark = spark
        self.className = className

    def execute(self):
        logging.info(f"PARAM: {self.className}")
        if self.className == "remuneration":
            return Remuneration(self.spark).execute()
    
        if self.className == "benefits":
            return Benefits(self.spark).execute()
        
        if self.className == "senators":
            return SenatorsSilver(self.spark).execute()

        if self.className == "votation_by_senator":
            return VotationBySenatorSilver(self.spark).execute()

        return generateResponse("ERROR", "Invalid parameter")
