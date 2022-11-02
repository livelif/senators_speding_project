import logging
import xml.etree.ElementTree as et

import pandas as pd
import requests

from ...utils.reponse_generator import generateResponse


class SenatorsExtractor():

    def execute(self):
        logging.info("Getting senators")
        try:
            r = requests.get("https://legis.senado.leg.br/dadosabertos/senador/lista/atual")
            node = et.fromstring(r.content)
            data = {
                "codigoPalamentar": [],
                "NomeParlamentar": [],
                "NomeCompletoParlamentar": []
            }
            for palamentar in node.iter("IdentificacaoParlamentar"):
                data["codigoPalamentar"].append(palamentar[0].text)
                data["NomeParlamentar"].append(palamentar[2].text)
                data["NomeCompletoParlamentar"].append(palamentar[3].text)

            
            dfPalamentar = pd.DataFrame().from_dict(data)
            dfPalamentar.to_csv("project/database/bronze/senators/list_senators.csv", sep=";")
            logging.info("Wrote senators in file system")

            return generateResponse(msg="Wrote senators in file system")
        except Exception as exp:
            logging.error(f"ERROR {exp}")
            return generateResponse(type="ERROR", msg=str(exp))