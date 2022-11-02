from ...utils.reponse_generator import generateResponse

import requests
import xml.etree.ElementTree as et
import pandas as pd
import logging

class VotationExtractor():

    def execute(self):
        try: 
            senators = pd.read_csv("project/data/list_senators.csv", sep = ";")
            data = {
                "codigoPalamentar": [],
                "CodigoSessao": [],
                "CodigoSessaoLegislativa": [],
                "NumeroSessao": [],
                "DataSessao": [],
                "HoraInicioSessao": [],
                "CodigoMateria": [],
                "IdentificacaoProcesso": []
            }
            for codigoPalamentar in senators["codigoPalamentar"]:
                url = f"https://legis.senado.leg.br/dadosabertos/senador/{codigoPalamentar}/votacoes"
                print("URL: ", url, " codigo: ", codigoPalamentar)
                r = requests.get(url)
                node = et.fromstring(r.content)
                for votacao in node.iter("Votacao"):
                    data["codigoPalamentar"].append(codigoPalamentar)
                    data["CodigoSessao"].append(votacao[0][0].text)
                    data["CodigoSessaoLegislativa"].append(votacao[0][2].text)
                    data["NumeroSessao"].append(votacao[0][4].text)
                    data["DataSessao"].append(votacao[0][5].text)
                    data["HoraInicioSessao"].append(votacao[0][6].text)
                    data["CodigoMateria"].append(votacao[1][0].text)
                    data["IdentificacaoProcesso"].append(votacao[1][1].text)

            dfVotationBySenator = pd.DataFrame().from_dict(data)
            dfVotationBySenator.to_csv("project/database/bronze/votation_by_senator/votation_by_senator.csv")

            return generateResponse(msg="Wrote votation by senator in file system")

        except Exception as exp:
            logging.error(f"ERROR: {exp}")

            return generateResponse(type="ERROR", msg=str(exp))

