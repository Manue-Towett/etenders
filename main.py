import os
from typing import Optional
from urllib.parse import urlencode
from datetime import date, datetime

import requests
import pandas as pd
from requests import Response

from utils import Logger

PARAMS = {"status": 1, "_": 1692458899698}

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.etenders.gov.za/Home/opportunities?id=1",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

BASE_URL = "https://www.etenders.gov.za{}"

class ETenderScraper:
    """Scrapes etenders from https://www.etenders.gov.za"""
    def __init__(self) -> None:
        self.logger = Logger(__class__.__name__)
        self.logger.info("*****ETender Scraper Started*****")

        self.tenders = []

    def __fetch_tenders(self) -> Optional[Response]:
        """Fetches tenders from https://www.etenders.gov.za"""
        self.logger.info("Fetching tenders...")

        for _ in range(3):
            try:
                response = requests.get(BASE_URL.format("/Home/TenderOpportunities/"), 
                                        params=PARAMS, 
                                        headers=HEADERS)

                if response.ok:
                    return response
                
            except:pass

            self.logger.warn("Fetching tenders failed! Retrying...")
        
        self.logger.error("FATAL ERROR: Couldn't retrieve tenders "
                          "after three trials!")
        
    def __extract_tenders(self, response: Response) -> None:
        """Extracts tenders from the response from the server"""
        try:
            tenders: list[dict[str, str|list[dict[str, str]]]] = response.json()

            self.logger.info(f"Tenders Extracted: {len(tenders)} "
                             "|| Filtering first 10 pages...")
        except:
            self.logger.error("FATAL ERROR: Failed to retrieve tenders!")
        
        for tender in tenders[:200]:
            try:
                tender["description"] = tender.get("description").capitalize()
            except:pass

            self.tenders.append({"Services": tender.get("category"),
                                 "Description": tender.get("description"),
                                 "Date": "",
                                 "Tender Number": tender.get("tender_No"),
                                 "Department": tender.get("department"),
                                 "Tender Type": tender.get("type"),
                                 "Province": tender.get("province"),
                                 "Date published": tender.get("dp"),
                                 "Closing date": tender.get("cd"),
                                 "Place where service will be required": tender.get("delivery"),
                                 "Special conditions": tender.get("conditions"),
                                 "Contact person": tender.get("contactPerson"),
                                 "Contact email": tender.get("email"),
                                 "Contact phone": tender.get("telephone"),
                                 "Contact fax": tender.get("fax"),
                                 "Briefing Session": tender.get("bf"),
                                 "Is briefing required": tender.get("bc"),
                                 "Briefing date and time": tender.get("brief"),
                                 "Briefing venue": tender.get("briefingVenue"),
                                 "Tender documents": tender.get("sd")})

            self.__remove_tab_spaces(self.tenders[-1])

            self.__format_dates(self.tenders[-1])
        
        self.logger.info("Filtered tenders: {}".format(len(self.tenders)))
    
    @staticmethod
    def __remove_tab_spaces(tender: dict[str, str]) -> None:
        """Removes tab spaces from a tender"""
        for key in list(tender.keys()):
            try:
                tender[key] = tender[key].replace("\t", "").strip()
            except:pass

            try:
                tender[key] = tender[key].replace(";", "")
            except:pass
    
    @staticmethod
    def __format_dates(tender: dict[str, str]) -> None:
        """Formats the dates in a specific format"""
        try:
            pub_date = tender["Date published"]

            date_ = datetime.strptime(pub_date, "%A, %d %B %Y")

            tender["Date published"] = date_.strftime("%d.%m.%Y")

            tender["Date"] = tender["Date published"].replace(".", "/")
        except:pass

        try:
            close_date = tender["Closing date"]

            c_date = datetime.strptime(close_date, "%A, %d %B %Y - %H:%M")

            tender["Closing date"] = c_date.strftime("%d.%m.%Y")
        except:pass
    
    def __extract_documents(self, 
                            tender: dict[str, str|list[dict[str, str]]]) -> None:
        """Extracts documents from a tender"""
        documents = []
        
        document_url = "/home/Download/?{}"

        if tender["Tender documents"]:
            for document in tender["Tender documents"]:
                try:
                    args = (document.get("supportDocumentID"),
                            document.get("extension"),
                            document.get("fileName"))
                    
                    url_params = urlencode({"blobName": args[0] + args[1],
                                            "downloadedFileName": args[-1]})
                    
                    documents.append(BASE_URL.format(document_url.format(url_params)))
                except:pass
        
        tender["Tender documents"] = ", ".join(documents)

        if tender["Briefing date and time"] == "<not available>":
            tender["Briefing date and time"] = ""

    def __save_to_csv(self) -> None:
        """Saves data retrieved to csv"""
        self.logger.info("Saving data to csv...")

        if not os.path.exists("./data/"):
            os.makedirs("./data/")
        
        df = pd.DataFrame(self.tenders).drop_duplicates()

        file_name = "results_{}.csv".format(date.today())

        df.to_csv(f"./data/{file_name}", index=False)

        self.logger.info("Data saved to {}".format(file_name))
    
    def run(self) -> None:
        """Entry point to the scraper"""
        response = self.__fetch_tenders()

        self.__extract_tenders(response)

        [self.__extract_documents(tender) for tender in self.tenders]

        self.__save_to_csv()


if __name__ == "__main__":
    app = ETenderScraper()
    app.run()