import requests
import logging

from dataclasses import dataclass

from utils.logger import log


class CommonService:
    def __init__(self):
        self.logger = logging

    @log
    def check_lookup(self, lookup_url: str, key: str):
        req = requests.get(lookup_url)
        if req.ok:
            return req.json()[key]
        else:
            self.logger.warning("Key not found")
            return None
