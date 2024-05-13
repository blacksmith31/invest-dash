
from abc import ABC, abstractmethod
from typing import List

from .trigger import ContinuousSubweekly

class Updater(ABC):

    @abstractmethod
    def pre_fetch(self):
        raise NotImplementedError

    @abstractmethod
    def fetch(self):
        raise NotImplementedError

    @abstractmethod
    def post_fetch(self):
        raise NotImplementedError

    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def update(self):
        pass


class SymbolUpdater(Updater):
    
    def __init__(self, resource, trigger: ContinuousSubweekly, strategy=None):
        self.resource = resource
        self.trigger = trigger
        self.strategy = strategy


    def fetch(self) -> List[dict]:
        # Getting data from json
        response = requests.get(
            url=url,
            headers=HEADERS,
            timeout=timeout
        )

        if response.status_code != 200:
            print(f"request for {url} failed")
            return []
        print(f"length: {len(response.json())} for {url.split('/')[-1]}")
        response_data += response.json()
    return response_data
