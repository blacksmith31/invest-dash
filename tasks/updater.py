
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from .trigger import ContinuousSubweekly
    from tasks.resources import ResourceBase
    from schemas.schemas import Symbol

class Updater(ABC):

    def pre_fetch(self):
        raise NotImplementedError

    def fetch(self):
        raise NotImplementedError

    def post_fetch(self):
        raise NotImplementedError

    def save(self):
        raise NotImplementedError

    @abstractmethod
    def update(self):
        pass


class SymbolUpdater(Updater):
    
    def __init__(self, resource: ResourceBase, trigger: ContinuousSubweekly, strategy=None):
        self.resource = resource
        self.trigger = trigger
        self.strategy = strategy

    def fetch(self) -> List[dict]:
        return self.resource.get()
    
    def validate(self, syms: List[dict]) -> List[Symbol]:
        updated = []
        # keys = ["symbol", "name", "marketCap", "country", "industry", "sector"]
        for sym in syms: 
            try:
                updated.append(Symbol.model_validate(sym))
            except:
                print(f"Symbol Update| input symbol: {sym['symbol']}, mktcap: {sym['marketCap']}")
                raise
        return updated
