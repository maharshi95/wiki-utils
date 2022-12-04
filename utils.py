import json
from typing import Optional
import requests

from . import cache
from . import wiki_properties as wp

DEFAULT_TIMEOUT = 30


class WikiClient(cache.Cached):
    def __init__(self, cache_filepath: Optional[str] = None, tag: Optional[str] = None):
        self.api_url = "https://www.wikidata.org/w/api.php"
        self.timeout = DEFAULT_TIMEOUT
        super().__init__(cache_filepath, tag)

    def read_cache_from_file(self, filepath):
        with open(filepath) as fp:
            return json.load(fp)

    def save_cache_to_file(
        self, filepath=None, tags: cache.Strings = None, pretty=False
    ):
        filepath = filepath or self.cache_filepath
        if filepath is None:
            raise ValueError(
                "No default filepath set for cache. Please provide `filepath`."
            )
        with open(filepath, "w") as fp:
            indent = 4 if pretty else None
            json.dump(self.get_caches(tags), fp, indent=indent, sort_keys=True)

    def wbgetentities(self, ids: str, lang: Optional[str] = "en"):
        resp = requests.get(
            url=self.api_url,
            params={
                "action": "wbgetentities",
                "ids": ids,
                "languages": lang,
                "format": "json",
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def wbgetclaims(self, entity: str, prop: str):
        resp = requests.get(
            url=self.api_url,
            params={
                "action": "wbgetclaims",
                "entity": entity,
                "property": prop,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    @cache.cached_method("entity_info")
    def get_entity_info(self, entity_qid, lang: str = "en"):
        obj = self.wbgetentities(entity_qid, lang)

        entity_dict = obj["entities"][entity_qid]
        info_dict = {
            key: entity_dict[key].get(lang, {"value": ""})["value"]
            for key in ["labels", "descriptions"]
        }
        # info_dict.update({
        #     'aliases': [a['value'] for a in entity_dict['aliases'][lang]]
        # })
        return info_dict

    def get_entity_name(self, entity_qid: str, lang: str = "en"):
        info_dict = self.get_entity_info(entity_qid, lang)
        return info_dict["labels"]

    @cache.cached_method("entity_prop")
    def get_entity_prop(self, qid: str, pid: str):
        resp = self.wbgetclaims(qid, pid)

        if "error" in resp and resp["error"]["code"] == "unresolved-redirect":
            entity_resp = self.wbgetentities(qid, "en")
            new_qid = entity_resp["entities"][qid]["redirects"]["to"]
            resp = self.wbgetclaims(new_qid, pid)

        if pid not in resp["claims"]:
            return []

        qids = [
            entry["mainsnak"]["datavalue"]["value"]["id"]
            for entry in resp["claims"][pid]
            if entry["mainsnak"]["snaktype"] == "value"
        ]
        return qids

    @cache.cached_method("entity_type")
    def get_entity_type(self, entity_qid: str, qids_only: bool = False):
        qids = self.get_entity_prop(entity_qid, wp.INSTANCE_OF)
        if qids_only:
            return qids
        names = [self.get_entity_name(qid) for qid in qids]
        return zip(qids, names)

    @cache.cached_method("entity_nationality")
    def get_nationality(self, person_qid: str):
        qids = self.get_entity_prop(person_qid, wp.COUNTRY_OF_CITIZENSHIP)
        names = [self.get_entity_name(qid) for qid in qids]
        return zip(qids, names)

    @cache.cached_method("entity_country")
    def get_country(self, location_qid: str):
        qids = self.get_entity_prop(location_qid, wp.COUNTRY)
        names = [self.get_entity_name(qid) for qid in qids]
        return zip(qids, names)

    @cache.cached_method("associated_country")
    def get_associated_country(self, entity_qid):
        countries = list(self.get_nationality(entity_qid))  # if the entity is a person
        if not countries:
            # if the entity is a location or a type that has a country associated with it
            countries = list(self.get_country(entity_qid))
        return countries


if __name__ == "__main__":
    wiki = WikiClient()
    qids = ["Q76", "Q34221"] * 3
    for qid in qids:
        print(qid, wiki.get_entity_name(qid))
        print("Country:", wiki.get_associated_country(qid))
    print(json.dumps(wiki.get_caches(), indent=4))
