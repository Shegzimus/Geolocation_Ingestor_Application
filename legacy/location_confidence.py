import pandas as pd
import json
import ast
from textblob import TextBlob
import spacy
from rapidfuzz import process, fuzz
from spacy.matcher import Matcher
import re


class LocationConfidence:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm", disable=["parser","tagger"])
        self.address_matcher = Matcher(self.nlp.vocab)
        
        
        self.irish_street_suffixes = [
            "street","st",
            "road","rd",
            "avenue","ave",
            "park","crescent","terrace","place","square","gardens",
            "court","lane","close","view"
        ]
        
        self.address_pattern = [
            # house number
            {"LIKE_NUM": True},
            # street name (one or more words)
            {"IS_ALPHA": True, "OP": "+"},
            # an Irish street suffix
            {"LOWER": {"IN": self.irish_street_suffixes}}
        ]
        
        # self.restaurants:pd.DataFrame = pd.read_csv("dublin_restaurants_20250422_204744.csv", header=0)
        # self.name:list    = self.restaurants["name"].tolist()
        # self.vicinity:list = self.restaurants["vicinity"].tolist()

    def match_name_and_vicinity(self, token: str, name_list: list, vicinity_list: list) -> dict:
        """
        Matches a token against lists of restaurant names and vicinities.
        """
        # match against names
        nm, nm_score, nm_idx = process.extractOne(token, name_list, scorer=fuzz.token_set_ratio)
        # match against vicinities
        vc, vc_score, vc_idx = process.extractOne(token, vicinity_list, scorer=fuzz.token_set_ratio)

        # normalize to 0–1
        nm_conf = nm_score / 100.0
        vc_conf = vc_score / 100.0

        # pick whichever field gave the higher confidence
        if nm_conf >= vc_conf:
            return {
                "matched_field": "name",
                "matched_value": nm,
                "restaurant_id": name_list[nm_idx],  # or some proper ID if available
                "confidence": nm_conf
            }
        else:
            return {
                "matched_field": "vicinity",
                "matched_value": vc,
                "restaurant_id": name_list[vc_idx],  # using name as ID from the vicinity match
                "confidence": vc_conf
            }
        

    def get_places_from_transcript(self, text: str):
        """
        Extracts place names from the given text using spaCy.
        """
        # nlp = spacy.load("en_core_web_sm", disable=["parser", "tagger"])


        # Matcher for Irish street addresses
        address_matcher = Matcher(self.nlp.vocab)
        
        address_matcher.add("ADDRESS", [self.address_pattern])

        eircode_re = re.compile(
        r"\b[ACDEFHKNPRTV-Y]\d{2}\s?[0-9ACDEFHKNPRTV-Y]{4}\b",
        re.IGNORECASE
)
        dublin_re = re.compile(r"\bDublin\s?\d{1,2}\b", re.IGNORECASE)


        doc = self.nlp(text)
        tokens = []


        for ent in doc.ents:
            if ent.label_ in ("GPE","LOC","FAC","ORG"):
                tokens.append({"text": ent.text, "type": ent.label_})

        for _, start, end in address_matcher(doc):
            span = doc[start:end]
            tokens.append({"text": span.text, "type": "ADDRESS"})

        for m in eircode_re.finditer(text):
            code = m.group(0)
            if not any(r["text"] == code for r in tokens):
                tokens.append({"text": code, "type": "EIRCODE"})

        for m in dublin_re.finditer(text):
            district = m.group(0)
            if not any(r["text"] == district for r in tokens):
                tokens.append({"text": district, "type": "DISTRICT"})

        return tokens
    
    def refine_address_span(self, span_text: str) -> list[str]:
        """
        Given a long ADDRESS span, rerun the Matcher to pull out
        the minimal house# + street-name + suffix matches.
        """
        doc = self.nlp(span_text)
        results = []
        for _, start, end in self.address_matcher(doc):
            addr = doc[start:end].text
            if addr not in results:
                results.append(addr)
        return results

    def run():
        restaurants = pd.read_csv("dublin_restaurants_20250422_204744.csv", header=0)
        transcript_df = pd.read_csv(
            "restaurant_video_transcripts_flat.csv",
            )
        
        merged = pd.merge(
            restaurants,
            transcript_df,
            left_on="name",
            right_on="restaurant_name",
            how="right"
        )
        to_csv = merged.to_csv(
            "merged.csv",
            index=False
        )
        print(merged.columns)
        # text:str = "hi I'm time from Char last March we tweeted saying. Brunch was dead and then at the Sunday Rosa taking it's about the severance favorite Camino me lies of the week and that did seem to be the case in Dublin for a bit but then it plateaued the 2024 showing it Sunday roast fever is picking up pace again and Dublin unless I'm going for at the end of the week especially one that we don't have to make herself so here's our list of nine of the best Sunday Road in Dublin let us know if we should do a car for a version the old spot on Bath Avenue with a cozy friendly Delight of the place what are Europe the bar or down in the booth it's a real trees it's very dog friendly on the wine list is 5 choice of chicken or beef roast come in a 28-0 the donkey donk is another dog friendly spots but with a more casual feel but there was still dumb delicious a 2250 do you choose between beef sirloin or roast chicken and it will not leave you wanting when 57 the headline bar on some brothel Street closed last year heart's Broke Girls"


        # # load once, outside your loop
        # nlp = spacy.load("en_core_web_sm", disable=["parser","tagger"])
        # address_matcher = Matcher(nlp.vocab)

        # # … set up your irish_street_suffixes + address_pattern as before …
        # address_matcher.add("ADDRESS", [self.address_pattern])

        # flattened_tokens = self.get_places_from_transcript(text)

        # # for every ADDRESS span, pull out the minimal matches
        # refined_addrs = []
        # for item in flattened_tokens:
        #     if item["type"] == "ADDRESS":
        #         refined_addrs.extend(
        #             self.refine_address_span(item["text"])
        #         )

        # for addr in refined_addrs:
        #     match = self.match_name_and_vicinity(addr, ["Giu"], ["74 North Strand Road, Dublin 3"])
        #     print(match)

LocationConfidence.run()