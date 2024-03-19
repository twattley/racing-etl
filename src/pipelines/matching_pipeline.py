from src.entity_matching.matcher import entity_match
from src.storage.sql_db import call_procedure


def run_entity_matching_pipeline():

    call_procedure("load_direct_matches", "public")
    entity_match("jockey")
    entity_match("trainer")
    entity_match("horse")
    entity_match("sire")
    entity_match("dam")
    call_procedure("load_fuzzy_matches", "public")


if __name__ == "__main__":
    run_entity_matching_pipeline()
