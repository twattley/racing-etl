from src.entity_matching.matcher import entity_match

def entity_matching_pipeline():
    entity_match('jockey')
    entity_match('trainer')
    entity_match('horse')
    entity_match('sire')
    entity_match('dam')


if __name__ == '__main__':
    entity_matching_pipeline()
