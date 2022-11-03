from mongoHandler import MongoHandler
from pymongo import MongoClient


def export_nominations_to_collection():
    client = MongoClient()
    collection = client.nobel['nominations']
    handler = MongoHandler("people")
    for person in handler.get_all():
        person_id = person['id']
        for nominator_id, nominations in person['nominations'].items():
            for nomination in nominations:
                nomination = {
                        "nominee_id": person_id,
                        "nominee_name": person['name'],
                        "nominator_id": nominator_id,
                        "nominator_name": handler.get_person_by_id(nominator_id)['name'],
                        "year": nomination['year'],
                        "type": nomination['type']
                        }
                collection.insert_one(nomination)


def export_yearly_nom_count_for_each_nominee(outputFile):
    collection = MongoClient().nobel['nominations']
    handler = MongoHandler("people")
    file = open(outputFile, mode="w", errors="replace")
    header = "nominee_id,nominee_name,year_NP"
    for year in range(1901, 1972):
        header += ","+str(year)
    file.write(header+"\n")

    for nominee in handler.get_all():
        line = ""
        line += nominee['id']+","
        line += "\""+nominee['name']+"\","
        first_ch_win = get_first_ch_win(nominee['nobel'])
        if first_ch_win is not None:
            line += str(first_ch_win)
        else:
            line += "None"
        for year in range(1901, 1972):
            count = collection.count_documents({"year": str(year),
                                                "nominee_id": nominee['id']})
            line += ","+str(count)
        file.write(line+"\n")
    file.close()


def export_yearly_nominators_count(outputFile):
    collection = MongoClient().nobel['nominations']
    handler = MongoHandler("people")
    file = open(outputFile, mode="w", errors="replace")
    header = "nominee_id,nominee_name,year_NP"
    for year in range(1901, 1972):
        header += ","+str(year)
    file.write(header+"\n")

    for nominee in handler.get_all():
        line = ""
        line += nominee['id']+","
        line += "\""+nominee['name']+"\","
        first_ch_win = get_first_ch_win(nominee['nobel'])
        if first_ch_win is not None:
            line += str(first_ch_win)
        else:
            line += "None"
        for year in range(1901, 1972):
            # equivalent to select distinct(nominator_id),nominee_id,
            # nominee_name from nominations.csv
            # where nominee_id=<id> and year=<year>
            count = list(collection.aggregate([
                        {
                            '$match': {
                                'year': str(year),
                                'nominee_id': nominee['id']
                            }
                        }, {
                            '$group': {
                                '_id': '$nominator_id'
                            }
                        }, {
                            '$count': 'n'
                        }]))
            line += ","+str(0 if not count else count[0]['n'])
        file.write(line+"\n")
    file.close()


def get_first_ch_win(wins):
    if not wins:
        return None
    lowest_win = None
    for win in wins:
        if win['type'] == "C" and (lowest_win is None or int(win['year']) < lowest_win):
            lowest_win = int(win['year'])
    return lowest_win


def main():
    export_nominations_to_collection()


if __name__ == "__main__":
    main()
