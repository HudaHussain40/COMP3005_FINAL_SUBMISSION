import psycopg
import json;
from Constants import *
import os;
from pathlib import Path

seasons = {"2020/2021", "2019/2020", "2018/2019", "2003/2004"}
competition_ids = set()
#competition_ids = {'37', '11', '16', '2'}
#season_ids = {'42', '90', '44','4'}
season_ids = set()
required_columns = {}

types = {"position":set(), "event_type":set(), "play_pattern":set(), 
"position": set(), "body_part": set(), "height":set(), "pass_type":set(), "technique":set(),
"outcome": set(), "shot_type":set(), "card_type": set(), "duel_type": set(), "foul_type": set()}

def connectToDatabase():
    # Open a cursor to perform database operations
    return psycopg.connect(dbname=DATABASE_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT);

tables_and_columns = {}
def retrieveTablesAndColumns():
    with connectToDatabase() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        table_names = cursor.fetchall()
        for table_name in table_names:
            table_name = table_name[0]
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'  ORDER BY ordinal_position;")
            # Fetch all column names
            res = cursor.fetchall()
            columns = []
            for data in res:
                columns.append(data[0])
            # Store table name and its columns in the dictionary
            tables_and_columns[table_name] = {"columns": columns, "insert_statment": f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES", "values":set()} 
            

def loadData():
    loadCompetitions()
    loadMatches()

def loadCompetitions():
    with open(os.path.abspath('./data/competitions.json'), encoding="utf8") as f:
        data = json.load(f);
        for competition in data:
            if(competition['season_name'] in seasons):
                addSeason({"season_name": competition['season_name'], "season_id":competition['season_id']})
                addCompetition(competition)
            

def addSeason(season):
    if createInsertSQL(season, "season"):
        season_ids.add(str(season["season_id"]))

def addCompetition(competition):
    if createInsertSQL(competition, "competition", ):
         competition_ids.add(str(competition["competition_id"]))

    

def loadMatches():
    competitions = os.listdir(os.path.abspath('./data/matches'));
    for competition in competitions:
        if(competition in competition_ids):
            seasons = os.listdir(os.path.abspath(f'./data/matches/{competition}'));
            for season in seasons:
                if(Path(season).stem in season_ids):
                    with open(os.path.abspath(f'./data/matches/{competition}/{season}'), encoding="utf8") as f:
                        data = json.load(f);
                        for match in data:
                            match["competition_id"] = match["competition"]["competition_id"]
                            match["season_id"] = match["season"]["season_id"]
                            addMatch(match)
                            
                            with open(os.path.abspath(f'./data/lineups/{match["match_id"]}.json'), encoding="utf8") as f:
                                data = json.load(f)
                                for lineup in data:
                                    lineup["match_id"] = match["match_id"]
                                    addLineUp(lineup)
                            with open(os.path.abspath(f'./data/events/{match["match_id"]}.json'), encoding="utf8") as f:
                                data = json.load(f)
                                for event in data:
                                    event["match_id"] = match["match_id"]
                                    addEvent(event)



def addMatch(match):
    
    if("away_team" in match):
        match["away_team_id"] =  match["away_team"]["away_team_id"]
        match["away_team"] = cleanKeys(match["away_team"], 'away_')
        addTeam(match['away_team'])
    if("home_team" in match):
        match["home_team_id"] =  match["home_team"]["home_team_id"]
        match["home_team"] = cleanKeys(match["home_team"], 'home_')
        addTeam(match['home_team'])
    if("stadium" in match):
        addStadium(match["stadium"])
    if("competition_stage" in match):
        createInsertSQL(match["competition_stage"], "competition_stage")
    if("referee" in match):
        addReferee(match["referee"])
    match = flattenAndCompressKeys(match)
    createInsertSQL(match, "match", )



def addStadium(stadium):
    if("country" in stadium):
        addCountry(stadium["country"])
    createInsertSQL(stadium, "stadium")

def addReferee(referee):
    if("country" in referee):
        addCountry(referee["country"])
    createInsertSQL(referee, "referee")

def addTeam(team):

    addCountry(team["country"])
    team["country_id"] = team["country"]["id"]
    del team["country"]
    managers = None
    if("managers" in team):
        managers = team["managers"]
        del team["managers"]
    createInsertSQL(team, "team")
    if(managers):
        for manager in managers:
            addManager(manager)
            createInsertSQL({"manager_id": manager["id"], "team_id": team["team_id"]}, "manages")

def addCountry(country):
     createInsertSQL(country, "country")

def addManager(manager):
    addCountry(manager["country"])
    manager["country_id"] = manager["country"]["id"]
    del manager["country"]
    createInsertSQL(manager, "manager")
    

def cleanKeys(dictionary, removeWord):
    new_dict = {}
    for key in dictionary:
        new_dict[key.replace(removeWord,'')] = dictionary[key]
    return new_dict

#make sure this works
def createInsertSQL(dictionary, table_name):

        dictionary = flattenAndCompressKeys(dictionary)
        if(table_name in tables_and_columns):
            insert = []
            for key in tables_and_columns[table_name]["columns"]:
                value = dictionary.get(key, None)
                if isinstance(value, str):
                    value = value.replace(',','')
                insert.append(value)

            tables_and_columns[table_name]["values"].add(tuple(insert))
        return(True)
     
def flattenAndCompressKeys(dictionary, separator='_', parent_key=''):
    items = []
    for key, value in dictionary.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flattenAndCompressKeys(value, separator, new_key).items())
        else:
            items.append((new_key, value)) 
    return dict(items)


def addLineUp(lineup):
    for player in lineup["lineup"]:
        addCountry(player["country"])
        positions = player["positions"]
        del player['positions']
        del player['cards']
        player["team_id"] = lineup["team_id"]
        createInsertSQL({'match_id': lineup["match_id"], 'player_id': player["player_id"]}, "lineup")
        if("country" in player):
            
            createInsertSQL({"country_id": player["country"]["id"],"player_id": player["player_id"]},"player_country", )
            
        createInsertSQL({"team_id": lineup["team_id"],"player_id": player["player_id"]},"player_team", )
        player = flattenAndCompressKeys(player)
        if("player_nickname" in player and player["player_nickname"]):
            createInsertSQL({"player_id": player["player_id"],"player_nickname": player["player_nickname"]} ,"player_nickname", )
        createInsertSQL(player,"player", )
        
        for player_position in positions:
            createInsertSQL({"name": player_position["position"], "id": player_position["position_id"]}, "position", )
            player_position["player_id"] = player["player_id"]
            player_position["from_time"] = convertmstohms(player_position["from"])
            player_position["to_time"] = convertmstohms(player_position["to"])
            player_position["match_id"] = lineup["match_id"]
            player_position["jersey_number"] = player["jersey_number"]
            createInsertSQL(player_position, "player_position", )
            #player cards are handled in events

def addEvent(event):
        event["type"]["name"] = event["type"]["name"].replace('*','')
        createInsertSQL(event["type"], "event_type")
        event["type_id"] = event["type"]["id"]
        event_type_name = event["type"]["name"].lower().replace(" ", "_").replace("*","")
        if(event_type_name == '50/50'):
            event_type_name = 'fifty_fifty'
        if (event_type_name == 'goal_keeper'):
            event_type_name = 'goalkeeper'

        del event["type"]
        if("play_pattern" in event):
            event["play_pattern_id"] = event["play_pattern"]["id"]
            createInsertSQL(event["play_pattern"], "play_pattern")
            del event["play_pattern"]
        if("position" in event):
            createInsertSQL(event["position"], "position")

        broad = ["player", "team", "possession_team", "position"]
        extractKeyID(event, broad)
        if("related_events" in event):
            del event["related_events"]
        tactics = None

        if("tactics" in event):
            tactics = event["tactics"]
            del event["tactics"]


        if(event_type_name in tables_and_columns):
            event_values = {}
            if(event_type_name == 'fifty_fifty'):
                if('50_50' in event):
                    event_values = event['50_50']
                    del event['50_50']
            else:
                if(event_type_name in event):
                    event_values = event[event_type_name]
                    del event[event_type_name]
            event_values["event_id"] = event["id"]
            addEventValues(event_values, event_type_name)
        
        expandLocation(event)
        try:
            createInsertSQL(event, "event")
        except Exception as e:
            print(e)
            print("couldn't add event to database")

            
        
        if(tactics):
            lineups = tactics["lineup"]
            for lineup in lineups:
                lineup["event_id"] = event["id"]
                lineup["player_id"] = lineup["player"]["id"]
                lineup["position_id"] = lineup["position"]["id"]
                createInsertSQL(lineup["position"], "position")

                lineup["formation"] = tactics["formation"]
                del lineup["player"]
                del lineup["position"]
                del lineup["jersey_number"]
                createInsertSQL(lineup, "tactic")
       
def extractKeyID(dictionary, extract):
    for key in extract:
        if key in dictionary:
            new_key = key+"_id"
            dictionary[new_key] = dictionary[key]["id"]
            del dictionary[key]

def addEventValues(event_values, event_table_name):

    if("location" in event_values or "end_location" in event_values):
        expandLocation(event_values)
    if("freeze_frame" in event_values):
        for freeze_frame in event_values["freeze_frame"]:
            expandLocation(freeze_frame)
            
            freeze_frame["player_id"] = freeze_frame["player"]["id"]
            del freeze_frame["player"]
            freeze_frame["position_id"] = freeze_frame["position"]["id"]
            
            freeze_frame["event_id"] = event_values["event_id"]
            createInsertSQL(freeze_frame["position"], "position")
            del freeze_frame["position"]
            createInsertSQL(freeze_frame, "freeze_frame")
        del event_values["freeze_frame"]

    broad_events = ["body_part", "outcome","technique","position","card","height"]

    for event_type in broad_events:
        if(event_type in event_values):
            createInsertSQL(event_values[event_type], event_type)
            key = event_type+"_id"
            event_values[key] = event_values[event_type]["id"]
            del event_values[event_type]
    
    if("duel" == event_table_name):
        if("type" in event_values):
            event_values["duel_type_id"] = event_values["type"]["id"]
            createInsertSQL(event_values["type"], "duel_type")
            del event_values["type"]
    elif("pass" == event_table_name):
         if("type" in event_values):
            event_values["pass_type_id"] = event_values["type"]["id"]
            createInsertSQL(event_values["type"], "pass_type")
            del event_values["type"]
            
    elif("shot" == event_table_name):
         if("type" in event_values):
            event_values["shot_type_id"] = event_values["type"]["id"]
            createInsertSQL(event_values["type"], "shot_type")
            del event_values["type"]
    elif("foul" in event_table_name):
        if("type" in event_values):
            event_values["foul_type_id"] = event_values["type"]["id"]
            createInsertSQL(event_values["type"], "foul_type")
            del event_values["type"]
    elif("goalkeeper" == event_table_name):
         if("type" in event_values):
            event_values["goal_type_id"] = event_values["type"]["id"]
            createInsertSQL(event_values["type"], "goal_type")
            del event_values["type"]
    elif("substitution" == event_table_name):
         if("replacement" in event_values):
            event_values["replacement_id"] = event_values["replacement"]["id"]
            del event_values["replacement"]

    if("cross" in event_values):
        event_values["cross_"] = event_values["cross"]
        del event_values["cross"]
    if("recipient" in event_values):
            event_values["recipient_id"] = event_values["recipient"]["id"]
            del event_values["recipient"]
    try:
        createInsertSQL(event_values, event_table_name)
    except Exception as e:
        print("cant add event_type to database")
    

   

def convertmstohms(minutes_seconds):
    if(minutes_seconds):
        minutes, seconds = map(int, minutes_seconds.split(':'))
        hours = minutes // 60
        remaining_minutes = minutes % 60
        return f"{hours:02d}:{remaining_minutes:02d}:{seconds:02d}"
    else:
        return None

def expandLocation(dictionary):
    coordinates = ['x','y','z']
    for i in range(len(coordinates)):
        coordinate = coordinates[i]
        if ("location" in dictionary):
            key = 'location_'+coordinate
            if(i < len(dictionary["location"])):
                dictionary[key] = dictionary["location"][i]
        elif ("end_location" in dictionary):
            key = 'end_location_'+coordinate
            if(i < len(dictionary["end_location"])):
                dictionary[key] = dictionary["end_location"][i]
    if("location" in dictionary):
        del dictionary["location"]
    elif("end_location" in dictionary):
        del dictionary["end_location"]
    return

def tableExists(table_name):
    with connectToDatabase() as conn:
        cursor = conn.cursor()
        try: 
            cursor.execute("select match_id from match")
            cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_name = %s
                    )
                """, (table_name,))

                # Fetch the result
            exists = cursor.fetchone()[0]
            return exists
        except Exception as e:
            print(e)

def default(o):
    if isinstance(o, set):
        return list(o)  # Convert set to list
    raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')

def writeToJson(dictionary, file_path):
    with open(file_path, 'w') as json_file:
        json.dump(dictionary, json_file, indent=3, default=default)



def loadAll():
    retrieveTablesAndColumns()
    loadData()
    writeToJson(tables_and_columns, 'output.json')