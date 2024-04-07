import json
import psycopg
import os;
from Constants import *

table_names = ['season', 'country', 'competition', 'competition_stage', 'stadium', 'referee', 
               'team', 'manager', 'manages', 'match', 'event_type', 'play_pattern', 'position',
                 'player', 'player_team', 'player_country', 'player_nickname', 'player_position', 
                 'body_part', 'event', 'height', 'pass_type', 'technique', 'outcome', 'pass',
                   'shot_type', 'shot', 'freeze_frame', 'injury_stoppage', 'tactic', 'lineup',
                     'card', 'interception', 'miscontrol', 'half_start', 'ball_receipt',
                       'ball_recovery', 'carry', 'dribble', 'clearance', 'substitution',
                         'fifty_fifty', 'duel_type', 'duel', 'foul_won', 'block', 'foul_type', 
                         'foul_committed', 'bad_behaviour', 'goal_type', 'goalkeeper']


def createTablesAndReferences():
    with open(os.path.abspath('./output.json'), encoding="utf8") as f:
        data = json.load(f);
        uploadTables(data)
        createReferences()

def connectToDatabase():
    return psycopg.connect(dbname=DATABASE_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT);



def uploadTables(data):
    for table_name in table_names:
            try:
                with connectToDatabase() as conn:
                    cursor = conn.cursor()
                    with cursor.copy(f"COPY {table_name} FROM STDIN") as copy:
                        for record in data[table_name]["values"]:
                            copy.write_row(record)
            except Exception as e:
                print(table_name)
                print(e)


references = {
    "competition": [("season_id", "season", "season_id")],
    "stadium": [("country_id", "country", "id")],
    "referee": [("country_id", "country", "id")],
    "team": [("country_id", "country", "id")],
    "manager": [("country_id", "country", "id")],
    "manages": [("manager_id", "manager", "id"),
                ("team_id", "team", "team_id")],
    "match": [
              ("competition_id,season_id", "competition", "competition_id,season_id", "competition_id_season_id"),
              ("competition_stage_id", "competition_stage", "id"),
              ("stadium_id", "stadium", "id"),
              ("referee_id", "referee", "id"),
              ("home_team_id", "team", "team_id"),
              ("away_team_id", "team", "team_id")],
    "player_country": [
         ("player_id", "player", "player_id"),
         ("country_id", "country", "id")
    ],
    "player_team": [
         ("player_id", "player", "player_id"),
         ("team_id", "team", "team_id"),
    ],
    "player_nickname": [
         ("player_id", "player", "player_id")
    ],
    "player_position": [
         ("match_id", "match", "match_id"),
         ("player_id", "player", "player_id"),
         ("position_id", "position", "id")
    ],
    "event": [
         ("match_id", "match", "match_id"),
         ("type_id", "event_type", "id"),
         ("play_pattern_id", "play_pattern", "id"),
         ("player_id", "player", "player_id"),
         ("team_id", "team", "team_id"),
         ("possession_team_id", "team", "team_id"),
         ("position_id", "position", "id"),
    ],
    "pass":[
         ("event_id", "event", "id"),
         ("recipient_id", "player", "player_id"),
         ("outcome_id", "outcome", "id"),
         ("assisted_shot_id", "event", "id"),
         ("body_part_id", "body_part", "id"),
         ("pass_type_id", "pass_type", "id"),
         ("technique_id", "technique", "id"),
    ],
    "shot": [
         ("event_id", "event", "id"),
         ("key_pass_id", "event", "id"),
         ("outcome_id", "outcome", "id"),
         ("body_part_id", "body_part", "id"),
         ("shot_type_id", "shot_type", "id"),
         ("technique_id", "technique", "id"),
    ],
    "freeze_frame":[
         ("event_id", "event", "id"),
         ("player_id", "player", "player_id"),
         ("position_id", "position", "id"),
    ],
    "injury_stoppage":[
         ("event_id", "event", "id"),
    ],
    "tactic": [
         ("event_id", "event", "id"),
         ("position_id", "position", "id"),
         ("player_id", "player", "player_id"),
    ],
    "lineup": [
         ("match_id", "match", "match_id"),
         ("player_id", "player", "player_id"),
    ],
    "interception":[
         ("event_id", "event", "id"),
    ],
    "miscontrol": [
         ("event_id", "event", "id"),
    ],
     "half_start": [
         ("event_id", "event", "id"),
    ],
     "ball_receipt": [
         ("event_id", "event", "id"),
         ("outcome_id", "outcome", "id"),
    ],
    "ball_recovery": [
         ("event_id", "event", "id"),
    ],
    "carry": [
         ("event_id", "event", "id"),
    ],
    "dribble": [
         ("event_id", "event", "id"),
         ("outcome_id", "outcome", "id"),
    ],
    "clearance": [
         ("event_id", "event", "id"),
         ("body_part_id", "body_part", "id"),
    ],
    "substitution": [
         ("event_id", "event", "id"),
         ("replacement_id", "player", "player_id"),
         ("outcome_id", "outcome", "id"),
    ],
    "fifty_fifty":[
         ("event_id", "event", "id"),
         ("outcome_id", "outcome", "id"),
    ],
    "duel":[
         ("event_id", "event", "id"),
         ("duel_type_id", "duel_type", "id"),
         ("outcome_id", "outcome", "id"),
    ],
    "foul_won": [
         ("event_id", "event", "id"),
    ],
    "block": [
         ("event_id", "event", "id"),
    ],
    "foul_committed": [
         ("event_id", "event", "id"),
         ("card_id", "card", "id"),
         ("foul_type_id", "foul_type", "id"),
    ],
    "bad_behaviour": [
         ("event_id", "event", "id"),
         ("card_id", "card", "id"),
    ],
    "goalkeeper":[
        ("event_id", "event", "id"),
        ("technique_id", "technique", "id"),
        ("position_id", "position", "id"),
        ("body_part_id", "body_part", "id"),
        ("outcome_id", "outcome", "id"),
        ("goal_type_id", "goal_type", "id"),
    ]

}   

#  for table_name in references:
def createReferences():
        for table in references:
            try:
                with connectToDatabase() as conn:
                    cursor = conn.cursor()
                    for reference in references[table]:
                        constraint_name = reference[0]
                        if(len(reference) > 3):
                            constraint_name = reference[3]
                        query = f"""
                            ALTER TABLE {table}
                            ADD CONSTRAINT fk_{table}_{constraint_name}
                            FOREIGN KEY ({reference[0]}) 
                            REFERENCES {reference[1]}({reference[2]});
                        """
                        cursor.execute(query)
            except Exception as e:
                 print(e)
                 
indexes = {"event": ["type_id"]
           }
def createIndexes():
     for table in indexes:
            try:
                with connectToDatabase() as conn:
                    cursor = conn.cursor()
                    for index in indexes[table]:
                        query = f"""
                           CREATE INDEX idx_{table}_{index} ON {table}({index})
                        """
                        cursor.execute(query)
            except Exception as e:
                 print(e)

def dropIndexes():
     for table in indexes:
            try:
                with connectToDatabase() as conn:
                    cursor = conn.cursor()
                    for index in indexes[table]:
                        query = f"""
                           DROP INDEX if exists idx_{table}_{index}
                        """
                        cursor.execute(query)
            except Exception as e:
                 print(e)

def dropAndCreateIndexes():
     createIndexes()
     dropIndexes()