import psycopg
from Constants import *

commands = (("season","""
        CREATE TABLE season (
            season_id INT PRIMARY KEY,
            season_name TEXT NOT NULL
        )
        """),
        ("country",
        """
        CREATE TABLE country (
            id INT PRIMARY KEY,
            name TEXT NOT NULL
        )
        """),
        ("competition",
         """
        CREATE TABLE competition (
            season_id INT NOT NULL,
            competition_id INT NOT NULL,
            country_name TEXT NOT NULL,
            competition_name TEXT NOT NULL,
            competition_gender VARCHAR(7) NOT NULL,
            competition_youth BOOLEAN NOT NULL,
            competition_international BOOLEAN NOT NULL,
            PRIMARY KEY (season_id, competition_id)
        )
        """),
        ("competition_stage",
         """
            CREATE TABLE competition_stage (
                id INT NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
            )
        """),
        ("stadium",
         """
            CREATE TABLE stadium (
                id INT NOT NULL PRIMARY KEY,
                country_id INT,
                name TEXT NOT NULL
            )
        """),
        ("referee",
         """
            CREATE TABLE referee (
                id INT NOT NULL PRIMARY KEY,
                name TEXT NOT NULL,
                country_id INT NOT NULL
            )
        """),
        ("team",
         """
            CREATE TABLE team (
                team_id INT NOT NULL PRIMARY KEY,
                team_name TEXT NOT NULL,
                team_gender TEXT NOT NULL,
                team_group TEXT,
                country_id INT NOT NULL
            )
        """),
        ("manager",
         """
            CREATE TABLE manager (
                id INT NOT NULL,
                name TEXT NOT NULL,
                nickname TEXT,
                dob DATE,
                country_id INT NOT NULL,
                PRIMARY KEY (id)
            )
        """),
        ("manages",
         """
            CREATE TABLE manages (
                manager_id INT NOT NULL,
                team_id INT NOT NULL,
                PRIMARY KEY (manager_id, team_id)
            )
        """),
        ("match",
         """
            CREATE TABLE match (
                match_id INT NOT NULL PRIMARY KEY,
                competition_id INT NOT NULL,
                season_id INT NOT NULL,
                competition_stage_id INT,
                stadium_id INT,
                match_date DATE NOT NULL,
                kick_off TIME,
                referee_id INT,
                home_team_id INT NOT NULL ,
                away_team_id INT NOT NULL,
                home_score INT NOT NULL,
                away_score INT NOT NULL,
                match_week INT NOT NULL
            ) 
        """),
        ("event_type",
         """
            CREATE TABLE event_type (
                id INT NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
            )
        """),
        ("play_pattern",
         """
            CREATE TABLE play_pattern (
                id INT NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
            )
        """),
        ("position",
         """
            CREATE TABLE position (
                id INT NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
            )
        """),
        ("player",
         """
            CREATE TABLE player (
                player_id INT NOT NULL,
                player_name TEXT NOT NULL,
                PRIMARY KEY (player_id)
            ) 
        """),
        ("player_team",
         """
            CREATE TABLE player_team (
                player_id INT NOT NULL,
                team_id  INT NOT NULL ,
                PRIMARY KEY (player_id, team_id)
            ) 
        """),
        ("player_country",
         """
            CREATE TABLE player_country (
                player_id INT NOT NULL,
                country_id INT NOT NULL ,
                PRIMARY KEY (player_id, country_id)
            ) 
        """),
        ("player_nickname",
         """
            CREATE TABLE player_nickname (
                player_id INT NOT NULL,
                player_nickname TEXT NOT NULL,
                PRIMARY KEY (player_id, player_nickname)
            ) 
        """),
         ("player_position",
         """
            CREATE TABLE player_position (
                
                match_id INT NOT NULL,
                position_id INT NOT NULL,
                player_id INT NOT NULL,
                jersey_number INT NOT NULL,
                from_time time not null,
                to_time time,
                from_period int not null,
                to_period int,
                start_reason text,
                end_reason text,
                PRIMARY KEY (player_id, match_id, position_id,jersey_number, from_period, from_time)
            ) 
        """),
        ("body_part",
         """
            CREATE TABLE body_part (
                id INT PRIMARY KEY,
                name  TEXT NOT NULL
            ) 
        """),
        ("event",
         """
            CREATE TABLE event (
                id uuid NOT NULL PRIMARY KEY,
                match_id INT NOT NULL ,
                type_id INT NOT NULL ,
                index INT NOT NULL,
                period INT,
                timestamp TIME NOT NULL,
                minute INT NOT NULL,
                second INT NOT NULL,
                duration DECIMAL,
                off_camera BOOLEAN,
                first_time BOOLEAN,
                location_x DECIMAL,
                location_y DECIMAL,
                counterpress BOOLEAN,
                under_pressure BOOLEAN,
                play_pattern_id INT ,
                player_id INT,
                team_id INT ,
                possession INT,
                possession_team_id INT ,
                position_id INT ,
                out boolean
            )
        """),
         ("height",
         """
            CREATE TABLE height (
                id INT NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
            )
        """),
        ("pass_type",
         """
            CREATE TABLE pass_type (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )
        """),
        ("technique",
         """
            CREATE TABLE technique (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )
        """),
        ("outcome",
         """
            CREATE TABLE outcome (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )
        """),
        ("pass",
         """
            CREATE TABLE pass (
                event_id uuid PRIMARY KEY ,
                recipient_id int,
                length decimal,
                angle decimal,
                height_id int ,
                end_location_x decimal,
                end_location_y decimal,
                body_part_id int ,
                pass_type_id int ,
                switch boolean,
                outcome_id int ,
                aerial_won boolean,
                assisted_shot_id uuid,
                shot_assist boolean,
                cross_ boolean, 
                through_ball boolean,
                technique_id int,
                inswinging boolean,
                miscommunication boolean,
                cut_back boolean,
                deflected boolean,
                no_touch boolean,
                straight boolean,
                goal_assist boolean,
                outswinging boolean,
                backheel boolean
            ) 
        """),
         ("shot_type",
         """
            CREATE TABLE shot_type (
                id int PRIMARY KEY,
                name TEXT
            ) 
        """),
         ("shot",
         """
            CREATE TABLE shot (
                event_id uuid PRIMARY KEY ,
                statsbomb_xg decimal,
                end_location_x decimal,
                end_location_y decimal,
                end_location_z decimal,
                key_pass_id uuid,
                outcome_id int ,
                first_time boolean,
                body_part_id int ,
                shot_type_id int,
                aerial_won boolean,
                one_on_one boolean,
                saved_to_post boolean,
                deflected boolean,
                saved_off_target boolean,
                open_goal boolean,
                follows_dribble boolean,
                redirect boolean,
                kick_off boolean,
                technique_id int
            ) 
        """),
        ("freeze_frame",
         """
            CREATE TABLE freeze_frame (
                event_id uuid ,
                player_id int,
                position_id int ,
                location_x decimal,
                location_y decimal,
                teammate boolean,
                PRIMARY KEY (event_id, player_id)
            ) 
        """),
        ("injury_stoppage",
         """ 
            CREATE TABLE injury_stoppage (
                event_id uuid primary key ,
                in_chain boolean
            ) 
        """),
        ("tactic",
         """ 
            CREATE TABLE tactic (
                event_id uuid ,
                position_id int ,
                formation int,
                player_id int,
                primary key (event_id, player_id)
            ) 
        """), 
        ("lineup",
         """ 
            CREATE TABLE lineup (
                match_id int ,
                player_id int,
                primary key (match_id, player_id)
            ) 
        """), 
        ("card",
         """ 
            CREATE TABLE card (
                id int primary key,
                name TEXT
            ) 
        """), 
        ("interception",
         """ 
            CREATE TABLE interception (
                event_id uuid primary key ,
                outcome_id int 
            ) 
        """),
        ("miscontrol",
         """ 
            CREATE TABLE miscontrol (
                event_id uuid primary key ,
                aerial_won boolean
            ) 
        """),    
        ("half_start",
         """ 
            CREATE TABLE half_start (
                event_id uuid primary key ,
                late_video_start boolean
            ) 
        """),
        ("ball_receipt",
         """ 
            CREATE TABLE ball_receipt (
                event_id uuid primary key ,
                outcome_id int 
            ) 
        """),
        ("ball_recovery",
         """ 
            CREATE TABLE ball_recovery (
                event_id uuid primary key ,
                recovery_failure boolean,
                offensive boolean
            ) 
        """),
        ("carry",
         """ 
            CREATE TABLE carry (
                event_id uuid primary key ,
                end_location_x decimal,
                end_location_y decimal
            ) 
        """),
        ("dribble",
         """ 
            CREATE TABLE dribble (
                event_id uuid primary key ,
                overrun boolean,
                nutmeg boolean,
                no_touch boolean,
                outcome_id int 
            ) 
        """),
        ("clearance",
         """ 
            CREATE TABLE clearance (
                event_id uuid primary key ,
                left_foot boolean,
                right_foot boolean,
                head boolean,
                aerial_won boolean,
                body_part_id int 
            ) 
        """),
        ("substitution",
         """ 
            CREATE TABLE substitution (
                event_id uuid primary key ,
                replacement_id int,
                outcome_id int 
            ) 
        """),
        ("fifty_fifty",
         """ 
            CREATE TABLE fifty_fifty (
                event_id uuid primary key ,
                outcome_id int 
            ) 
        """),
        
        ("duel_type",
         """ 
            CREATE TABLE duel_type (
                id int primary key,
                name TEXT not null
            ) 
        """),
        ("duel",
         """ 
            CREATE TABLE duel (
                event_id uuid primary key ,
                duel_type_id int,
                outcome_id int 
            ) 
        """),
        ("foul_won",
         """ 
            CREATE TABLE foul_won (
                event_id uuid primary key ,
                advantage boolean,
                defensive boolean,
                penalty boolean
            ) 
        """),
         ("block",
         """ 
            CREATE TABLE block (
                event_id uuid primary key ,
                deflection boolean,
                offensive boolean,
                save_block boolean
            ) 
        """),
        ("foul_type",
         """ 
            CREATE TABLE foul_type (
                id int primary key,
                name TEXT
            ) 
        """),
        ("foul_committed",
         """ 
            CREATE TABLE foul_committed (
                event_id uuid primary key ,
                card_id int,
                foul_type_id int ,
                penalty boolean,
                offensive boolean,
                advantage boolean
            ) 
        """),
        ("bad_behaviour",
         """ 
            CREATE TABLE bad_behaviour (
                event_id uuid primary key ,
                card_id int 
            ) 
        """),
        ("goal_type",
         """ 
            CREATE TABLE goal_type (
                id int primary key,
                name text
            ) 
        """),
        ("goalkeeper",
                """
                    CREATE TABLE goalkeeper (
                        event_id uuid primary key ,
                        technique_id int,
                        position_id int,
                        body_part_id int,
                        outcome_id int,
                        end_location_x decimal,
                        end_location_y decimal,
                        goal_type_id int,
                        shot_saved_to_post boolean,
                        shot_saved_off_target boolean,
                        punched_out boolean,
                        lost_in_play boolean,
                        success_out boolean,
                        success_in_play boolean,
                        saved_to_post boolean,
                        penalty_saved_to_post boolean 
                    ) 
                """),

        )

def connectToDatabase():
    # Open a cursor to perform database operations
    return psycopg.connect(dbname=DATABASE_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT);
# Connect to an existing database


def createTables(ignore):
    with connectToDatabase() as conn:
        cursor = conn.cursor()
        
        for command in commands:
            if(command[0] not in ignore):
                try:
                    #cursor.execute(f"DROP TABLE IF EXISTS {command[0]} CASCADE");
                    #print("dropped,",command[0] )
                    cursor.execute(command[1]);
                except (psycopg.DatabaseError, Exception) as error:
                    print(error)
                    print(command[0])

def deleteTables(ignore):
    with connectToDatabase() as conn:
        cursor = conn.cursor()
        for command in commands:
            if(command[0] not in ignore):
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {command[0]} CASCADE");
                except (psycopg.DatabaseError, Exception) as error:
                    print(error)

ignore = {'player','match','competition','season','position','team'}


def deleteAndRecreateTables():
    deleteTables({})
    createTables({})