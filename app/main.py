import requests
import json
import pandas as pd
import psycopg2


def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    query = ""
    if table == 'missions':
        query = "INSERT INTO missions (ID_MISSION, DESCRIPTION, MANUFACTURES, NAME, TWITTER, WEBSITE) " \
                "VALUES (%s, %s, %s, %s, %s, %s)"
    elif table == 'rockets':
        query = "INSERT INTO rockets (ID_ROCKET, ROCKET_NAME, ACTIVE, BOOSTERS, COMPANY, COST_PER_LAUNCH, COUNTRY, " \
                "STAGES, SUCCESS_RATE_PCT, ROCKET_TYPE, DIAMETER_M, HEIGHT_M, MASS_KG) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    elif table == 'launches':
        query = "INSERT INTO launches (ID_LAUNCH, DETAILS, ID_MISSION, MISSION_NAME, UPCOMING, LAUNCH_SUCCESS, " \
                "ROCKET_NAME, ROCKET_TYPE, ID_ROCKET) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


def collect_from_space(query, format_d):
    json_post = {
        'query': query
    }
    url = 'https://spacex-production.up.railway.app/'
    r = requests.post(url, json=json_post)
    json_data = json.loads(r.text)
    df = pd.json_normalize(json_data['data'][format_d])
    return df


def query_archive(format_d):
    query = ""
    if format_d == 'missions':
        query = """
                    query Query {
                      missions {
                        id
                        description
                        manufacturers
                        name
                        twitter
                        website
                      }
                    }
                    """
    elif format_d == 'rockets':
        query = """
                query Query {
                  rockets {
                    id
                    name
                    active
                    boosters
                    company
                    cost_per_launch
                    country
                    diameter {
                      meters
                    }
                    height {
                      meters
                    }
                    mass {
                      kg
                    }
                    stages
                    success_rate_pct
                    type
                  }
                }
                """
    elif format_d == 'launches':
        query = """
                query Query {
                  launches {
                    id
                    details
                    mission_id
                    mission_name
                    rocket {
                      rocket_name
                      rocket_type
                      rocket {
                        id
                      }
                    }
                    upcoming
                    launch_success
                  }
                }
                """
    return query


def db_create(conn, table):
    table_scheme = ""
    if table == 'missions':
        table_scheme = '''CREATE TABLE missions  
             (
             ID_MISSION CHAR(50) PRIMARY KEY NOT NULL,
             DESCRIPTION CHAR(50),
             MANUFACTURES CHAR(50),
             NAME CHAR(50),
             TWITTER CHAR(50),
             WEBSITE CHAR(50)
             );'''
    elif table == 'rockets':
        table_scheme = '''CREATE TABLE rockets  
             (
             ID_ROCKET CHAR(50) PRIMARY KEY NOT NULL,
             ROCKET_NAME CHAR(50),
             ACTIVE BOOLEAN,
             BOOSTERS INT,
             COMPANY CHAR(50),
             COST_PER_LAUNCH INT,
             COUNTRY CHAR(50),
             STAGES INT,
             SUCCESS_RATE_PCT INT,
             ROCKET_TYPE CHAR(50),
             DIAMETER_M FLOAT,
             HEIGHT_M FLOAT,
             MASS_KG INT
             );'''
    elif table == 'launches':
        table_scheme = '''CREATE TABLE launches  
             (
             ID_LAUNCH CHAR(50) PRIMARY KEY NOT NULL,
             DETAILS TEXT,
             ID_MISSION CHAR(50),
             MISSION_NAME CHAR(50),
             UPCOMING BOOLEAN,
             LAUNCH_SUCCESS BOOLEAN,
             ROCKET_NAME CHAR(50),
             ROCKET_TYPE CHAR(50),
             ID_ROCKET CHAR(50)
             );'''
    elif table == 'all_vitrine':
        table_scheme = '''CREATE TABLE all_vitrine  
                 (
                 id SERIAL PRIMARY KEY NOT NULL,
                 ID_MISSION CHAR(50),
                 ID_ROCKET CHAR(50),
                 ID_LAUNCH CHAR(50),
                 COUNT_MISSIONS INT,
                 COUNT_ROCKETS INT,
                 COUNT_LAUNCHES INT,
                 CONSTRAINT ID_MISSION FOREIGN KEY (ID_MISSION)
                    REFERENCES missions (ID_MISSION) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
                    NOT VALID,
                 CONSTRAINT ID_ROCKET FOREIGN KEY (ID_ROCKET)
                    REFERENCES rockets (ID_ROCKET) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
                    NOT VALID,
                 CONSTRAINT ID_LAUNCH FOREIGN KEY (ID_LAUNCH)
                    REFERENCES launches (ID_LAUNCH) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
                    NOT VALID
                );
                '''

    try:
        curs = conn.cursor()
        curs.execute(table_scheme)
        print("Table created successfully")
        curs.close()
        conn.commit()
    except psycopg2.Error as e:
        print(e.pgerror)


if __name__ == '__main__':
    try:
        conn = psycopg2.connect(database="Space_DB", user="Space_User", password="1A2B", port=5432)
        print("Database opened successfully")
    except psycopg2.Error as e:
        conn = None
        print(e.pgerror)

    list_form = ['missions', 'rockets', 'launches']
    list_count = []

    for form in list_form:
        query = query_archive(form)
        df_form = collect_from_space(query, form)
        list_count.append(len(df_form.index))
        db_create(conn, form)
        execute_values(conn, df_form, form)

    db_create(conn, 'all_vitrine')
    query = "INSERT INTO all_vitrine (COUNT_MISSIONS, COUNT_ROCKETS, COUNT_LAUNCHES) " \
            "VALUES (%s, %s, %s)"
    cursor = conn.cursor()
    cursor.execute(query, tuple(list_count))
    cursor.close()

    conn.commit()
    conn.close()
