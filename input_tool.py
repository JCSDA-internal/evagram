import pickle
import json
import os
import time
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# can be modified to the file path of experiment data
EXPERIMENT_DATA_PATH = os.path.join(os.getcwd(),'local')

# environment variables for connecting to database
db_host = os.environ.get('DB_HOST')
db_port = os.environ.get('DB_PORT')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')

def create_tables(cur):
    # Users table
    cur.execute("""CREATE TABLE IF NOT EXISTS owners (
                owner_id serial PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                username VARCHAR NOT NULL UNIQUE
    )""")
    # Experiments table
    cur.execute("""CREATE TABLE IF NOT EXISTS experiments (
                experiment_id serial PRIMARY KEY,
                experiment_name VARCHAR,
                owner_id INTEGER NOT NULL,
                CONSTRAINT fk_owner
                    FOREIGN KEY (owner_id)
                        REFERENCES owners(owner_id)
                        ON DELETE CASCADE,
                UNIQUE(experiment_name, owner_id)
    )""")
    # Groups table
    cur.execute("""CREATE TABLE IF NOT EXISTS groups (
                group_id serial PRIMARY KEY,
                group_name VARCHAR
    )""")
    # Variables table
    cur.execute("""CREATE TABLE IF NOT EXISTS variables (
                variable_id serial PRIMARY KEY,
                variable_name VARCHAR,
                channel INTEGER
    )""")
    # Observations table
    cur.execute("""CREATE TABLE IF NOT EXISTS observations (
                observation_id serial PRIMARY KEY,
                observation_name VARCHAR,
                variable_id INTEGER NOT NULL,
                CONSTRAINT fk_variable
                    FOREIGN KEY (variable_id)
                        REFERENCES variables(variable_id)
                        ON DELETE CASCADE      
    )""")
    # Plots table
    cur.execute("""CREATE TABLE IF NOT EXISTS plots (
                plot_id serial PRIMARY KEY,
                div VARCHAR,
                script VARCHAR,
                experiment_id INTEGER NOT NULL,
                group_id INTEGER NOT NULL,
                observation_id INTEGER NOT NULL,
                CONSTRAINT fk_experiment
                    FOREIGN KEY (experiment_id)
                        REFERENCES experiments(experiment_id)
                        ON DELETE CASCADE,
                CONSTRAINT fk_group
                    FOREIGN KEY (group_id)
                        REFERENCES groups(group_id)
                        ON DELETE CASCADE,
                CONSTRAINT fk_observation
                    FOREIGN KEY (observation_id)
                        REFERENCES observations(observation_id)
                        ON DELETE CASCADE
    )""")

def drop_tables(cur):
    cur.execute("DROP TABLE IF EXISTS owners CASCADE")
    cur.execute("DROP TABLE IF EXISTS experiments CASCADE")
    cur.execute("DROP TABLE IF EXISTS plots CASCADE")
    cur.execute("DROP TABLE IF EXISTS groups CASCADE")
    cur.execute("DROP TABLE IF EXISTS observations CASCADE")
    cur.execute("DROP TABLE IF EXISTS variables CASCADE")

def load_dataset_to_db(cur):
    with open("dataset.json", 'rb') as dataset:
        sample_dataset = json.load(dataset)
    for owner in sample_dataset['owners']:
        add_user(cur, owner)
    for experiment in sample_dataset['experiments']:
        add_experiment(cur, experiment)
    for plot in (sample_dataset['plots']):
        add_plot(cur, plot, sample_dataset['observation_dirs'])

def get_observation_name(obs_dirs, filename):
    for observation in obs_dirs:
        obs_dir_path = os.path.join(EXPERIMENT_DATA_PATH, observation)
        for plot in os.listdir(obs_dir_path):
            if filename == plot:
                return observation
    return None

def add_user(cur, user_obj):
    # check preconditions
    if 'owner_id' not in user_obj or 'username' not in user_obj:
        print("Skipping current user: needs both owner_id and username")
        return 1
    elif 'first_name' not in user_obj or 'last_name' not in user_obj:
        cur.execute("INSERT INTO owners (owner_id, username, first_name, last_name) VALUES (%s, %s, %s, %s)", (user_obj["owner_id"], user_obj["username"], user_obj["first_name"], user_obj["last_name"]))
    else:
        cur.execute("INSERT INTO owners (owner_id, username, first_name, last_name) VALUES (%s, %s, %s, %s)", (user_obj["owner_id"], user_obj["username"], user_obj["first_name"], user_obj["last_name"]))
    return 0

def delete_user(cur, username):
    cur.execute("DELETE FROM owners WHERE username=%s", (username,))

def add_experiment(cur, experiment_obj):
    if 'experiment_id' not in experiment_obj or 'owner_id' not in experiment_obj:
        print("Skipping current experiment: must provide both experiment_id and owner_id")
        return 1
    else:
        cur.execute("INSERT INTO experiments (experiment_id, experiment_name, owner_id) VALUES (%s, %s, %s)", (experiment_obj["experiment_id"], experiment_obj["experiment_name"], experiment_obj["owner_id"]))
    return 0

def add_plot(cur, plot_obj, observation_dirs):
    if 'plot_id' not in plot_obj or 'plot_file' not in plot_obj or 'experiment_id' not in plot_obj or 'group_id' not in plot_obj or 'observation_id' not in plot_obj:
        print("Skipping current plot: needs plot, experiment, group, and observation ids")
        return 1
    
    plot_filename = plot_obj['plot_file']
    observation_name = get_observation_name(observation_dirs, plot_filename)
    plot_file_path = os.path.join(EXPERIMENT_DATA_PATH, observation_name, plot_filename)

    with open(plot_file_path, 'rb') as file:
        dictionary = pickle.load(file)

    # extract the div and script components
    div = dictionary['div']
    script = dictionary['script']

    # parse filename
    filename_no_extension = os.path.splitext(plot_filename)[0]
    plot_components = filename_no_extension.split("_")
    # TODO: find real solution for optional channel fields
    if len(plot_components) == 3:
        channel = 0
        group_name = plot_components[2]
    else:
        channel = plot_components[1]
        group_name = plot_components[3]
    var_name = plot_components[0]

    # add observation, group, variable records to database
    cur.execute("INSERT INTO variables (variable_name, channel) VALUES (%s, %s)", (var_name, channel))
    # variables are dynamically generated, require an extra query to get variable_id
    cur.execute("SELECT variable_id FROM variables WHERE variable_name=%s AND channel=%s", (var_name, channel))
    variable_id = cur.fetchone()[0]
    cur.execute("INSERT INTO observations (observation_id, observation_name, variable_id) VALUES (%s, %s, %s)", (plot_obj['observation_id'], observation_name, variable_id))
    cur.execute("INSERT INTO groups (group_id, group_name) VALUES (%s, %s)", (plot_obj['group_id'], group_name))
    # insert plot to database
    cur.execute("INSERT INTO plots (plot_id, div, script, experiment_id, group_id, observation_id) VALUES (%s, %s, %s, %s, %s, %s)", (plot_obj['plot_id'], div, script, plot_obj['experiment_id'], plot_obj['group_id'], plot_obj['observation_id']))
    return 0


if __name__ == "__main__":
    t1 = time.time()
    eva_dir_path = EXPERIMENT_DATA_PATH
    # observation_dirs = [f for f in os.listdir(eva_dir_path) if os.path.isdir(os.path.join(eva_dir_path, f))]

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name, 
        user=db_user, 
        password=db_password
    )
    cur = conn.cursor()

    drop_tables(cur)
    create_tables(cur)
    load_dataset_to_db(cur)

    # count = 0
    # for observation in observation_dirs:
    #     obs_dir_path = os.path.join(eva_dir_path, observation)
    #     for plot in os.listdir(obs_dir_path):
            # file_path = os.path.join(obs_dir_path, experiment)
            # file_name = os.path.splitext(file_path)[0]
            # file_extension = os.path.splitext(file_path)[1]
            # if file_extension == '.pkl':
            #     count += 1

    # t2 = time.time()
    # print("seconds to scan and insert {} files: {}".format(count, t2-t1))

    conn.commit()
    cur.close()
    conn.close()
