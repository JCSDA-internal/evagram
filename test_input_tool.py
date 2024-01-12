import input_tool
import unittest
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# environment variables for connecting to database
db_host = os.environ.get('DB_HOST')
db_port = os.environ.get('DB_PORT')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')

conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name, 
        user=db_user, 
        password=db_password
)

class TestDatabaseInputTool(unittest.TestCase):
    def setUp(self):
        self.cur = conn.cursor()
        input_tool.drop_tables(self.cur)
        input_tool.create_tables(self.cur)
        input_tool.load_dataset_to_db(self.cur)
        
        self.cur.execute("SELECT setval('owners_owner_id_seq', (SELECT MAX(owner_id) FROM owners)+1)")
        self.cur.execute("SELECT setval('experiments_experiment_id_seq', (SELECT MAX(experiment_id) FROM experiments)+1)")
        self.cur.execute("SELECT setval('plots_plot_id_seq', (SELECT MAX(plot_id) FROM plots)+1)")
        self.cur.execute("SELECT setval('observations_observation_id_seq', (SELECT MAX(observation_id) FROM observations)+1)")
    
    def tearDown(self):
        conn.rollback()
        self.cur.close()

    def test_InsertOwnerExpected(self):
        self.cur.execute("INSERT INTO owners (username, first_name, last_name) VALUES (%s, %s, %s)", ("jdoe", "John", "Doe"))
        self.cur.execute("SELECT (username) FROM owners WHERE username=%s", ("jdoe",))
        assert len(self.cur.fetchall()) == 1

    def test_InsertSameOwner(self):
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            self.cur.execute("INSERT INTO owners (username, first_name, last_name) VALUES (%s, %s, %s)", ("bzhu", "Brandon", "Zhu"))
    
    def test_InsertOwnerNoUsername(self):
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            self.cur.execute("INSERT INTO owners (first_name, last_name) VALUES (%s, %s)", ("John", "Doe"))
    
    def test_DeleteOwnerAndExperiments(self):
        self.cur.execute("DELETE FROM owners WHERE owner_id=%s", (1,))
        self.cur.execute("SELECT (username) FROM owners WHERE owner_id=%s", (1,))
        assert len(self.cur.fetchall()) == 0
        self.cur.execute("SELECT (experiment_id) FROM experiments WHERE owner_id=%s", (1,))
        assert len(self.cur.fetchall()) == 0
    
    def test_InsertExperimentExpected(self):
        self.cur.execute("INSERT INTO experiments (experiment_name, owner_id) VALUES (%s, %s)", ("control", 1))
        self.cur.execute("SELECT (experiment_name) FROM experiments WHERE experiment_name=%s AND owner_id=%s", ("control", 1))
        assert len(self.cur.fetchall()) == 1
    
    def test_InsertExperimentWithoutOwner(self):
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            self.cur.execute("INSERT INTO experiments (experiment_name) VALUES (%s)", ("control",))
    
    def test_InsertExperimentWithOwnerNotFound(self):
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            self.cur.execute("INSERT INTO experiments (experiment_name, owner_id) VALUES (%s, %s)", ("control", -1))
    
    def test_InsertExperimentWithSameNameAndOwner(self):
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            self.cur.execute("INSERT INTO experiments (experiment_name, owner_id) VALUES (%s, %s)", ("experiment_control", 1))

    def test_DeleteExperimentAndPlots(self):
        self.cur.execute("DELETE FROM experiments WHERE experiment_id=%s", (12,))
        self.cur.execute("SELECT (experiment_id) FROM experiments WHERE experiment_id=%s", (12,))
        assert len(self.cur.fetchall()) == 0
        self.cur.execute("SELECT (plot_id) FROM plots WHERE experiment_id=%s", (12,))
        assert len(self.cur.fetchall()) == 0

    def test_InsertPlotExpected(self):
        self.cur.execute("INSERT INTO plots (plot_id, experiment_id, group_id, observation_id) VALUES (%s, %s, %s, %s)", (115, 1, 4, 5))
        self.cur.execute("SELECT (plot_id) FROM plots WHERE plot_id=%s", (115,))
        assert len(self.cur.fetchall()) == 1
    
    def test_InsertPlotWithoutExperimentGroupOrObservation(self):
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            self.cur.execute("INSERT INTO plots (group_id, observation_id) VALUES (%s, %s)", (1, 1))
        self.tearDown()
        self.setUp()
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            self.cur.execute("INSERT INTO plots (experiment_id, observation_id) VALUES (%s, %s)", (12, 1))
        self.tearDown()
        self.setUp()
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            self.cur.execute("INSERT INTO plots (experiment_id, group_id) VALUES (%s, %s)", (12, 1))
    
    def test_InsertPlotWithExperimentGroupOrObservationNotFound(self):
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            self.cur.execute("INSERT INTO plots (experiment_id, group_id, observation_id) VALUES (%s, %s, %s)", (-1, 1, 1))
        self.tearDown()
        self.setUp()
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            self.cur.execute("INSERT INTO plots (experiment_id, group_id, observation_id) VALUES (%s, %s, %s)", (12, -1, 1))
        self.tearDown()
        self.setUp()
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            self.cur.execute("INSERT INTO plots (experiment_id, group_id, observation_id) VALUES (%s, %s, %s)", (12, 1, -1))

    def test_InsertObservationExpected(self):
        self.cur.execute("INSERT INTO observations (observation_name, variable_id) VALUES (%s, %s)", ("satwind", 1))
        self.cur.execute("SELECT (observation_id) FROM observations WHERE observation_name=%s AND variable_id=%s", ("satwind", 1))
        assert len(self.cur.fetchall()) == 1

    def test_InsertObservationWithoutVariable(self):
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            self.cur.execute("INSERT INTO observations (observation_name) VALUES (%s)", ("amsua_n19",))
    
    def test_InsertObservationWithVariableNotFound(self):
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            self.cur.execute("INSERT INTO observations (observation_name, variable_id) VALUES (%s, %s)", ("amsua_n19", -1))

    def test_FetchExistingPlots(self):
        # get all amsua_n18 plots in experiment "experiment_iv_2" where the user is asewnath
        self.cur.execute("""SELECT plot_id, plots.experiment_id FROM plots
                            JOIN experiments ON plots.experiment_id = experiments.experiment_id
                            JOIN observations ON plots.observation_id = observations.observation_id
                            JOIN owners ON owners.owner_id = experiments.owner_id
                            WHERE experiments.experiment_name = %s AND owners.username = %s AND observations.observation_name = %s; """, ("experiment_iv_2", "asewnath", "amsua_n18"))
        plots = self.cur.fetchall()
        self.assertTrue(len(plots) == 1)
        self.assertTrue((114, 1) in plots) # checks if plot with plot_id=114 and experiment_id=1 was found

    def test_FetchNonExistingPlots(self):
        # get all satwind plots in experiment "experiment_iv_1"
        self.cur.execute("""SELECT plot_id FROM plots
                            JOIN experiments ON plots.experiment_id = experiments.experiment_id 
                            JOIN observations ON plots.observation_id = observations.observation_id
                            WHERE experiments.experiment_name = %s AND observations.observation_name = %s;""", ("experiment_iv_1", "satwind"))
        self.assertTrue(len(self.cur.fetchall()) == 0)
    
    def test_GroupsRelationWithPlotsAndExperiments(self):
        self.cur.execute("""SELECT group_name, plots.plot_id, experiments.experiment_id FROM groups
                            JOIN plots ON plots.group_id = groups.group_id
                            JOIN experiments ON experiments.experiment_id = plots.experiment_id
                            WHERE group_name = %s;""", ("effectiveerror-vs-gsifinalerror",))
        plots, experiments = [], []
        for item in self.cur.fetchall():
            assert len(item) == 3 # sanity checks
            assert item[0] == "effectiveerror-vs-gsifinalerror"
            plots.append(item[1])
            experiments.append(item[2])
        # check that plots associated with group have unique plot ids (relation)
        self.assertEqual(plots, list(set(plots)))
        # check that experiments associated with group can be the same (no relation)
        self.assertGreaterEqual(len(experiments), len(set(experiments)))

unittest.main()
