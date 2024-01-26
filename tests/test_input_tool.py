from evagram.database import input_tool
import sys
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

        self.cur.execute(
            """SELECT setval('owners_owner_id_seq',
            (SELECT MAX(owner_id) FROM owners)+1)""")
        self.cur.execute(
            """SELECT setval('experiments_experiment_id_seq',
            (SELECT MAX(experiment_id) FROM experiments)+1)""")
        self.cur.execute(
            """SELECT setval('plots_plot_id_seq',
            (SELECT MAX(plot_id) FROM plots)+1)""")
        self.cur.execute(
            """SELECT setval('observations_observation_id_seq',
            (SELECT MAX(observation_id) FROM observations)+1)""")

    def tearDown(self):
        conn.rollback()
        self.cur.close()

    def test_InsertOwnerExpected(self):
        user_obj = {
            "username": "jdoe",
            "first_name": "John",
            "last_name": "Doe"
        }
        input_tool.insert_table_record(self.cur, user_obj, "owners")
        self.cur.execute(
            "SELECT (username) FROM owners WHERE username=%s", ("jdoe",))
        assert len(self.cur.fetchall()) == 1

    def test_InsertSameOwner(self):
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            user_obj = {
                "username": "bzhu",
                "first_name": "Brandon",
                "last_name": "Zhu"
            }
            input_tool.insert_table_record(self.cur, user_obj, "owners")

    def test_InsertOwnerNoUsername(self):
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            user_obj = {
                "first_name": "John",
                "last_name": "Doe"
            }
            input_tool.insert_table_record(self.cur, user_obj, "owners")

    def test_DeleteOwnerAndExperiments(self):
        self.cur.execute("DELETE FROM owners WHERE owner_id=%s", (1,))
        self.cur.execute(
            "SELECT (username) FROM owners WHERE owner_id=%s", (1,))
        assert len(self.cur.fetchall()) == 0
        self.cur.execute(
            "SELECT (experiment_id) FROM experiments WHERE owner_id=%s", (1,))
        assert len(self.cur.fetchall()) == 0

    def test_InsertExperimentExpected(self):
        experiment_obj = {
            "experiment_name": "control",
            "owner_id": 1
        }
        input_tool.insert_table_record(self.cur, experiment_obj, "experiments")
        self.cur.execute(
            "SELECT (experiment_name) FROM experiments WHERE experiment_name=%s AND owner_id=%s",
            ("control", 1)
        )
        assert len(self.cur.fetchall()) == 1

    def test_InsertExperimentWithoutOwner(self):
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            experiment_obj = {
                "experiment_name": "control"
            }
            input_tool.insert_table_record(self.cur, experiment_obj, "experiments")

    def test_InsertExperimentWithOwnerNotFound(self):
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            experiment_obj = {
                "experiment_name": "control",
                "owner_id": -1
            }
            input_tool.insert_table_record(self.cur, experiment_obj, "experiments")

    def test_InsertExperimentWithSameNameAndOwner(self):
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            experiment_obj = {
                "experiment_name": "experiment_control",
                "owner_id": 1
            }
            input_tool.insert_table_record(self.cur, experiment_obj, "experiments")

    def test_DeleteExperimentAndPlots(self):
        self.cur.execute(
            "DELETE FROM experiments WHERE experiment_id=%s", (12,))
        self.cur.execute(
            "SELECT (experiment_id) FROM experiments WHERE experiment_id=%s", (12,))
        assert len(self.cur.fetchall()) == 0
        self.cur.execute(
            "SELECT (plot_id) FROM plots WHERE experiment_id=%s", (12,))
        assert len(self.cur.fetchall()) == 0

    def test_InsertPlotExpected(self):
        plot_obj = {
            "plot_id": 115,
            "experiment_id": 1,
            "group_id": 3,
            "observation_id": 2
        }
        input_tool.insert_table_record(self.cur, plot_obj, "plots")
        self.cur.execute(
            "SELECT (plot_id) FROM plots WHERE plot_id=%s", (115,))
        assert len(self.cur.fetchall()) == 1

    def test_InsertPlotWithoutExperimentGroupOrObservation(self):
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            plot_obj = {
                "group_id": 1,
                "observation_id": 1
            }
            input_tool.insert_table_record(self.cur, plot_obj, "plots")
        self.tearDown()
        self.setUp()
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            plot_obj = {
                "experiment_id": 12,
                "observation_id": 1
            }
            input_tool.insert_table_record(self.cur, plot_obj, "plots")
        self.tearDown()
        self.setUp()
        with self.assertRaises(psycopg2.errors.NotNullViolation):
            plot_obj = {
                "experiment_id": 12,
                "group_id": 1
            }
            input_tool.insert_table_record(self.cur, plot_obj, "plots")

    def test_InsertPlotWithExperimentGroupOrObservationNotFound(self):
        plot_obj = {
            "experiment_id": 12,
            "group_id": 1,
            "observation_id": 1
        }
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            plot_obj["experiment_id"] = -12
            input_tool.insert_table_record(self.cur, plot_obj, "plots")
        self.tearDown()
        self.setUp()
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            plot_obj["experiment_id"] = 12
            plot_obj["group_id"] = -1
            input_tool.insert_table_record(self.cur, plot_obj, "plots")
        self.tearDown()
        self.setUp()
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            plot_obj["group_id"] = 1
            plot_obj["observation_id"] = -1
            input_tool.insert_table_record(self.cur, plot_obj, "plots")

    def test_InsertObservationExpected(self):
        observation_obj = {
            "observation_name": "aircraft"
        }
        input_tool.insert_table_record(self.cur, observation_obj, "observations")
        self.cur.execute(
            """SELECT (observation_id) FROM observations
            WHERE observation_name=%s""",
            ("satwind",))
        assert len(self.cur.fetchall()) == 1

    def test_InsertNewObservationToExistingVariable(self):
        observation_obj = {
            "observation_name": "amsua_n19"
        }
        input_tool.insert_table_record(self.cur, observation_obj, "observations")
        # get existing variable and its variable_id
        self.cur.execute("SELECT variable_id FROM variables WHERE variable_name=%s AND channel=%s",
                         ("brightnessTemperature", 4))
        variable_id = self.cur.fetchone()[0]

        # get inserted observation and its observation_id
        self.cur.execute("SELECT observation_id FROM observations WHERE observation_name=%s",
                         ("amsua_n19",))
        observation_id = self.cur.fetchone()[0]

        # establish relationship between observation and variable in junction table
        observation_variable_obj = {
            "observation_id": observation_id,
            "variable_id": variable_id
        }
        input_tool.insert_table_record(self.cur, observation_variable_obj, "observation_variable")

        # check if variable exists in observation
        self.cur.execute("SELECT variable_id FROM observation_variable WHERE observation_id=%s",
                         (observation_id,))
        variables = self.cur.fetchall()
        self.assertTrue((variable_id,) in variables)

    def test_InsertObservationWithSameName(self):
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            observation_obj = {
                "observation_name": "satwind",
            }
            input_tool.insert_table_record(self.cur, observation_obj, "observations")

    def test_InsertExistingObservationToVariableTwice(self):
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            observation_variable_obj = {
                "observation_id": 1,
                "variable_id": 1
            }
            input_tool.insert_table_record(
                self.cur, observation_variable_obj, "observation_variable")

    def test_FetchExistingPlots(self):
        # get all amsua_n18 plots in experiment "experiment_iv_2" where the user is asewnath
        self.cur.execute("""SELECT plot_id, plots.experiment_id FROM plots
                            JOIN experiments ON plots.experiment_id = experiments.experiment_id
                            JOIN observations ON plots.observation_id = observations.observation_id
                            JOIN owners ON owners.owner_id = experiments.owner_id
                            WHERE experiments.experiment_name = %s
                            AND owners.username = %s AND observations.observation_name = %s; """,
                         ("experiment_iv_2", "asewnath", "amsua_n18"))
        plots = self.cur.fetchall()
        self.assertTrue(len(plots) == 1)
        # checks if plot with plot_id=114 and experiment_id=1 was found
        self.assertTrue((114, 1) in plots)

    def test_FetchNonExistingPlots(self):
        # get all satwind plots in experiment "experiment_iv_1"
        self.cur.execute("""SELECT plot_id FROM plots
                            JOIN experiments ON plots.experiment_id = experiments.experiment_id
                            JOIN observations ON plots.observation_id = observations.observation_id
                            WHERE experiments.experiment_name = %s
                            AND observations.observation_name = %s;""",
                         ("experiment_iv_1", "satwind"))
        self.assertTrue(len(self.cur.fetchall()) == 0)

    def test_GroupsRelationWithPlotsAndExperiments(self):
        self.cur.execute("""SELECT group_name, plots.plot_id, experiments.experiment_id FROM groups
                            JOIN plots ON plots.group_id = groups.group_id
                            JOIN experiments ON experiments.experiment_id = plots.experiment_id
                            WHERE group_name = %s;""", ("effectiveerror-vs-gsifinalerror",))
        plots, experiments = [], []
        for item in self.cur.fetchall():
            assert len(item) == 3  # sanity checks
            assert item[0] == "effectiveerror-vs-gsifinalerror"
            plots.append(item[1])
            experiments.append(item[2])
        # check that plots associated with group have unique plot ids (relation)
        self.assertEqual(plots, list(set(plots)))
        # check that experiments associated with group can be the same (no relation)
        self.assertGreaterEqual(len(experiments), len(set(experiments)))


unittest.main()
