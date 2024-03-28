from django.test import TestCase
from api.models import Owners, Plots


class TestAPIView(TestCase):
    fixtures = ["test_data.json"]

    def test_initial_load(self):
        response = self.client.get("/api/initial-load/")
        self.assertEqual(200, response.status_code)
        self.assertTrue("owners" in response.json())
        self.assertTrue("experiments" in response.json())
        self.assertTrue("groups" in response.json())
        self.assertTrue("observations" in response.json())
        self.assertTrue("variables" in response.json())

    def test_get_plot_components(self):
        response = self.client.get(
            "/api/get-plot-components/?experiment_id=12&observation_id=1&variable_id=1&group_id=1")
        self.assertEqual("experiment_id=12&observation_id=1&variable_id=1&group_id=1",
                         response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)
        # check if plot components in response match with plot id in database
        plot = Plots.objects.get(pk=response.json()["plot_id"])
        self.assertEqual(plot.div, response.json()["div"])
        self.assertEqual(plot.script, response.json()["script"])

    def test_plot_insufficient_params(self):
        response = self.client.get("/api/get-plot-components/?experiment_id=12&observation_id=1")
        self.assertEqual("experiment_id=12&observation_id=1", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Missing request parameter detected: 'variable_id'",
                         response.json()['error'])

    def test_plot_invalid_plot_fks(self):
        response = self.client.get(
            "/api/get-plot-components/?experiment_id=-12&observation_id=1&variable_id=1&group_id=1")
        self.assertEqual("experiment_id=-12&observation_id=1&variable_id=1&group_id=1",
                         response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Plots matching query does not exist.", response.json()['error'])

    def test_plot_bad_input(self):
        response = self.client.get(
            "/api/get-plot-components/?experiment_id=xyz&observation_id=1&variable_id=1&group_id=1")
        self.assertEqual("experiment_id=xyz&observation_id=1&variable_id=1&group_id=1",
                         response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Field 'experiment_id' expected a number but got 'xyz'.",
                         response.json()['error'])

    def test_update_user_valid_pk(self):
        response = self.client.get("/api/update-user-option/?owner_id=1")
        self.assertEqual("owner_id=1", response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)
        self.assertTrue("experiments" in response.json())
        self.assertTrue("observations" in response.json())
        self.assertTrue("variables" in response.json())
        self.assertTrue("groups" in response.json())

    def test_update_user_invalid_pk(self):
        response = self.client.get("/api/update-user-option/?owner_id=-1")
        self.assertEqual("owner_id=-1", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Owners matching query does not exist.", response.json()['error'])

    def test_update_user_bad_input(self):
        response = self.client.get("/api/update-user-option/?owner_id=owner")
        self.assertEqual("owner_id=owner", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Field 'owner_id' expected a number but got 'owner'.",
                         response.json()['error'])

    def test_update_user_insufficient_params(self):
        response = self.client.get("/api/update-user-option/")
        self.assertEqual("", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Missing request parameter detected: 'owner_id'", response.json()['error'])

    def test_update_experiment_valid_pk(self):
        response = self.client.get("/api/update-experiment-option/?experiment_id=1")
        self.assertEqual("experiment_id=1", response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)
        self.assertTrue("observations" in response.json())
        self.assertTrue("variables" in response.json())
        self.assertTrue("groups" in response.json())

    def test_update_experiment_invalid_pk(self):
        response = self.client.get("/api/update-experiment-option/?experiment_id=-1")
        self.assertEqual("experiment_id=-1", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Experiments matching query does not exist.", response.json()['error'])

    def test_update_experiment_bad_input(self):
        response = self.client.get("/api/update-experiment-option/?experiment_id=!")
        self.assertEqual("experiment_id=!", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Field 'experiment_id' expected a number but got '!'.",
                         response.json()['error'])

    def test_update_experiment_insufficient_params(self):
        response = self.client.get("/api/update-experiment-option/?key1=value1")
        self.assertEqual("key1=value1", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Missing request parameter detected: 'experiment_id'",
                         response.json()['error'])

    def test_update_observation_valid_pk(self):
        response = self.client.get("/api/update-observation-option/?observation_id=1")
        self.assertEqual("observation_id=1", response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)
        self.assertTrue("groups" in response.json())
        self.assertTrue("variables" in response.json())

    def test_update_observation_invalid_pk(self):
        response = self.client.get("/api/update-observation-option/?observation_id=-1")
        self.assertEqual("observation_id=-1", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Observations matching query does not exist.", response.json()['error'])

    def test_update_observation_bad_input(self):
        response = self.client.get("/api/update-observation-option/?observation_id=~")
        self.assertEqual("observation_id=~", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Field 'observation_id' expected a number but got '~'.",
                         response.json()['error'])

    def test_update_observation_insufficient_params(self):
        response = self.client.get("/api/update-observation-option/")
        self.assertEqual("", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Missing request parameter detected: 'observation_id'",
                         response.json()['error'])

    def test_update_variable_valid_pk(self):
        response = self.client.get("/api/update-variable-option/?variable_id=1")
        self.assertEqual("variable_id=1", response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)
        self.assertTrue("groups" in response.json())

    def test_update_variable_invalid_pk(self):
        response = self.client.get("/api/update-variable-option/?variable_id=-1")
        self.assertEqual("variable_id=-1", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Variables matching query does not exist.", response.json()['error'])

    def test_update_variable_bad_input(self):
        response = self.client.get("/api/update-variable-option/?variable_id=~")
        self.assertEqual("variable_id=~", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Field 'variable_id' expected a number but got '~'.",
                         response.json()['error'])

    def test_update_variable_insufficient_params(self):
        response = self.client.get("/api/update-variable-option/")
        self.assertEqual("", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Missing request parameter detected: 'variable_id'",
                         response.json()['error'])
