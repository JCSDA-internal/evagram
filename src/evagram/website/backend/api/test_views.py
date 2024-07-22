from django.test import TestCase
from api.models import Owners, Plots


class TestAPIView(TestCase):
    fixtures = ["test_data.json"]

    def test_initial_load(self):
        response = self.client.get("/api/initial-load/")
        self.assertEqual(200, response.status_code)
        self.assertTrue("owners" in response.json())

    def test_get_single_plot(self):
        response = self.client.get(
            "/api/get-plots-by-field/?owner_id=1&experiment_id=12&observation_id=1"
            "&variable_name=brightnessTemperature&channel=4&group_id=1")
        self.assertEqual("owner_id=1&experiment_id=12&observation_id=1&"
                         "variable_name=brightnessTemperature&channel=4&group_id=1",
                         response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)
        # check if plot components in response match with plot id in database
        plot = Plots.objects.get(pk=response.json()[0]["plot_id"])
        self.assertEqual(plot.div, response.json()[0]["div"])
        self.assertEqual(plot.script, response.json()[0]["script"])

    def test_plot_insufficient_params(self):
        response = self.client.get("/api/get-plots-by-field/?owner_id=1")
        self.assertEqual("owner_id=1", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)

    def test_plot_invalid_username(self):
        response = self.client.get(
            "/api/get-plots-by-field/?owner_id=null&experiment_id=12&observation_id=1"
            "&variable_name=brightnessTemperature&channel=4&group_id=1")
        self.assertEqual("owner_id=null&experiment_id=12&observation_id=1"
                         "&variable_name=brightnessTemperature&channel=4&group_id=1",
                         response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)

    def test_plot_username_not_found(self):
        response = self.client.get(
            "/api/get-plots-by-field/?owner_id=-1&experiment_id=12&observation_id=1"
            "&variable_name=brightnessTemperature&channel=4&group_id=1")
        self.assertEqual("owner_id=-1&experiment_id=12&observation_id=1"
                         "&variable_name=brightnessTemperature&channel=4&group_id=1",
                         response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)

    def test_plot_value_error(self):
        response = self.client.get(
            "/api/get-plots-by-field/?owner_id=1&experiment_id=xyz&observation_id=1"
            "&variable_name=brightnessTemperature&channel=4&group_id=1")
        self.assertEqual("owner_id=1&experiment_id=xyz&observation_id=1"
                         "&variable_name=brightnessTemperature&channel=4&group_id=1",
                         response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)

    def test_plot_input_cascade(self):
        # Tests if the experiment_id is handled before the observation_id
        response = self.client.get(
            "/api/get-plots-by-field/?owner_id=1&experiment_id=null&observation_id=1"
            "&variable_name=null&channel=null&group_id=null")
        self.assertEqual("owner_id=1&experiment_id=null&observation_id=1"
                         "&variable_name=null&channel=null&group_id=null",
                         response.request['QUERY_STRING'])
        # If diagnostics were queried out of order by observation_id,
        # the queryset will be empty because experiment_id=null
        self.assertNotEqual(0, len(response.data))

    def test_plot_channel_error(self):
        # Tests if channel was not provided and variable requires it
        response = self.client.get(
            "/api/get-plots-by-field/?owner_id=1&experiment_id=12&observation_id=1"
            "&variable_name=brightnessTemperature&channel=null&group_id=null")
        self.assertEqual("owner_id=1&experiment_id=12&observation_id=1"
                         "&variable_name=brightnessTemperature&channel=null&group_id=null",
                         response.request['QUERY_STRING'])
        self.assertEqual(404, response.status_code)

    def test_plot_channel_is_optional(self):
        # Tests if channel was not provided but variable takes optional channel value
        response = self.client.get(
            "/api/get-plots-by-field/?owner_id=1&experiment_id=12&observation_id=1"
            "&variable_name=windEastward&channel=null&group_id=null")
        self.assertEqual("owner_id=1&experiment_id=12&observation_id=1"
                         "&variable_name=windEastward&channel=null&group_id=null",
                         response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)

    def test_update_user_option_valid_params(self):
        response = self.client.get("/api/update-user-option/?owner_id=1")
        self.assertEqual("owner_id=1", response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)
        self.assertTrue("experiments" in response.json())
        self.assertTrue("observations" in response.json())
        self.assertTrue("variables" in response.json())
        self.assertTrue("groups" in response.json())

    def test_update_user_option_object_not_found(self):
        response = self.client.get("/api/update-user-option/?owner_id=-1")
        self.assertEqual("owner_id=-1", response.request['QUERY_STRING'])
        self.assertEqual(404, response.status_code)

    def test_update_user_option_value_error(self):
        response = self.client.get("/api/update-user-option/?owner_id=owner")
        self.assertEqual("owner_id=owner", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)

    def test_update_user_option_insufficient_params(self):
        response = self.client.get("/api/update-user-option/")
        self.assertEqual("", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
        self.assertEqual("Missing request parameter detected: 'owner_id'", response.json()['error'])

    def test_update_experiment_option_valid_params(self):
        response = self.client.get("/api/update-experiment-option/?experiment_id=1")
        self.assertEqual("experiment_id=1", response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)
        self.assertTrue("observations" in response.json())
        self.assertTrue("variables" in response.json())
        self.assertTrue("groups" in response.json())

    def test_update_experiment_option_object_not_found(self):
        response = self.client.get("/api/update-experiment-option/?experiment_id=-1")
        self.assertEqual("experiment_id=-1", response.request['QUERY_STRING'])
        self.assertEqual(404, response.status_code)

    def test_update_experiment_option_value_error(self):
        response = self.client.get("/api/update-experiment-option/?experiment_id=!")
        self.assertEqual("experiment_id=!", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)

    def test_update_experiment_option_insufficient_params(self):
        response = self.client.get("/api/update-experiment-option/?key1=value1")
        self.assertEqual("key1=value1", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)

    def test_update_observation_option_valid_params(self):
        response = self.client.get("/api/update-observation-option/"
                                   "?experiment_id=12&observation_id=1")
        self.assertEqual("experiment_id=12&observation_id=1", response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)
        self.assertTrue("groups" in response.json())
        self.assertTrue("variables" in response.json())
        self.assertTrue("variablesMap" in response.json())

    def test_update_observation_option_object_not_found(self):
        response = self.client.get("/api/update-observation-option/"
                                   "?experiment_id=1&observation_id=-1")
        self.assertEqual("experiment_id=1&observation_id=-1", response.request['QUERY_STRING'])
        self.assertEqual(404, response.status_code)

    def test_update_observation_option_value_error(self):
        response = self.client.get("/api/update-observation-option/?observation_id=~")
        self.assertEqual("observation_id=~", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)

    def test_update_observation_option_insufficient_params(self):
        response = self.client.get("/api/update-observation-option/")
        self.assertEqual("", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)

    def test_update_variable_option_valid_params(self):
        response = self.client.get(
            "/api/update-variable-option/?experiment_id=12&observation_id=1&"
            "variable_name=brightnessTemperature&channel=4")
        self.assertEqual("experiment_id=12&observation_id=1"
                         "&variable_name=brightnessTemperature&channel=4",
                         response.request['QUERY_STRING'])
        self.assertEqual(200, response.status_code)
        self.assertTrue("groups" in response.json())
        self.assertTrue("channel" in response.json())

    def test_update_variable_option_object_not_found(self):
        response = self.client.get(
            "/api/update-variable-option/?experiment_id=12&observation_id=1&"
            "variable_name=variable&channel=null")
        self.assertEqual("experiment_id=12&observation_id=1&"
                         "variable_name=variable&channel=null",
                         response.request['QUERY_STRING'])
        self.assertEqual(404, response.status_code)

    def test_update_variable_option_insufficient_params(self):
        response = self.client.get("/api/update-variable-option/")
        self.assertEqual("", response.request['QUERY_STRING'])
        self.assertEqual(400, response.status_code)
