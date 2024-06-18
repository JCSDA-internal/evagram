from django.test import TestCase
from evagram_input import input_data
from api.models import *


class TestEvagramInputTool(TestCase):
    @classmethod
    def setUpTestData(cls):
        input_data(owner="postgres", experiment="experiment1", eva_directory="tests/eva")

    def test_OwnerInSession(self):
        queryset = Owners.objects.filter(username="postgres")
        self.assertEqual(1, len(queryset))

    def test_ExperimentInSession(self):
        owner = Owners.objects.get(username="postgres")
        queryset = Experiments.objects.filter(experiment_name="experiment1", owner=owner)
        self.assertEqual(1, len(queryset))

    def test_WrongRootOwner(self):
        with self.assertRaises(Exception):
            input_data(owner="test", experiment="experiment1", eva_directory="tests/eva")

    def test_ExperimentPathNotFound(self):
        with self.assertRaises(FileNotFoundError):
            input_data(owner="postgres", experiment="experiment1", eva_directory="not/a/path")

    def test_RollbackOnException(self):
        with self.assertRaises(Exception):
            input_data(owner="postgres", experiment="bad_experiment", eva_directory="tests/dummy")

        owner = Owners.objects.get(username="postgres")
        experiments = Experiments.objects.filter(experiment_name="bad_experiment", owner=owner)
        self.assertEqual(0, len(experiments))
        observations = Observations.objects.filter(
            observation_name="airs_aqua").filter(observation_name="eva")
        self.assertEqual(0, len(observations))
