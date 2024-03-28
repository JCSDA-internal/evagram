from django.test import TestCase
import django.db
from api.models import *


class TestModels(TestCase):
    fixtures = ["test_data.json"]

    def test_insert_duplicate_owner(self):
        with self.assertRaises(django.db.IntegrityError):
            Owners.objects.create(username='jdoe')
            Owners.objects.create(username='jdoe')

    def test_delete_owner_cascade(self):
        owner = Owners.objects.get(pk=1)
        owner.delete()
        self.assertEqual(0, len(Owners.objects.filter(pk=1)))
        self.assertEqual(0, len(Experiments.objects.filter(owner=1)))

    def test_insert_experiment_insufficient_fields(self):
        with self.assertRaises(django.db.IntegrityError):
            Experiments.objects.create(experiment_name="experiment1")

    def test_insert_experiment_invalid_owner(self):
        with self.assertRaises(ValueError):
            Experiments.objects.create(experiment_name="experiment1", owner=-1)

    def test_insert_duplicate_experiment(self):
        with self.assertRaises(django.db.IntegrityError):
            owner = Owners.objects.create(username='jdoe')
            experiment1 = Experiments.objects.create(experiment_name="experiment1", owner=owner)
            experiment2 = Experiments.objects.create(experiment_name="experiment1", owner=owner)

    def test_delete_experiment_cascade(self):
        experiment = Experiments.objects.get(pk=1)
        experiment.delete()
        self.assertEqual(0, len(Experiments.objects.filter(pk=1)))
        self.assertEqual(0, len(Plots.objects.filter(experiment=1)))

    def test_insert_plot_insufficient_fields(self):
        # Missing fields: group, variable
        with self.assertRaises(django.db.IntegrityError):
            experiment = Experiments.objects.get(pk=1)
            observation = Observations.objects.get(pk=1)
            Plots.objects.create(experiment=experiment, observation=observation)

    def test_insert_plot_invalid_fields(self):
        # Invalid fields: experiment, observation
        with self.assertRaises(ValueError):
            group = Groups.objects.get(pk=1)
            variable = Variables.objects.get(pk=1)
            Plots.objects.create(experiment=-1, observation=-1, group=group, variable=variable)

    def test_insert_duplicate_observation(self):
        with self.assertRaises(django.db.IntegrityError):
            Observations.objects.create(observation_name="observation1")
            Observations.objects.create(observation_name="observation1")

    def test_query_existing_plots(self):
        # get all amsua_n18 plots in experiment "experiment_iv_1" where the user is thamzey
        owner = Owners.objects.get(username="thamzey")
        experiment = Experiments.objects.get(experiment_name="experiment_iv_1", owner=owner)
        observation = Observations.objects.get(observation_name="amsua_n18")
        queryset = Plots.objects.filter(experiment=experiment, observation=observation)
        self.assertEqual(2, len(queryset))
        self.assertTrue(Plots.objects.get(pk=114) in queryset)
        self.assertTrue(Plots.objects.get(pk=323) in queryset)

    def test_query_nonexistent_plots(self):
        # get all satwind plots in experiment "experiment_iv_1"
        experiment = Experiments.objects.get(experiment_name="experiment_iv_1")
        observation = Observations.objects.get(observation_name="satwind")
        queryset = Plots.objects.filter(experiment=experiment, observation=observation)
        self.assertEqual(0, len(queryset))
