from rest_framework import serializers
from api.models import *
from django.db import models


class OwnerSerializer(serializers.ModelSerializer):
    key = serializers.ModelField(model_field=Owners()._meta.get_field('owner_id'))
    value = serializers.ModelField(model_field=Owners()._meta.get_field('owner_id'))
    content = serializers.ModelField(model_field=Owners()._meta.get_field('username'))
    type = models.CharField(default='owner')

    class Meta:
        model = Owners
        # fields = ['owner_id', 'first_name', 'last_name', 'username']
        fields = ['key', 'value', 'content', 'owner_id', 'username']


class ExperimentSerializer(serializers.ModelSerializer):
    key = serializers.ModelField(model_field=Experiments()._meta.get_field('experiment_id'))
    value = serializers.ModelField(model_field=Experiments()._meta.get_field('experiment_id'))
    content = serializers.ModelField(model_field=Experiments()._meta.get_field('experiment_name'))
    type = models.CharField(default='experiment')

    class Meta:
        model = Experiments
        fields = ['key', 'value', 'content', 'experiment_id', 'experiment_name']


class ObservationSerializer(serializers.ModelSerializer):
    key = serializers.ModelField(model_field=Observations()._meta.get_field('observation_id'))
    value = serializers.ModelField(model_field=Observations()._meta.get_field('observation_id'))
    content = serializers.ModelField(model_field=Observations()._meta.get_field('observation_name'))
    type = models.CharField(default='observation')

    class Meta:
        model = Observations
        fields = ['key', 'value', 'content', 'observation_id', 'observation_name']


class VariableSerializer(serializers.ModelSerializer):
    key = serializers.ModelField(model_field=Variables()._meta.get_field('variable_id'))
    value = serializers.ModelField(model_field=Variables()._meta.get_field('variable_id'))
    content = serializers.ModelField(model_field=Variables()._meta.get_field('variable_name'))
    type = models.CharField(default='variable')

    class Meta:
        model = Variables
        fields = ['key', 'value', 'content', 'variable_id', 'variable_name', 'channel']


class GroupSerializer(serializers.ModelSerializer):
    key = serializers.ModelField(model_field=Groups()._meta.get_field('group_id'))
    value = serializers.ModelField(model_field=Groups()._meta.get_field('group_id'))
    content = serializers.ModelField(model_field=Groups()._meta.get_field('group_name'))
    type = models.CharField(default='group')

    class Meta:
        model = Groups
        fields = ['key', 'value', 'content', 'group_id', 'group_name']


class PlotSerializer(serializers.ModelSerializer):
    class Meta:
        model = Plots
        fields = ['plot_id', 'div', 'script']
