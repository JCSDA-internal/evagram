from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from api.models import Experiments, Owners, Observations, Groups, Plots
from api.serializers import *
from django.core.exceptions import ObjectDoesNotExist


@api_view(['GET'])
def initial_load(request):
    data = {
        "owners": []
    }
    data["owners"] = get_owners()
    return Response(data)


@api_view(['GET'])
def get_plots_by_field(request):
    try:
        owner_id = request.GET["owner_id"]
        experiment_id = request.GET["experiment_id"]
        observation_id = request.GET["observation_id"]
        variable_name = request.GET["variable_name"]
        channel = request.GET["channel"]
        group_id = request.GET["group_id"]

        plots = Plots.objects.none()

        # invalid input
        if owner_id == "null":
            serializer = PlotSerializer(plots, many=True)
            return Response(
                {"error": "Please specify a username. The 'null' value is not a valid username."},
                status=400)

        # get plots by owner field
        elif experiment_id == "null":
            experiments = Experiments.objects.filter(owner_id=owner_id)
            plots = Plots.objects.filter(experiment_id__in=experiments)

        # check if owner and experiment are selected
        if owner_id != "null" and experiment_id != "null":
            # verify experiment is part of owner
            experiments = Experiments.objects.filter(owner_id=owner_id)
            current_experiment = Experiments.objects.get(experiment_id=experiment_id)
            error_msg = ("The selected experiment cannot be found with the given username. "
                         "Please make sure both the username and experiment exists "
                         "and experiment is a part of that username.")
            assert current_experiment in experiments, error_msg
            # get plots by experiment field
            if observation_id == "null":
                plots = Plots.objects.filter(experiment=experiment_id)

            # get plots by observation field
            elif variable_name == "null":
                plots = Plots.objects.filter(experiment=experiment_id,
                                             observation=observation_id)

            # get plots by variable field
            elif group_id == "null":
                # lookup variable id by variable name and channel
                if channel == "null":
                    channel = None
                variable_id = Variables.objects.get(
                    variable_name=variable_name, channel=channel).variable_id
                plots = Plots.objects.filter(
                    experiment=experiment_id,
                    observation=observation_id,
                    variable=variable_id)

            elif group_id != "":
                if channel == "null":
                    channel = None
                variable_id = Variables.objects.get(variable_name=variable_name, channel=channel)
                plots = Plots.objects.filter(experiment=experiment_id,
                                             group=group_id,
                                             observation=observation_id,
                                             variable=variable_id)

        serializer = PlotSerializer(plots, many=True)
        return Response(serializer.data)

    except AssertionError as e:
        return Response({"error": str(e)}, status=400)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=404)


@api_view(['GET'])
def update_user_option(request):
    try:
        owner_id = request.GET["owner_id"]
        Owners.objects.get(pk=owner_id)
        data = {
            "experiments": [],
            "observations": [],
            "variables": [],
            "groups": []
        }
        data["experiments"] = get_experiments_by_owner(owner_id)
        return Response(data)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=404)


@api_view(['GET'])
def update_experiment_option(request):
    try:
        experiment_id = request.GET["experiment_id"]
        Experiments.objects.get(pk=experiment_id)
        data = {
            "observations": [],
            "variables": [],
            "groups": []
        }
        data["observations"] = get_observations_by_experiment(experiment_id)
        return Response(data)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=404)


@api_view(['GET'])
def update_observation_option(request):
    try:
        experiment_id = request.GET["experiment_id"]
        observation_id = request.GET["observation_id"]
        Observations.objects.get(pk=observation_id)
        data = {
            "variables": [],
            "groups": [],
            "variablesMap": {}
        }
        data["variables"] = get_variables_by_observation(experiment_id, observation_id)
        variablesMap = {}
        for variable in data["variables"]:
            variablesMap[variable['variable_name']] = variablesMap.get(
                variable['variable_name'], []) + [variable['channel']]
        data["variablesMap"] = variablesMap
        return Response(data)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=404)


@api_view(['GET'])
def update_variable_option(request):
    try:
        experiment_id = request.GET["experiment_id"]
        observation_id = request.GET["observation_id"]
        variable_name = request.GET["variable_name"]
        channel = request.GET["channel"]
        variable_id = None

        if channel == "null":
            # check if variable does not include a channel by default,
            # otherwise it has not been configured yet in the PlotMenu
            queryset = Variables.objects.filter(variable_name=variable_name, channel=None)
            if len(queryset) == 1:
                variable_id = queryset[0].variable_id
            # pull the top channel from variable name
            else:
                queryset = Variables.objects.filter(variable_name=variable_name)
                assert len(queryset) > 0
                channel = queryset[0].channel
                variable_id = queryset[0].variable_id
        # get variable id from variable name and channel
        else:
            variable_id = Variables.objects.get(
                variable_name=variable_name, channel=channel).variable_id

        data = {
            "groups": [],
            "channel": channel
        }
        data["groups"] = get_groups_by_variable(experiment_id, observation_id, variable_id)
        return Response(data)

    except AssertionError as e:
        error_msg = ("Unable to locate the given variable name. "
                     "Make sure a channel option is selected if exists.")
        return Response({"error": error_msg}, status=404)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=404)


def get_owners():
    queryset = Owners.objects.all()
    serializer = OwnerSerializer(queryset, many=True)
    return serializer.data


def get_experiments_by_owner(pk_owner):
    queryset = Experiments.objects.filter(owner_id=pk_owner)
    serializer = ExperimentSerializer(queryset, many=True)
    return serializer.data


def get_observations_by_experiment(pk_experiment):
    queryset = Observations.objects.filter(plots__experiment_id=pk_experiment).distinct()
    serializer = ObservationSerializer(queryset, many=True)
    return serializer.data


def get_variables_by_observation(pk_experiment, pk_observation):
    queryset = Plots.objects.filter(experiment=pk_experiment,
                                    observation=pk_observation).values_list("variable_id")
    variables = Variables.objects.filter(variable_id__in=queryset)
    serializer = VariableSerializer(variables, many=True)
    return serializer.data


def get_groups_by_variable(pk_experiment, pk_observation, pk_variable):
    queryset = Plots.objects.filter(experiment=pk_experiment,
                                    observation=pk_observation,
                                    variable=pk_variable).values_list("group_id")
    groups = Groups.objects.filter(group_id__in=queryset)
    serializer = GroupSerializer(groups, many=True)
    return serializer.data
