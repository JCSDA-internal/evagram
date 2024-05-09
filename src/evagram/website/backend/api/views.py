from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from api.models import Experiments, Owners, Observations, Groups, Plots
from api.serializers import *
from django.core.exceptions import ObjectDoesNotExist


@api_view(['GET'])
def initial_load(request):
    data = {
        "owners": [],
        "experiments": [],
        "groups": [],
        "observations": [],
        "variables": []
    }
    data["owners"] = get_owners()
    if len(data["owners"]) > 0:
        first_owner = data["owners"][0]["owner_id"]
        data["experiments"] = get_experiments_by_owner(first_owner)
    if len(data["experiments"]) > 0:
        first_experiment = data["experiments"][0]["experiment_id"]
        data["observations"] = get_observations_by_experiment(first_experiment)
    if len(data["observations"]) > 0:
        first_observation = data["observations"][0]["observation_id"]
        data["variables"] = get_variables_by_observation(first_observation)
    if len(data["variables"]) > 0:
        first_variable = data["variables"][0]["variable_id"]
        data["groups"] = get_groups_by_variable(first_variable)
    return Response(data)


@api_view(['GET'])
def get_plot_components(request):
    try:
        experiment_id = request.GET["experiment_id"]
        observation_id = request.GET["observation_id"]
        variable_id = request.GET["variable_id"]
        group_id = request.GET["group_id"]

        plot = Plots.objects.get(experiment=experiment_id,
                                 group=group_id,
                                 observation=observation_id,
                                 variable=variable_id)
        serializer = PlotSerializer(plot)
        return Response(serializer.data)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=400)

@api_view(['GET'])
def get_plots_by_field(request):
    try:
        owner_id = request.GET["owner_id"]
        experiment_id = request.GET["experiment_id"]
        observation_id = request.GET["observation_id"]
        variable_id = request.GET["variable_id"]
        group_id = request.GET["group_id"]

        # get plots by owner
        if experiment_id == "placeholder":
            experiments = Experiments.objects.filter(owner_id=owner_id)
            plots = Plots.objects.filter(experiment_id__in=experiments)

        # get plots by experiment
        elif observation_id == "placeholder":
            plots = Plots.objects.filter(experiment_id=experiment_id)

        # get plots by observation
        elif variable_id == "placeholder":
            plots = Plots.objects.filter(experiment_id=experiment_id, observation_id=observation_id)

        # get plots by variable
        elif group_id == "placeholder":
            plots = Plots.objects.filter(experiment_id=experiment_id, observation_id=observation_id, variable_id=variable_id)
        
        elif group_id != "":
            plots = Plots.objects.filter(experiment=experiment_id,
                                 group=group_id,
                                 observation=observation_id,
                                 variable=variable_id)

        else:
            return Response()
            
        serializer = PlotSerializer(plots, many=True)
        return Response(serializer.data)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=400)

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
        if len(data["experiments"]) > 0:
            data["observations"] = get_observations_by_experiment(
                data["experiments"][0]["experiment_id"])
        if len(data["observations"]) > 0:
            data["variables"] = get_variables_by_observation(
                data["observations"][0]["observation_id"])
        if len(data["variables"]) > 0:
            data["groups"] = get_groups_by_variable(data["variables"][0]["variable_id"])
        return Response(data)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=400)


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
        if len(data["observations"]) > 0:
            data["variables"] = get_variables_by_observation(
                data["observations"][0]["observation_id"])
        if len(data["variables"]) > 0:
            data["groups"] = get_groups_by_variable(data["variables"][0]["variable_id"])
        return Response(data)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=400)


@api_view(['GET'])
def update_observation_option(request):
    try:
        observation_id = request.GET["observation_id"]
        Observations.objects.get(pk=observation_id)
        data = {
            "variables": [],
            "groups": []
        }
        data["variables"] = get_variables_by_observation(observation_id)
        if len(data["variables"]) > 0:
            data["groups"] = get_groups_by_variable(data["variables"][0]["variable_id"])
        return Response(data)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=400)


@api_view(['GET'])
def update_variable_option(request):
    try:
        variable_id = request.GET["variable_id"]
        Variables.objects.get(pk=variable_id)
        data = {
            "groups": []
        }
        data["groups"] = get_groups_by_variable(variable_id)
        return Response(data)

    except ValueError as e:
        return Response({"error": str(e)}, status=400)

    except KeyError as e:
        error_msg = "Missing request parameter detected: {}".format(str(e))
        return Response({"error": error_msg}, status=400)

    except ObjectDoesNotExist as e:
        return Response({"error": str(e)}, status=400)


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


def get_variables_by_observation(pk_observation):
    queryset = Plots.objects.filter(observation_id=pk_observation).values_list("variable_id")
    variables = Variables.objects.filter(variable_id__in=queryset)
    serializer = VariableSerializer(variables, many=True)
    return serializer.data


def get_groups_by_variable(pk_variable):
    queryset = Plots.objects.filter(variable_id=pk_variable).values_list("group_id")
    groups = Groups.objects.filter(group_id__in=queryset)
    serializer = GroupSerializer(groups, many=True)
    return serializer.data
