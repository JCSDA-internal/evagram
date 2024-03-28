from django.db import models


class Experiments(models.Model):
    experiment_id = models.AutoField(primary_key=True)
    experiment_name = models.CharField(null=False, default="null")
    owner = models.ForeignKey('Owners', models.CASCADE)

    class Meta:
        db_table = 'experiments'
        unique_together = (('experiment_name', 'owner'),)


class Groups(models.Model):
    group_id = models.AutoField(primary_key=True)
    group_name = models.CharField(unique=True, null=False, default="null")

    class Meta:
        db_table = 'groups'


class Variables(models.Model):
    variable_id = models.AutoField(primary_key=True)
    variable_name = models.CharField(null=False, default="null")
    channel = models.IntegerField(blank=True, null=True)

    class Meta:
        db_table = 'variables'
        unique_together = (('variable_name', 'channel'),)


class Observations(models.Model):
    observation_id = models.AutoField(primary_key=True)
    observation_name = models.CharField(null=False, unique=True, default="null")

    class Meta:
        db_table = 'observations'


class Owners(models.Model):
    owner_id = models.AutoField(primary_key=True)
    first_name = models.CharField(null=True)
    last_name = models.CharField(null=True)
    username = models.CharField(null=False, unique=True, default="null")

    class Meta:
        db_table = 'owners'


class Plots(models.Model):
    plot_id = models.AutoField(primary_key=True)
    div = models.CharField(blank=True, null=True)
    script = models.CharField(blank=True, null=True)
    experiment = models.ForeignKey(Experiments, models.CASCADE)
    group = models.ForeignKey(Groups, models.CASCADE)
    observation = models.ForeignKey(Observations, models.CASCADE)
    variable = models.ForeignKey(Variables, models.CASCADE)

    class Meta:
        db_table = 'plots'
        unique_together = (('experiment', 'group', 'observation', 'variable'),)
