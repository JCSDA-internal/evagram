select owners.username, experiments.experiment_id, experiments.experiment_name, plots.plot_id, observation_name, variable_name, variables.variable_id, channel, group_name, groups.group_id from owners
join experiments on owners.owner_id = experiments.owner_id
join plots on plots.experiment_id = experiments.experiment_id
join groups on groups.group_id = plots.group_id
join observations on observations.observation_id = plots.observation_id
join variables on variables.variable_id = observations.variable_id;