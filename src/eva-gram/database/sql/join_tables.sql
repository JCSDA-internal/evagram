select owners.username, experiments.experiment_id, experiments.experiment_name, plots.plot_id, group_name, groups.group_id, variable_name, channel, variables.variable_id from owners
join experiments on owners.owner_id = experiments.owner_id
join plots on plots.experiment_id = experiments.experiment_id
join groups on groups.group_id = plots.group_id
join observation_variable on observation_variable.observation_id = plots.observation_id
join variables on variables.variable_id = observation_variable.variable_id;