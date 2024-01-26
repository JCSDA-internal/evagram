-- PROCEDURE: public.create_plots()

-- DROP PROCEDURE IF EXISTS public.create_plots();

CREATE OR REPLACE PROCEDURE public.create_plots(
	)
LANGUAGE 'sql'
AS $BODY$
CREATE TABLE IF NOT EXISTS plots (
	plot_id serial PRIMARY KEY,
	div VARCHAR,
	script VARCHAR,
	experiment_id INTEGER NOT NULL,
	group_id INTEGER NOT NULL,
	observation_id INTEGER NOT NULL,
	CONSTRAINT fk_experiment
		FOREIGN KEY (experiment_id)
			REFERENCES experiments(experiment_id)
			ON DELETE CASCADE,
	CONSTRAINT fk_group
		FOREIGN KEY (group_id)
			REFERENCES groups(group_id)
			ON DELETE CASCADE,
	CONSTRAINT fk_observation
		FOREIGN KEY (observation_id)
			REFERENCES observations(observation_id)
			ON DELETE CASCADE,
	UNIQUE(experiment_id, group_id, observation_id)
);
$BODY$;
ALTER PROCEDURE public.create_plots()
    OWNER TO postgres;
