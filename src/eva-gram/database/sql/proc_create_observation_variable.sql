-- PROCEDURE: public.create_observation_variable()

-- DROP PROCEDURE IF EXISTS public.create_observation_variable();

CREATE OR REPLACE PROCEDURE public.create_observation_variable(
	)
LANGUAGE 'sql'
AS $BODY$
CREATE TABLE observation_variable (
	observation_id INTEGER NOT NULL,
	variable_id INTEGER NOT NULL,
	CONSTRAINT observation_variable_id PRIMARY KEY (observation_id, variable_id),
	CONSTRAINT fk_observation
		FOREIGN KEY (observation_id)
			REFERENCES observations(observation_id)
			ON DELETE CASCADE,
	CONSTRAINT fk_variable
		FOREIGN KEY (variable_id)
			REFERENCES variables(variable_id)
			ON DELETE CASCADE
);
$BODY$;
ALTER PROCEDURE public.create_observation_variable()
    OWNER TO postgres;
