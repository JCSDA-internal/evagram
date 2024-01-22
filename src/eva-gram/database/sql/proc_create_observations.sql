-- PROCEDURE: public.create_observations()

-- DROP PROCEDURE IF EXISTS public.create_observations();

CREATE OR REPLACE PROCEDURE public.create_observations(
	)
LANGUAGE 'sql'
AS $BODY$
CREATE TABLE IF NOT EXISTS observations (
	observation_id serial PRIMARY KEY,
	observation_name VARCHAR NOT NULL,
	variable_id INTEGER NOT NULL,
	CONSTRAINT fk_variable
		FOREIGN KEY (variable_id)
			REFERENCES variables(variable_id)
			ON DELETE CASCADE 
);
$BODY$;
ALTER PROCEDURE public.create_observations()
    OWNER TO postgres;
