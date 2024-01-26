-- PROCEDURE: public.create_variables()

-- DROP PROCEDURE IF EXISTS public.create_variables();

CREATE OR REPLACE PROCEDURE public.create_variables(
	)
LANGUAGE 'sql'
AS $BODY$
CREATE TABLE IF NOT EXISTS variables (
	variable_id serial PRIMARY KEY,
	variable_name VARCHAR NOT NULL,
	channel INTEGER,
	UNIQUE(variable_name, channel)
);
$BODY$;
ALTER PROCEDURE public.create_variables()
    OWNER TO postgres;
