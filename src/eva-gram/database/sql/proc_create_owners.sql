-- PROCEDURE: public.create_owners()

-- DROP PROCEDURE IF EXISTS public.create_owners();

CREATE OR REPLACE PROCEDURE public.create_owners(
	)
LANGUAGE 'sql'
AS $BODY$
CREATE TABLE IF NOT EXISTS owners (
	owner_id serial PRIMARY KEY,
	first_name VARCHAR,
	last_name VARCHAR,
	username VARCHAR NOT NULL UNIQUE
);
$BODY$;
ALTER PROCEDURE public.create_owners()
    OWNER TO postgres;
