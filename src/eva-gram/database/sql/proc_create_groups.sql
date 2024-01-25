-- PROCEDURE: public.create_groups()

-- DROP PROCEDURE IF EXISTS public.create_groups();

CREATE OR REPLACE PROCEDURE public.create_groups(
	)
LANGUAGE 'sql'
AS $BODY$
CREATE TABLE IF NOT EXISTS groups (
	group_id serial PRIMARY KEY,
	group_name VARCHAR NOT NULL UNIQUE
);
$BODY$;
ALTER PROCEDURE public.create_groups()
    OWNER TO postgres;
