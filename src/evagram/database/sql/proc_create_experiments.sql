-- PROCEDURE: public.create_experiments()

-- DROP PROCEDURE IF EXISTS public.create_experiments();

CREATE OR REPLACE PROCEDURE public.create_experiments(
	)
LANGUAGE 'sql'
AS $BODY$
CREATE TABLE IF NOT EXISTS experiments (
	experiment_id serial PRIMARY KEY,
	experiment_name VARCHAR NOT NULL,
	owner_id INTEGER NOT NULL,
	CONSTRAINT fk_owner
		FOREIGN KEY (owner_id)
			REFERENCES owners(owner_id)
			ON DELETE CASCADE,
	UNIQUE(experiment_name, owner_id)
);
$BODY$;
ALTER PROCEDURE public.create_experiments()
    OWNER TO postgres;
