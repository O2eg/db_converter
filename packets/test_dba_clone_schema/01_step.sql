DROP schema if exists schema_1 cascade;
DROP schema if exists template cascade;

CREATE schema template;

DROP TABLE IF EXISTS template.test_tbl;

CREATE TABLE template.test_tbl
(
    id serial,
    fld_1 bigint,
    fld_2 text
);

CREATE FUNCTION template.dbc_ins_tg_test_tbl_func()
	RETURNS trigger
	LANGUAGE 'plpgsql'
	COST 100
AS $BODY$
  BEGIN
	NEW.id := NEW.id + 100;
	return NEW;
  END;
$BODY$;

CREATE TRIGGER test_tbl_tg
	BEFORE INSERT
	ON template.test_tbl
	FOR EACH ROW
	EXECUTE PROCEDURE template.dbc_ins_tg_test_tbl_func();

set search_path = 'template', 'public';

CREATE VIEW template.test_tbl_v as
	SELECT * FROM template.test_tbl;

CREATE OR REPLACE FUNCTION template.test_tbl_f() 
	RETURNS TABLE (
		id integer,
		fld_1 bigint,
		fld_2 text
) 
AS $$
BEGIN
	RETURN QUERY SELECT * FROM test_tbl_v;
END; $$ 
LANGUAGE 'plpgsql';
