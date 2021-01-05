-----------------------------
-- test data
drop table if exists public.tbl_index_case;
drop table if exists public.tbl_with_pk;

CREATE TABLE public.tbl_index_case
(
    id bigserial,
    text_fld text,
    text_fld_2 character varying(10),
    fld_1 integer,
    fld_2 integer,
    fld_3 integer
);

CREATE INDEX tbl_index_case_text_fld_idx ON tbl_index_case (text_fld) WITH (fillfactor = 100);
CREATE INDEX tbl_index_case_text_fld_idx1 ON tbl_index_case using hash (text_fld);

INSERT INTO tbl_index_case (text_fld, fld_1, fld_2, fld_3)
    SELECT T.v || 'abcdabcdabcdabcdabcdabcdabcd', T.v, T.v, T.v
    FROM (
        select generate_series(1, 100000) as v
    ) T;

CREATE INDEX tbl_index_case_text_fld_idx2 ON tbl_index_case using btree (text_fld, fld_1, fld_2, fld_3);
CREATE INDEX tbl_index_case_text_fld_idx3 ON tbl_index_case (text_fld_2);
CREATE INDEX tbl_index_case_fld_1_idx4 ON tbl_index_case using btree (fld_1);
CREATE INDEX tbl_index_case_text_fld_idx_dub_1 ON tbl_index_case using btree (fld_1);
CREATE INDEX tbl_index_case_text_fld_idx_dub_2 ON tbl_index_case using btree (fld_1, fld_2);
CREATE INDEX tbl_index_case_text_fld_idx_dub_3 ON tbl_index_case using btree (fld_1, fld_2, fld_3);
CREATE INDEX tbl_index_case_text_fld_idx_dub_4 ON tbl_index_case using btree (fld_3, fld_2, fld_1);
CREATE INDEX tbl_index_case_text_fld_idx_dub_5 ON tbl_index_case using btree (fld_1);

CREATE TABLE public.tbl_with_pk
(
    id bigserial,
    text_fld text,
    fld_1 integer,
    fld_2 integer,
    fld_3 integer,
    CONSTRAINT tbl_with_pk_pkey PRIMARY KEY (id)
);

do $$
begin
    for counter in 1..1100 loop
        perform * from tbl_index_case where fld_1 = counter;
    end loop;
end$$;

analyze public.tbl_index_case;
-----------------------------
-- table without indexes
drop table if exists public.tbl_index_case_2;
CREATE TABLE public.tbl_index_case_2
(
    id bigserial,
    fld_1 integer
);

INSERT INTO tbl_index_case_2 (fld_1)
    select generate_series(1, 10);
	
do $$
begin
    for counter in 1..1100 loop
        perform * from tbl_index_case_2 where fld_1 = 1;
    end loop;
end$$;

INSERT INTO tbl_index_case_2 (fld_1)
    select generate_series(1, 250000);

analyze public.tbl_index_case_2;
-----------------------------
-- table with missed index
drop table if exists public.tbl_index_case_3;
CREATE TABLE public.tbl_index_case_3
(
    id bigserial,
    fld_1 integer,
	CONSTRAINT tbl_index_case_3_pkey PRIMARY KEY (id)
);

INSERT INTO tbl_index_case_3 (fld_1)
    select generate_series(1, 250000);

do $$
begin
    for counter in 1..1100 loop
		-- emulate seq_tup_read/seq_scan > 1000
        perform * from tbl_index_case_3 limit 20000;
    end loop;
end$$;

analyze public.tbl_index_case_3;
-----------------------------
-- fk test data
drop table if exists public.tbl_a cascade;
drop table if exists public.tbl_b cascade;

CREATE TABLE public.tbl_a
(
    id bigserial,
    tbl_b_id integer,        -- <---- needs index
    some_fld integer,
    CONSTRAINT tbl_a_pk PRIMARY KEY (id)
);

CREATE TABLE public.tbl_b
(
    id bigserial,
    tbl_a_id integer,        -- <---- needs index
    some_fld integer,
    CONSTRAINT tbl_b_pk PRIMARY KEY (id)
);

INSERT INTO tbl_a (tbl_b_id) SELECT generate_series(1,10000);
INSERT INTO tbl_b (tbl_a_id) SELECT generate_series(1,10000);

ALTER TABLE public.tbl_a
    ADD CONSTRAINT fk_tbl_b FOREIGN KEY (tbl_b_id)
    REFERENCES public.tbl_b (id) ON DELETE CASCADE;

ALTER TABLE public.tbl_b
    ADD CONSTRAINT fk_tbl_a FOREIGN KEY (tbl_a_id)
    REFERENCES public.tbl_a (id) ON DELETE CASCADE;
-----------------------------