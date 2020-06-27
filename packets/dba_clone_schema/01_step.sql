-- ------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.clone_schema_exec(
	text,
	text[],
	boolean
);
-- ------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.clone_schema(
	text,
	text,
	boolean
);
-- ------------------------------------------------------------------------------
create or replace function public.clone_schema_exec(
	p_descr text,
	p_cdms text[],
	p_dry_run boolean
)
returns void
language plpgsql
as $$
declare
	r record;
begin
	raise notice '% - clone_schema_exec: started %...', (select clock_timestamp()), p_descr;
	for r in (select unnest(p_cdms) as v) loop
		raise notice '%', r.v;
		if p_dry_run = false then
			execute r.v;
		end if;
	end loop;
return;
end$$;
-- ------------------------------------------------------------------------------
create or replace function public.clone_schema(
	p_nsp_template text,
	p_nsp_new text,
	p_dry_run boolean default false
)
returns boolean
language plpgsql
as $$
declare
	tv text;
	v_state   text;
	v_msg	 text;
	v_detail  text;
	v_hint	text;
	v_context text;

	ddl_nsp text[];
	ddl_tbls text[];
	ddl_sequences text[];
	ddl_own_sequences text[];
	ddl_defaults text[];
	ddl_fks text[];

	ddl_views text[];
	ddl_funcs text[];
	ddl_tgs text[];

	dcl_nsp text[];
	dcl_tbls text[];
	dcl_funcs text[];

	r record;
begin

	if not exists(select 1 from pg_namespace where nspname = p_nsp_template) then
		raise exception 'Template namespace % does not exists!', p_nsp_template;
		return false;
	end if;

	if exists(select 1 from pg_namespace where nspname = p_nsp_new) then
		raise exception 'Namespace % already exists!', p_nsp_new;
		return false;
	end if;

	if p_dry_run then
		raise notice E'dry_run mode is on!\n';
	end if;

	ddl_nsp = array_append(ddl_nsp, format('CREATE SCHEMA %I', p_nsp_new));
	ddl_nsp = array_append(ddl_nsp, format('SET search_path = ''%I'', ''operating'', ''public''', p_nsp_new));

	-- ddl_tbls
	raise notice '% - Started ddl_tbls...', (select clock_timestamp());
	FOR r IN SELECT relname, relpersistence, relkind
		FROM pg_class c
		JOIN pg_namespace n on c.relnamespace = n.oid
		WHERE n.nspname = quote_ident(p_nsp_template) and relkind in ('r', 'f')
	LOOP
		-- raise notice '% %', r.relname, r.relpersistence;
		IF r.relkind = 'r' AND r.relpersistence = 'u' THEN
			tv = 'UNLOGGED TABLE';
		ELSEIF r.relkind = 'r' AND r.relpersistence = 'p' THEN
			tv = 'TABLE';
		ELSE
			raise exception 'Not supported: relkind = %, relpersistence = %', r.relkind, r.relpersistence;
		END IF;

		ddl_tbls = array_append(ddl_tbls, 
			format('CREATE %s %I.%I (LIKE %I.%I INCLUDING ALL)',
				tv,
				p_nsp_new,
				r.relname,
				p_nsp_template,
				r.relname
			)
		);
	END LOOP;

	-- ddl_sequences
	raise notice '% - Started ddl_sequences...', (select clock_timestamp());
	FOR r IN SELECT relname, relkind
		FROM pg_class c
		JOIN pg_namespace n on c.relnamespace = n.oid
		WHERE n.nspname = quote_ident(p_nsp_template) and relkind = 'S'
	LOOP
		ddl_sequences = array_append(ddl_sequences, 
			format('CREATE SEQUENCE %I.%I',
				p_nsp_new,
				r.relname
			)
		);
	END LOOP;

	-- ddl_own_sequences
	raise notice '% - Started ddl_own_sequences...', (select clock_timestamp());
	FOR r IN SELECT * from (
		SELECT
			c.relname, a.attname, (
				select pg_get_serial_sequence(
					(quote_ident(n.nspname) || '.' || quote_ident(c.relname)),
					quote_ident(a.attname)
				)
			) as pgs
		FROM pg_class c
		JOIN pg_namespace n on n.oid = c.relnamespace
		JOIN pg_attribute a on a.attrelid = c.oid
		JOIN pg_attrdef ad on a.attrelid = ad.adrelid AND a.attnum=ad.adnum
		WHERE c.relkind = 'r'
			and n.nspname = quote_ident(p_nsp_template)
			and a.attnum > 0
			and not attisdropped
			and atthasdef
	) T WHERE T.pgs is not null
	LOOP
		ddl_own_sequences = array_append(ddl_own_sequences,
			format('ALTER SEQUENCE %s OWNED BY %I.%I.%I',
				REPLACE(
					r.pgs,
					quote_ident(p_nsp_template) || '.',
					quote_ident(p_nsp_new) || '.'
				),
				p_nsp_new,
				r.relname,
				r.attname
			)
		);
	END LOOP;

	-- ddl_defaults
	raise notice '% - Started ddl_defaults...', (select clock_timestamp());
	FOR r IN SELECT
			c.relname, a.attname, pg_get_expr(ad.adbin, ad.adrelid) as adsrc
		from pg_class c
		join pg_namespace n on n.oid = c.relnamespace
		join pg_attribute a on a.attrelid = c.oid
		join pg_attrdef ad on a.attrelid = ad.adrelid AND a.attnum=ad.adnum
		where c.relkind = 'r'
			and n.nspname = quote_ident(p_nsp_template)
			and a.attnum > 0
			and not attisdropped
			and atthasdef
	LOOP
		ddl_defaults = array_append(ddl_defaults,
			format('ALTER TABLE %I.%I ALTER COLUMN %I SET DEFAULT %s',
				p_nsp_new,
				r.relname,
				r.attname,
				REPLACE(
					r.adsrc,
					quote_ident(p_nsp_template) || '.',
					quote_ident(p_nsp_new) || '.'
				)
			)
		);
	END LOOP;

	-- ddl_fks
	raise notice '% - Started ddl_fks...', (select clock_timestamp());
	FOR r IN SELECT c.relname, cn.conname, pg_get_constraintdef(cn.oid) as condef
		FROM pg_constraint cn
		JOIN pg_namespace n on cn.connamespace = n.oid
		JOIN pg_class c ON c.oid = cn.conrelid
		WHERE n.nspname = quote_ident(p_nsp_template) and contype not in ('p', 'u')
	LOOP
		ddl_fks = array_append(ddl_fks,
			format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
				p_nsp_new,
				r.relname,
				r.conname,
				REPLACE(
					r.condef,
					'REFERENCES ' || quote_ident(p_nsp_template) || '.',
					'REFERENCES ' || quote_ident(p_nsp_new) || '.'
				)
			)
		);
	END LOOP;

	-- ddl_views
	raise notice '% - Started ddl_views...', (select clock_timestamp());
	FOR r IN SELECT
			n.nspname,
			c.relname,
			case when c.relkind = 'm' then
				'MATERIALIZED VIEW'
			when c.relkind = 'v' then
				'VIEW'
			end as obj,
			pg_get_viewdef(c.oid, true) as def
		FROM pg_class c
		JOIN pg_namespace n on n.oid = c.relnamespace
		WHERE
			c.relkind in ('v', 'm') and n.nspname = quote_ident(p_nsp_template)
	LOOP
		ddl_views = array_append(ddl_views,
			format('CREATE %s %I.%I as %s',
				r.obj,
				p_nsp_new,
				r.relname,
				REPLACE(
					r.def,
					quote_ident(p_nsp_template) || '.',
					quote_ident(p_nsp_new) || '.'
				)
			)
		);
	END LOOP;

	-- ddl_funcs
	raise notice '% - Started ddl_funcs...', (select clock_timestamp());
	FOR r IN SELECT
			n.nspname,
			p.proname,
			pg_get_functiondef(p.oid) as def
		FROM pg_proc p
		JOIN pg_namespace n on n.oid = p.pronamespace
		WHERE
			n.nspname = quote_ident(p_nsp_template)
	LOOP
		ddl_funcs = array_append(ddl_funcs,
			REPLACE(
				r.def,
				quote_ident(p_nsp_template) || '.',
				quote_ident(p_nsp_new) || '.'
			)
		);
	END LOOP;

	-- ddl_tgs
	raise notice '% - Started ddl_tgs...', (select clock_timestamp());
	FOR r IN SELECT
			n.nspname,
			t.tgname,
			pg_get_triggerdef(t.oid) as def
		FROM pg_trigger t
		JOIN pg_class c on c.oid = t.tgrelid
		JOIN pg_namespace n on n.oid = c.relnamespace
		WHERE
			n.nspname = quote_ident(p_nsp_template) and t.tgisinternal = false
	LOOP
		ddl_tgs = array_append(ddl_tgs,
			REPLACE(
				r.def,
				quote_ident(p_nsp_template) || '.',
				quote_ident(p_nsp_new) || '.'
			)
		);
	END LOOP;

	-- -----------------------------------------------------------------------------
	-- dcl_nsp
	raise notice '% - Started dcl_nsp...', (select clock_timestamp());
	FOR r IN SELECT
			COALESCE(gt.rolname, 'PUBLIC') AS grantee,
			-- g.rolname AS grantor,
			string_agg(b.privilege_type, ', ') AS privileges
		FROM
			(SELECT
				(d).grantee AS grantee, (d).grantor AS grantor,
				(d).is_grantable AS is_grantable,
				(d).privilege_type
			FROM
				(SELECT aclexplode(nsp.nspacl) as d
				FROM pg_namespace nsp
				WHERE nspname = quote_ident(p_nsp_template)
				) a
			) b
			LEFT JOIN pg_catalog.pg_roles g ON (b.grantor = g.oid)
			LEFT JOIN pg_catalog.pg_roles gt ON (b.grantee = gt.oid)
		GROUP BY g.rolname, gt.rolname
	LOOP
		dcl_nsp = array_append(dcl_nsp,
			format('GRANT %s ON SCHEMA %I TO %I',
				r.privileges,
				quote_ident(p_nsp_new),
				r.grantee
			)
		);
	END LOOP;

	-- dcl_tbls
	raise notice '% - Started dcl_tbls...', (select clock_timestamp());
	FOR r IN SELECT
			b.relname,
			case
				when b.relkind in ('r', 'v', 'm', 'f') then 'TABLE'
				when b.relkind in ('S') then 'SEQUENCE'
			else 'todo'
			end as obj,
			COALESCE(gt.rolname, 'PUBLIC') AS grantee,
			-- g.rolname AS grantor,
			string_agg(b.privilege_type, ', ') AS privileges
		FROM
			(SELECT
				(d).grantee AS grantee, (d).grantor AS grantor,
				(d).is_grantable AS is_grantable,
				(d).privilege_type, a.relname, a.relkind
			FROM
				(SELECT c.relname, c.relkind, aclexplode(c.relacl) as d
				FROM pg_namespace nsp
				JOIN pg_class c ON nsp.oid = c.relnamespace
				WHERE nspname = quote_ident(p_nsp_template) and relkind in ('r', 'm', 'v', 'S', 'f')
				) a
			) b
			LEFT JOIN pg_catalog.pg_roles g ON (b.grantor = g.oid)
			LEFT JOIN pg_catalog.pg_roles gt ON (b.grantee = gt.oid)
		GROUP BY b.relname, b.relkind, g.rolname, gt.rolname
		ORDER BY b.relname, b.relkind, g.rolname
	LOOP
		dcl_tbls = array_append(dcl_tbls,
			format('GRANT %s ON %s %I.%I TO %I',
				r.privileges,
				r.obj,
				quote_ident(p_nsp_new),
				quote_ident(r.relname),
				r.grantee
			)
		);
	END LOOP;

	-- dcl_funcs
	raise notice '% - Started dcl_funcs...', (select clock_timestamp());
	FOR r IN SELECT
			b.proname, b.def,
			COALESCE(gt.rolname, 'PUBLIC') AS grantee,
			-- g.rolname AS grantor,
			string_agg(b.privilege_type, ', ') AS privileges
		FROM
			(SELECT
				(d).grantee AS grantee, (d).grantor AS grantor,
				(d).is_grantable AS is_grantable,
				(d).privilege_type, a.proname, a.def
			FROM
				(SELECT p.proname, aclexplode(p.proacl) as d,
					pg_get_function_arguments(p.oid) as def
				FROM pg_namespace nsp
				JOIN pg_proc p ON nsp.oid = p.pronamespace
				WHERE nspname = quote_ident(p_nsp_template)
				) a
			) b
			LEFT JOIN pg_catalog.pg_roles g ON (b.grantor = g.oid)
			LEFT JOIN pg_catalog.pg_roles gt ON (b.grantee = gt.oid)
		GROUP BY b.proname, b.def, g.rolname, gt.rolname
		ORDER BY b.proname, b.def, g.rolname
	LOOP
		dcl_funcs = array_append(dcl_funcs,
			format('GRANT %s ON FUNCTION %I.%I(%s) TO %I',
				r.privileges,
				quote_ident(p_nsp_new),
				quote_ident(r.proname),
				r.def,
				r.grantee
			)
		);
	END LOOP;
	-- -----------------------------------------------------------------------------
	perform public.clone_schema_exec('ddl_nsp', ddl_nsp, p_dry_run);
	perform public.clone_schema_exec('ddl_tbls', ddl_tbls, p_dry_run);
	perform public.clone_schema_exec('ddl_sequences', ddl_sequences, p_dry_run);
	perform public.clone_schema_exec('ddl_own_sequences', ddl_own_sequences, p_dry_run);
	perform public.clone_schema_exec('ddl_defaults', ddl_defaults, p_dry_run);
	perform public.clone_schema_exec('ddl_fks', ddl_fks, p_dry_run);

	perform public.clone_schema_exec('ddl_views', ddl_views, p_dry_run);
	perform public.clone_schema_exec('ddl_funcs', ddl_funcs, p_dry_run);
	perform public.clone_schema_exec('ddl_tgs', ddl_tgs, p_dry_run);

	perform public.clone_schema_exec('dcl_nsp', dcl_nsp, p_dry_run);
	perform public.clone_schema_exec('dcl_tbls', dcl_tbls, p_dry_run);
	perform public.clone_schema_exec('dcl_funcs', dcl_funcs, p_dry_run);

	if p_dry_run then
		RETURN false;
	end if;

	raise notice '% - Done', (select clock_timestamp());
	SET search_path TO DEFAULT;
	RETURN true;

	EXCEPTION WHEN others THEN BEGIN
		GET STACKED DIAGNOSTICS
			v_state   = RETURNED_SQLSTATE,
			v_msg	 = MESSAGE_TEXT,
			v_detail  = PG_EXCEPTION_DETAIL,
			v_hint	= PG_EXCEPTION_HINT,
			v_context = PG_EXCEPTION_CONTEXT;
		raise notice E'Got exception:
			state  : %
			message: %
			detail : %
			hint   : %
			context: %', v_state, v_msg, v_detail, v_hint, v_context;

		 SET search_path TO DEFAULT;
		 RETURN false;
	END;

end$$;
-- ------------------------------------------------------------------------------
