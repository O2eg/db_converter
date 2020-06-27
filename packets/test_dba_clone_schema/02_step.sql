select public.clone_schema(
	'template',
	'schema_1',
	true -- p_dry_run
);

select public.clone_schema(
	'template',
	'schema_1',
	false -- p_dry_run
);