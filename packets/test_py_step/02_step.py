data_dir = os.path.join(
	self.sys_conf.current_dir,
	"packets",
	self.args.packet_name,
	"data"
)

for file in os.listdir(data_dir):
	current_file = open(os.path.join(data_dir, file), 'r', encoding="utf8")
	file_content = current_file.read()
	print(file_content)
	current_file.close()

self.execute_q(ctx, db_local, """INSERT INTO public.test_tbl_import(fld_1)
    SELECT 'text_' || T.v from(SELECT generate_series(1, 20) as v) T;""")
self.execute_q(ctx, db_local, 'select version()')