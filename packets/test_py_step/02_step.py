data_dir = os.path.join(
	self.sys_conf.current_dir,
	"packets",
	self.args.packet_name,
	"data"
)

p_insert = db_local.prepare("INSERT INTO public.test_tbl_import(dir, fname, content) VALUES ($1, $2, $3)")

with db_local.xact():
	for file in os.listdir(data_dir):
		current_file = open(os.path.join(data_dir, file), 'r', encoding="utf8")
		file_content = current_file.read()
		p_insert(data_dir, file, file_content)
		current_file.close()
