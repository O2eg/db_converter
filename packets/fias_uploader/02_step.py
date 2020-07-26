from dbfread import DBF

data_dir = os.path.join(
	self.sys_conf.current_dir,
	"packets",
	self.args.packet_name,
	"data"
)

fias_dicts = {
	'fias_dict_centerst': [],
	'fias_dict_currentstid': [],
	'fias_dict_eststat': [],
	'fias_dict_flattype': [],
	'fias_dict_ndoctype': [],
	'fias_dict_operstat': [],
	'fias_dict_roomtype': [],
	'fias_dict_socrbase': [],
	'fias_dict_strstat': []
}

fias_dict_files = {
	'fias_dict_centerst': 'CENTERST.DBF',
	'fias_dict_currentstid': 'CURENTST.DBF',
	'fias_dict_eststat': 'ESTSTAT.DBF',
	'fias_dict_flattype': 'FLATTYPE.DBF',
	'fias_dict_ndoctype': 'NDOCTYPE.DBF',
	'fias_dict_operstat': 'OPERSTAT.DBF',
	'fias_dict_roomtype': 'ROOMTYPE.DBF',
	'fias_dict_socrbase': 'SOCRBASE.DBF',
	'fias_dict_strstat': 'STRSTAT.DBF'
}

for d_name, f_name in fias_dict_files.items():
	for rec in DBF(os.path.join(data_dir, f_name), lowernames=True):
		fias_dicts[d_name] = (list(rec.keys()))
		break


def wrapper(func, args):  # without star
	func(*args)


for d_name, _keys in fias_dicts.items():
	self.logger.log("Loading ============> " + d_name, "Info", do_print=True)
	p_insert = db_local.prepare(
		"INSERT INTO public.%s(%s) VALUES (%s)" % (
			d_name,
			','.join(_keys),
			','.join('$' + str(n) for n, i in enumerate(_keys, start=1))
		)
	)
	for rec in DBF(os.path.join(data_dir, fias_dict_files[d_name]), lowernames=True):
		params = []
		for pp in fias_dicts[d_name]:
			params.append(rec[pp])
		wrapper(p_insert, params)


for file in os.listdir(data_dir):
	if (
			file.startswith('ADDR') or file.startswith('HOUSE') or file.startswith('NORDOC') or \
			file.startswith('ROOM') or file.startswith('ROOM')
	) and file.endswith('.DBF'):
		print(file)
