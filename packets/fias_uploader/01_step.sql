---------------------------------------------------------------
-- dicts
---------------------------------------------------------------
drop table if exists public.fias_dict_actstat;
drop table if exists public.fias_dict_centerst;
drop table if exists public.fias_dict_roomtype;
drop table if exists public.fias_dict_strstat;
drop table if exists public.fias_dict_operstat;
drop table if exists public.fias_dict_eststat;
drop table if exists public.fias_dict_ndoctype;
drop table if exists public.fias_dict_flattype;
drop table if exists public.fias_dict_currentstid;
drop table if exists public.fias_dict_socrbase;

CREATE TABLE public.fias_dict_actstat
(
	actstatid smallint,
	name character varying(50)
);

CREATE TABLE public.fias_dict_centerst
(
	centerstid smallint,
	name character varying(50)
);

CREATE TABLE public.fias_dict_roomtype
(
	rmtypeid smallint,
	name character varying(50),
	shortname character varying(50)
);

CREATE TABLE public.fias_dict_strstat
(
	strstatid smallint,
	name character varying(50),
	shortname character varying(50)
);

CREATE TABLE public.fias_dict_operstat
(
	operstatid smallint,
	name character varying(50)
);

CREATE TABLE public.fias_dict_eststat
(
	eststatid smallint,
	name character varying(50),
	shortname character varying(50)
);

CREATE TABLE public.fias_dict_ndoctype
(
	ndtypeid smallint,
	name character varying(50)
);

CREATE TABLE public.fias_dict_flattype
(
	fltypeid smallint,
	name character varying(50),
	shortname character varying(50)
);

CREATE TABLE public.fias_dict_currentstid  -- ?
(
	curentstid smallint,
	name character varying(50)
);

CREATE TABLE public.fias_dict_socrbase  -- ?
(
	level smallint,
	socrname character varying(50),
	scname character varying(10),
	kod_t_st character varying(4)
);
---------------------------------------------------------------
-- main tables
---------------------------------------------------------------
DROP TABLE IF EXISTS public.fias_addr;
DROP TABLE IF EXISTS public.fias_house;
DROP TABLE IF EXISTS public.fias_nordoc;
DROP TABLE IF EXISTS public.fias_room;
DROP TABLE IF EXISTS public.fias_stead;


CREATE TABLE public.fias_addr
(
    id bigserial,
	actstatus boolean,
	aoguid UUID,
	aoid UUID,
	aolevel integer,
	areacode character varying(3),
	autocode character varying(1),
	centstatus smallint,
	citycode character varying(3),
	code character varying(17),
	currstatus smallint,
	enddate date,
	formalname character varying(120),
	ifnsfl character varying(4),
	ifnsul character varying(4),
	nextid UUID,
	offname character varying(4),
	okato character varying(11),
	oktmo character varying(11),
	operstatus smallint,
	parentguid UUID,
	placecode character varying(4),
	plaincode character varying(15),
	postalcode character varying(6),
	previd UUID,
	regioncode character varying(2),
	shortname character varying(10),
	startdate date,
	streencode character varying(4),
	terrifnsfl character varying(4),
	terrifnsul character varying(4),
	updatedate date,
	ctarcode character varying(3),
	extrcode character varying(4),
	livestatus smallint,
	normdoc UUID,
	plancode character varying(4),
	cadnum character varying(100),
	divtype smallint
);

CREATE TABLE public.fias_house
(
    id bigserial,
	aoguid UUID,
	buildnum character varying(50),
	enddate date,
	endstatus smallint,
	houseguid UUID,
	houseid UUID,
	housenum character varying(20),
	statstatus integer,
	ifnsfl character varying(4),
	ifnsul character varying(4),
	okato character varying(11),
	oktmo character varying(11),
	postalcode character varying(6),
	startdate date,
	strucnum character varying(50),
	strstatus smallint,
	terrifnsfl character varying(4),
	terrifnsul character varying(4),
	updatedate date,
	normdoc UUID,
	counter integer,
	cadnum character varying(100),
	divtype smallint
);

CREATE TABLE public.fias_nordoc
(
    id bigserial,
	normdocid UUID,
	docname character varying(250),
	docdate date,
	docnum character varying(20),
	doctype integer,
	docimgid character varying(36)
);


CREATE TABLE public.fias_room
(
    id bigserial,
	roomid UUID,
	roomguid UUID,
	houseguid UUID,
	regioncode character varying(2),
	flatnubmer character varying(50),
	flattype smallint,
	roonumber character varying(50),
	roomtype character varying(2),
	cadnum character varying(100),
	rommcadnum character varying(100),
	postalcode character varying(6),
	updatedate date,
	previd UUID,
	nextid UUID,
	operstatus smallint,
	startdate date,
	enddate date,
	livestatus smallint,
	normdoc UUID
);

CREATE TABLE public.fias_stead
(
	id bigserial,
	steadguid UUID,
	steadnumber character varying(120),
	regioncode character varying(2),
	postalcode character varying(6),
	ifnsfl character varying(4),
	terrifnsfl character varying(4),
	ifnsul character varying(4),
	terrifnsul character varying(4),
	okato character varying(11),
	updatedate date,
	parentguid UUID,
	steadid UUID,
	previd UUID,
	operstatus smallint,
	startdate date,
	enddate date,
	nextid character varying(36),
	oktmo character varying(11),
	livestatus smallint,
	cadnum character varying(100),
	divtype smallint,
	counter integer,
	normdoc UUID
);