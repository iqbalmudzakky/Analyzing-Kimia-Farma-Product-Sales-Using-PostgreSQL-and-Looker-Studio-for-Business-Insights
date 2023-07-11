--------------------------------Create Table--------------------------------

--table penjualan--
create table penjualan (
	id_distributor varchar,
	id_cabang varchar,
	id_invoice varchar,
	tanggal date,
	id_customer varchar,
	id_barang varchar,
	jumlah_barang integer,
	unit varchar,
	harga float,
	mata_uang varchar,
	brand_id varchar,
	lini varchar
);
copy penjualan
from 'D:\Data Science Boot Camp\Project\6. Data Source Salicyl - Kimia Farma\database\penjualan.csv'
delimiter ','
csv header;
--table penjualan_ds--
create table penjualan_ds(
	id_invoice varchar,
	tanggal date,
	id_customer varchar,
	id_barang varchar,
	jumlah_barang integer,
	unit varchar,
	harga integer,
	mata_uang varchar
);
copy penjualan_ds
from 'D:\Data Science Boot Camp\Project\6. Data Source Salicyl - Kimia Farma\database\penjualan_ds.csv'
delimiter ','
csv header;
--table pelanggan--
create table pelanggan(
	id_customer varchar,
	level varchar,
	nama varchar,
	id_cabang_sales varchar,
	cabang_sales varchar,
	id_group varchar,
	customer_group varchar
);
copy pelanggan
from 'D:\Data Science Boot Camp\Project\6. Data Source Salicyl - Kimia Farma\database\pelanggan.csv'
delimiter ','
csv header;
--table pelanggan_ds--
create table pelanggan_ds(
	id_customer varchar,
	level varchar,
	nama varchar,
	id_cabang_sales varchar,
	cabang_sales varchar,
	id_distributor varchar,
	customer_group varchar
);
copy pelanggan_ds
from 'D:\Data Science Boot Camp\Project\6. Data Source Salicyl - Kimia Farma\database\pelanggan_ds.csv'
delimiter ','
csv header;
--table barang--
create table barang(
	kode_barang varchar,
	sektor varchar,
	nama_barang varchar,
	tipe varchar,
	nama_tipe varchar,
	kode_lini integer,
	lini varchar,
	kemasan varchar
);
copy barang
from 'D:\Data Science Boot Camp\Project\6. Data Source Salicyl - Kimia Farma\database\barang.csv'
delimiter ','
csv header;
--table barang_ds--
create table barang_ds(
	kode_barang varchar,
	nama_barang varchar,
	kemasan varchar,
	harga integer,
	nama_tipe varchar,
	kode_brand integer,
	brand varchar
);
copy barang_ds
from 'D:\Data Science Boot Camp\Project\6. Data Source Salicyl - Kimia Farma\database\barang_ds.csv'
delimiter ','
csv header;

--------------------------------Join Table--------------------------------

create table data_penjualan as 
with sales as (
	select distinct
		penjualan.id_distributor,
		penjualan.id_cabang,
		penjualan.id_invoice,
		penjualan.tanggal,
		penjualan.id_customer,
		penjualan.id_barang,
		penjualan.jumlah_barang,
		penjualan_ds.unit,
		penjualan.mata_uang,
		penjualan.brand_id,
		penjualan.lini,
		penjualan_ds.harga
	from penjualan
	left join penjualan_ds 
		on penjualan.id_barang = penjualan_ds.id_barang
	where penjualan.id_customer is not null
),
customer as (
	select
		pelanggan.id_customer,
		pelanggan.level,
		pelanggan.nama,
		pelanggan.id_cabang_sales,
		pelanggan.cabang_sales,
		pelanggan.id_group,
		pelanggan.customer_group,
		pelanggan_ds.id_distributor
	from pelanggan
	left join pelanggan_ds
		on pelanggan.id_customer = pelanggan_ds.id_customer
),
product as(
	select
	barang.kode_barang,
	barang.sektor,
	barang.nama_barang,
	barang.tipe,
	barang.nama_tipe,
	barang.kemasan,
	barang_ds.harga,
	barang_ds.kode_brand,
	barang_ds.brand
	from barang
	left join barang_ds
		on barang.kode_barang = barang_ds.kode_barang
)
select 
	s.id_customer,
	s.tanggal,
	p.nama_barang,
	p.brand,
	s.jumlah_barang,
	p.kemasan,
	s.harga,
	s.jumlah_barang * s.harga as revenue,
	c.nama as nama_cabang,
	c.cabang_sales as kota,
	c.customer_group as kategori,
	to_char (tanggal::date, 'Month') as bulan
from sales as s
left join customer as c
	on s.id_customer = c.id_customer
left join product as p
	on s.id_barang = p.kode_barang
;

--------------------------------Table Aggregat--------------------------------

create table table_agg as 
select
	extract (month from tanggal) as bulan_ke,
	bulan,
	kota,
	nama_barang,
	brand,
	sum (jumlah_barang) as sold_product,
	sum (revenue) as total_revenue
from data_penjualan
group by 1,2,3,4,5
order by 1,2,3,4
;
