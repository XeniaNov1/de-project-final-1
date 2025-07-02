create table STV202506270__STAGING.transactions
(
	operation_id varchar(60),
	account_number_from int,
	account_number_to int,
	currency_code int,
	country varchar(30),
	status varchar(30),
	transaction_type varchar(30),
	amount int,
	transaction_dt timestamp
)
order by operation_id,transaction_dt
 	SEGMENTED BY HASH(operation_id,transaction_dt) ALL NODES
 	PARTITION BY transaction_dt::date;

drop table if exists STV202506270__STAGING.currencies;
create table STV202506270__STAGING.currencies
(
	currency_code int PRIMARY KEY,
	currency_code_with int,
	currency_code_div numeric(5,3),
	date_update timestamp
)
order by currency_code, date_update
 	SEGMENTED BY HASH(currency_code,date_update) ALL NODES
 	PARTITION BY date_update::date;
 	

 create table STV202506270__DWH.global_metrics
 (
 	date_update datetime PRIMARY KEY,
 	currency_from int,
 	amount_total numeric,
 	cnt_transactions int,
 	avg_transactions_per_account numeric,
 	cnt_accounts_make_transactions numeric
 )
 order by currency_from, date_update
 	SEGMENTED BY HASH(currency_from,date_update) ALL NODES
 	PARTITION BY date_update::date;


CREATE TABLE STV202506270__DWH.latest_load_dates (
    id INT IDENTITY PRIMARY KEY,
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);