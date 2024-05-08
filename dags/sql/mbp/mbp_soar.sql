--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS soar.t_mbp_soar;
--DROP TABLE IF EXISTS soar.t_mbp_soar_log;

--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS soar.t_mbp_soar (
    skey BIGSERIAL PRIMARY KEY,       -- ключ
    date_collected TIMESTAMP NOT NULL,     -- метка времени

    identifier TEXT NULL,
    creation_date TIMESTAMP NULL,
    description TEXT NULL,
    inc_owner_name TEXT NULL,
    inc_owner_uuid TEXT NULL,
    inc_owner_id INT4 NULL,
    irp_dst_ip TEXT NULL,
    irp_src_ip TEXT NULL,
    level_id INT4 NULL,
    level_name TEXT NULL,
    status_id INT4 NULL,
    status_name TEXT NULL,
    updated_date TIMESTAMP NULL,
    closure_date TIMESTAMP NULL);


CREATE TABLE IF NOT EXISTS soar.t_mbp_soar_log (
    skey BIGSERIAL NOT NULL,       -- ключ
    date_collected TIMESTAMP DEFAULT timezone('UTC'::TEXT, clock_timestamp()) NOT NULL,     -- метка времени
    is_success BOOLEAN NOT NULL,
    groupname TEXT NULL,
    task TEXT NULL,
    extra TEXT NULL,
    CONSTRAINT t_mbp_soar_log_pkey PRIMARY KEY(skey));
