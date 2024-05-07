--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS smft.t_mbp_gpbsmflstepm1;
--DROP TABLE IF EXISTS smft.t_mbp_smft_log;

--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS smft.t_mbp_gpbsmflstepm1 (
    skey BIGSERIAL PRIMARY KEY,       -- ключ
    date_collected TIMESTAMP NOT NULL,     -- метка времени

    incident_id TEXT NULL,
    create_time TIMESTAMP NULL,
    work_start_time TIMESTAMP NULL,
    assignment TEXT NULL);


CREATE TABLE IF NOT EXISTS smft.t_mbp_smft_log (
    skey BIGSERIAL NOT NULL,       -- ключ
    date_collected TIMESTAMP DEFAULT timezone('UTC'::TEXT, clock_timestamp()) NOT NULL,     -- метка времени
    is_success BOOLEAN NOT NULL,
    table_name TEXT NULL,
    extra TEXT NULL,
    CONSTRAINT t_mbp_smft_log_pkey PRIMARY KEY(skey));
