--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS nagios.t_mbp_nagios;
--DROP TABLE IF EXISTS nagios.t_mbp_nagios_log;

--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS nagios.t_mbp_nagios (
    skey BIGSERIAL PRIMARY KEY,       -- ключ
    date_collected TIMESTAMP DEFAULT timezone('UTC'::TEXT, clock_timestamp()) NOT NULL,     -- метка времени

    output TEXT NULL,
    host_name TEXT NULL,
    host_address TEXT NULL,
    display_name TEXT NULL,
    metric_name TEXT NULL,
    metric_val NUMERIC NULL,
    check_time TIMESTAMP NULL);


CREATE TABLE IF NOT EXISTS nagios.t_mbp_nagios_log (
    skey BIGSERIAL NOT NULL,       -- ключ
    date_collected TIMESTAMP DEFAULT timezone('UTC'::TEXT, clock_timestamp()) NOT NULL,     -- метка времени
    is_success BOOLEAN NOT NULL,
    host_name TEXT NULL,
    metric_name TEXT NULL,
    extra TEXT NULL,
    CONSTRAINT t_mbp_nagios_log_pkey PRIMARY KEY(skey));
