--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS nagios.t_mbp_nagios;


--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS nagios.t_mbp_nagios (
    skey BIGSERIAL PRIMARY KEY,       -- ключ
    date_collected TIMESTAMP NOT NULL,     -- метка времени

    raw_output TEXT NULL,
    host_name TEXT NULL,
    host_address TEXT NULL,
    display_name TEXT NULL,
    metric_name TEXT NULL,
    metric_val NUMERIC NULL,
    check_time TIMESTAMP NULL);
