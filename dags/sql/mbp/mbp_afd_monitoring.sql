--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS t_afd_monitoring_raw;


--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS t_afd_monitoring_raw (
    dt_sys_collected TIMESTAMP WITHOUT TIME ZONE  NOT NULL,     -- метка времени
    id_sys SERIAL PRIMARY KEY,       -- ключ

    id VARCHAR(32) NOT NULL DEFAULT '',
    dt_creation_date TIMESTAMP WITHOUT TIME ZONE  NOT NULL,
    resp_prcnt VARCHAR(250) NOT NULL DEFAULT '',
    resp_max NUMERIC(10, 0) NOT NULL DEFAULT 0);
