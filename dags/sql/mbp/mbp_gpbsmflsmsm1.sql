--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS t_gpbsmflsmsm1_raw;


--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS t_gpbsmflsmsm1_raw (
    dt_sys_collected TIMESTAMP WITHOUT TIME ZONE  NOT NULL,     -- метка времени
    id_sys SERIAL PRIMARY KEY,       -- ключ

    request_id VARCHAR(60) NOT NULL DEFAULT '',
    dt_processing_state_time TIMESTAMP WITHOUT TIME ZONE  NOT NULL,
    dt_validate_state_time TIMESTAMP WITHOUT TIME ZONE  NOT NULL);
