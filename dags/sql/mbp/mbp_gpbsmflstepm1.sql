--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS t_gpbsmflstepm1_raw;


--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS t_gpbsmflstepm1_raw (
    dt_sys_collected TIMESTAMP WITHOUT TIME ZONE  NOT NULL,     -- метка времени
    id_sys SERIAL PRIMARY KEY,       -- ключ

    incident_id VARCHAR(60) NOT NULL DEFAULT '',
    dt_create_date TIMESTAMP WITHOUT TIME ZONE  NOT NULL,
    dt_work_start_time TIMESTAMP WITHOUT TIME ZONE  NOT NULL,
    assignment VARCHAR(255) NOT NULL DEFAULT '');
