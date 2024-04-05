--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS t_sys_all_events;


--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS t_sys_all_events_raw (
    dt_sys_collected TIMESTAMP WITHOUT TIME ZONE  NOT NULL,     -- метка времени
    id_sys SERIAL PRIMARY KEY,       -- ключ

    id VARCHAR(100) NOT NULL DEFAULT '',
    client_trx_id VARCHAR(255) NOT NULL DEFAULT '',
    dt_event_time TIMESTAMP WITHOUT TIME ZONE  NOT NULL);
