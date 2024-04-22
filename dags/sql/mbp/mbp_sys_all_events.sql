--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS smft.t_mbp_sys_all_events;


--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS smft.t_mbp_sys_all_events (
    skey BIGSERIAL PRIMARY KEY,       -- ключ
    date_collected TIMESTAMP NOT NULL,     -- метка времени

    id TEXT NULL,
    client_trx_id TEXT NULL,
    event_time TIMESTAMP NULL);
