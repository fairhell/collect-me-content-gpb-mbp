--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS smft.t_mbp_gpbsmflstepm1;


--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS smft.t_mbp_gpbsmflstepm1 (
    skey BIGSERIAL PRIMARY KEY,       -- ключ
    date_collected TIMESTAMP NOT NULL,     -- метка времени

    incident_id TEXT NULL,
    incident_step INT4 NULL,
    create_time TIMESTAMP NULL,
    work_start_time TIMESTAMP NULL,
    assignment TEXT NULL);
