--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS smft.t_mbp_gpbsmflsmsm1;


--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS smft.t_mbp_gpbsmflsmsm1 (
    skey BIGSERIAL PRIMARY KEY,       -- ключ
    date_collected TIMESTAMP NOT NULL,     -- метка времени

    request_id TEXT NULL,
    processing_state_time TIMESTAMP NULL,
    validate_state_time TIMESTAMP NULL);
