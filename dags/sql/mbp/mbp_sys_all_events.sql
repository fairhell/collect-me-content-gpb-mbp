--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS smft.t_mbp_sys_all_events;
--DROP TABLE IF EXISTS smft.t_mbp_smft_log;

--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS smft.t_mbp_sys_all_events (
    skey BIGSERIAL PRIMARY KEY,       -- ключ
    date_collected TIMESTAMP DEFAULT timezone('UTC'::TEXT, clock_timestamp()) NOT NULL,     -- метка времени

    id TEXT NULL,
    client_trx_id TEXT NULL,
    event_time TIMESTAMP NULL,
    policy_rule_name TEXT NULL);


CREATE TABLE IF NOT EXISTS smft.t_mbp_smft_log (
    skey BIGSERIAL NOT NULL,       -- ключ
    date_collected TIMESTAMP DEFAULT timezone('UTC'::TEXT, clock_timestamp()) NOT NULL,     -- метка времени
    is_success BOOLEAN NOT NULL,
    table_name TEXT NULL,
    extra TEXT NULL,
    CONSTRAINT t_mbp_smft_log_pkey PRIMARY KEY(skey));
