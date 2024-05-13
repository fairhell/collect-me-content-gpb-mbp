--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS smft.t_mbp_afd_monitoring;
--DROP TABLE IF EXISTS smft.t_mbp_smft_log;

--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS smft.t_mbp_afd_monitoring (
    skey BIGSERIAL PRIMARY KEY,       -- ключ
    date_collected TIMESTAMP DEFAULT timezone('UTC'::TEXT, clock_timestamp()) NOT NULL,     -- метка времени

    id TEXT NULL,
    creation_date TIMESTAMP NULL,
    resp_prcnt TEXT NULL,
    resp_max NUMERIC(10, 0) NULL,
    hostname TEXT NULL,
    servername TEXT NULL,
    tpm_in INT4 NULL,
    tpm_out INT4 NULL);


CREATE TABLE IF NOT EXISTS smft.t_mbp_smft_log (
    skey BIGSERIAL NOT NULL,       -- ключ
    date_collected TIMESTAMP DEFAULT timezone('UTC'::TEXT, clock_timestamp()) NOT NULL,     -- метка времени
    is_success BOOLEAN NOT NULL,
    table_name TEXT NULL,
    extra TEXT NULL,
    CONSTRAINT t_mbp_smft_log_pkey PRIMARY KEY(skey));
