--
-- Автор: Владимир
-- 2024
--

--DROP TABLE IF EXISTS smft.t_mbp_afd_monitoring;

--
-- Основная таблица
--
CREATE TABLE IF NOT EXISTS smft.t_mbp_afd_monitoring (
    skey BIGSERIAL PRIMARY KEY,       -- ключ
    date_collected TIMESTAMP NOT NULL,     -- метка времени

    id TEXT NULL,
    creation_date TIMESTAMP NULL,
    resp_prcnt TEXT NULL,
    resp_max NUMERIC(10, 0) NULL,
    hostname TEXT NULL,
    servername TEXT NULL,
    tpm_in INT4 NULL,
    tpm_out INT4 NULL);
