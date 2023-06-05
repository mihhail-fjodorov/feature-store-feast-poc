CREATE TABLE public.transaction_minutly_stats (
  id                   BIGSERIAL                    NOT NULL,
  customer_uid         UUID                         NOT NULL,
  event_time           TIMESTAMP WITHOUT TIME ZONE  NOT NULL,
  count_50_last_hour   INTEGER,
  count_200_last_hour  INTEGER,
  count_500_last_hour  INTEGER,
  sum_50_last_hour     NUMERIC(19, 2),
  sum_200_last_hour    NUMERIC(19, 2),
  sum_500_last_hour    NUMERIC(19, 2),
  created_at           TIMESTAMP WITHOUT TIME ZONE  NOT NULL,
  CONSTRAINT pk_transaction PRIMARY KEY (id)
);

CREATE UNIQUE INDEX idx_transaction_1 ON transaction_minutly_stats (customer_uid, event_time);
CREATE INDEX idx_transaction_2 ON transaction_minutly_stats (created_at);

