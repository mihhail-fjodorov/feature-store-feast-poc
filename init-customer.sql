CREATE TABLE transaction (
  id             BIGSERIAL                    NOT NULL,
  customer_uid   UUID                         NOT NULL,
  amount         NUMERIC(19, 2)               NOT NULL,
  direction      TEXT                         NOT NULL,
  created_at     TIMESTAMP WITHOUT TIME ZONE  NOT NULL,
  CONSTRAINT pk_transaction PRIMARY KEY (id)
);

CREATE INDEX idx_transaction ON transaction (customer_uid);

