CREATE TABLE IF NOT EXISTS orders
(
    id
    VARCHAR
(
    36
) PRIMARY KEY,
    customer VARCHAR
(
    100
) NOT NULL,
    amount INT NOT NULL,
    created_at TIMESTAMP NOT NULL
    );

CREATE TABLE IF NOT EXISTS outbox
(
    id
    BIGINT
    AUTO_INCREMENT
    PRIMARY
    KEY,
    event_id
    VARCHAR
(
    36
) NOT NULL UNIQUE,
    aggregate_id VARCHAR
(
    36
) NOT NULL,
    event_type VARCHAR
(
    100
) NOT NULL,
    payload VARCHAR
(
    4000
) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    sent_at TIMESTAMP
    );

CREATE TABLE IF NOT EXISTS processed_events
(
    event_id
    VARCHAR
(
    36
) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL
    );

CREATE TABLE IF NOT EXISTS shipments
(
    id
    BIGINT
    AUTO_INCREMENT
    PRIMARY
    KEY,
    order_id
    VARCHAR
(
    36
) NOT NULL,
    amount INT NOT NULL
    );