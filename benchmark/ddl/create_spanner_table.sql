CREATE TABLE TransferTest (
    id              STRING(36) NOT NULL,
    json_payload    JSON,
    short_label     STRING(100),
    related_guid    STRING(36),
    status_flag     STRING(1),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
) PRIMARY KEY (id);