CREATE TABLE IF NOT EXISTS accounts (
    id          BIGSERIAL PRIMARY KEY,
    username    TEXT NOT NULL,
    email       TEXT NOT NULL,
    balance     NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    status      TEXT NOT NULL DEFAULT 'active',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO accounts (username, email, balance, status)
SELECT
    'user_' || i,
    'user_' || i || '@example.com',
    round((random() * 10000)::numeric, 2),
    CASE WHEN random() > 0.1 THEN 'active' ELSE 'inactive' END
FROM generate_series(1, 10000) AS s(i);
