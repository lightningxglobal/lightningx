-- HMAC signing secrets for API keys.
--
-- secret IS NULL  → legacy key: bare-key session auth still accepted
--                   (deprecation window).
-- secret NOT NULL → signed auth REQUIRED: the client must present
--                   timestamp + HMAC-SHA256(secret, timestamp); bare-key
--                   auth for such keys is rejected. ±30s timestamp window
--                   bounds replay.

ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS secret TEXT;
