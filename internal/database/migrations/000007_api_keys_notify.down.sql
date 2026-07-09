-- HOR-327: reverse api_keys_changed NOTIFY trigger.

DROP TRIGGER IF EXISTS api_keys_notify ON identity.api_keys;
DROP FUNCTION IF EXISTS identity.notify_api_keys_change();
