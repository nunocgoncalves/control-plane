-- HOR-327: api_keys_changed NOTIFY trigger on identity.api_keys, mirroring
-- HOR-243's permissions_changed (000003). Lets the gateway (HOR-247) LISTEN for
-- API-key issue/revoke and refresh its auth snapshot promptly (no polling). The
-- payload names the table + key_hash so a consumer may refine its refresh;
-- control-plane's Go code is unaware of this channel (mirrors permissions).

CREATE OR REPLACE FUNCTION identity.notify_api_keys_change() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM pg_notify('api_keys_changed', json_build_object('table', TG_TABLE_NAME, 'key', OLD.key_hash)::text);
    ELSE
        PERFORM pg_notify('api_keys_changed', json_build_object('table', TG_TABLE_NAME, 'key', NEW.key_hash)::text);
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER api_keys_notify AFTER INSERT OR UPDATE OR DELETE ON identity.api_keys
    FOR EACH ROW EXECUTE FUNCTION identity.notify_api_keys_change();
