CREATE OR REPLACE FUNCTION change_schema_objects_owner(schema_name TEXT, new_owner TEXT)
RETURNS void AS $$
DECLARE
    object_name TEXT;
    object_type TEXT;
BEGIN
    FOR object_name, object_type IN
        SELECT table_name, 'TABLE' FROM information_schema.tables WHERE table_schema = schema_name
        UNION ALL
        SELECT view_name, 'VIEW' FROM information_schema.views WHERE table_schema = schema_name
        -- Add other object types as needed (e.g., functions, sequences)
    LOOP
        BEGIN
            EXECUTE format('ALTER %s %I.%I OWNER TO %I',
                           object_type, schema_name, object_name, new_owner);
            RAISE NOTICE 'Changed owner of %s %I.%I to %I', object_type, schema_name, object_name, new_owner;
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE NOTICE 'Insufficient privileges to change owner of %s %I.%I', object_type, schema_name, object_name;
            -- Log the error or take other appropriate action (e.g., skip, retry)
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
