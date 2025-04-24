CREATE FUNCTION change_schema_objects_owner(schema_name TEXT, new_owner TEXT)
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
        EXECUTE format('ALTER %s %I.%I OWNER TO %I',
                       object_type, schema_name, object_name, new_owner);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
SELECT change_schema_objects_owner('my_schema', 'new_user');
