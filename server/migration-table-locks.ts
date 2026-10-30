/** Prefix one existing DDL query, so PostgreSQL holds these locks in that same
 * implicit/explicit transaction. Never wait for a second relation while owning
 * the first: an active reader yields 55P03 and rolls back this whole DDL unit.
 * Callers declare every relation they will lock or read while installing. */
export function migrationTableLocks(tables:readonly string[]){
 if(!tables.length||new Set(tables).size!==tables.length||tables.some(name=>!/^[a-z][a-z0-9_]*$/.test(name)))
  throw Object.assign(Error('MIGRATION_LOCK_SET'),{code:'MIGRATION_LOCK_SET'});
 const names=[...tables].sort().map(name=>`'${name}'`).join(',');
 return `DO $agentos_migration_locks$ DECLARE relation_name text; BEGIN
  FOREACH relation_name IN ARRAY ARRAY[${names}] LOOP
   IF to_regclass(format('public.%I',relation_name)) IS NOT NULL THEN
    EXECUTE format('LOCK TABLE public.%I IN ACCESS EXCLUSIVE MODE NOWAIT',relation_name);
   END IF;
  END LOOP;
 END $agentos_migration_locks$;\n`;
}
