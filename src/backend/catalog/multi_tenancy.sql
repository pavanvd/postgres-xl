/*
 * Multi-tenancy security extension
 */

/*
 * Access to pg_database is disabled to public, provide this view instead.
 */
CREATE VIEW pg_database_list AS
	SELECT datname,
		   datdba,
		   encoding,
		   datcollate,
		   datctype,
		   datistemplate,
		   datallowconn,
		   datconnlimit,
		   datlastsysoid,
		   datfrozenxid,
		   dattablespace,
		   datacl
	FROM pg_database
	WHERE datallowconn
		  AND has_database_privilege(datname, 'CONNECT');

GRANT SELECT ON pg_database_list TO PUBLIC;


/*
 * Following views are already defined, we redefine them so non-privileged user
 * could see only users it is allowed to see.
 */
CREATE OR REPLACE VIEW pg_roles AS
    SELECT
        rolname,
        rolsuper,
        rolinherit,
        rolcreaterole,
        rolcreatedb,
        rolcatupdate,
        rolcanlogin,
        rolreplication,
        rolconnlimit,
        '********'::text as rolpassword,
        rolvaliduntil,
        setconfig as rolconfig,
        a1.oid
    FROM pg_authid a1 LEFT JOIN pg_db_role_setting s
    ON (a1.oid = setrole AND setdatabase = 0)
	WHERE EXISTS (SELECT 1 FROM pg_authid a2 WHERE pg_has_role(a2.rolname, 'MEMBER') AND pg_has_role(a1.rolname, a2.rolname, 'MEMBER'));

CREATE OR REPLACE VIEW pg_shadow AS
    SELECT
        rolname AS usename,
        a1.oid AS usesysid,
        rolcreatedb AS usecreatedb,
        rolsuper AS usesuper,
        rolcatupdate AS usecatupd,
        rolreplication AS userepl,
        rolpassword AS passwd,
        rolvaliduntil::abstime AS valuntil,
        setconfig AS useconfig
    FROM pg_authid a1 LEFT JOIN pg_db_role_setting s
    ON (a1.oid = setrole AND setdatabase = 0)
    WHERE rolcanlogin
		  AND EXISTS (SELECT 1 FROM pg_authid a2 WHERE pg_has_role(a2.rolname, 'MEMBER') AND pg_has_role(a1.rolname, a2.rolname, 'MEMBER'));

CREATE OR REPLACE VIEW pg_group AS
    SELECT
        rolname AS groname,
        oid AS grosysid,
        ARRAY(SELECT member FROM pg_auth_members WHERE roleid = oid) AS grolist
    FROM pg_authid a1
    WHERE NOT rolcanlogin
		  AND EXISTS (SELECT 1 FROM pg_authid a2 WHERE pg_has_role(a2.rolname, 'MEMBER') AND pg_has_role(a1.rolname, a2.rolname, 'MEMBER'));

CREATE OR REPLACE VIEW pg_stat_database AS
    SELECT
            D.oid AS datid,
            D.datname AS datname,
            pg_stat_get_db_numbackends(D.oid) AS numbackends,
            pg_stat_get_db_xact_commit(D.oid) AS xact_commit,
            pg_stat_get_db_xact_rollback(D.oid) AS xact_rollback,
            pg_stat_get_db_blocks_fetched(D.oid) -
                    pg_stat_get_db_blocks_hit(D.oid) AS blks_read,
            pg_stat_get_db_blocks_hit(D.oid) AS blks_hit,
            pg_stat_get_db_tuples_returned(D.oid) AS tup_returned,
            pg_stat_get_db_tuples_fetched(D.oid) AS tup_fetched,
            pg_stat_get_db_tuples_inserted(D.oid) AS tup_inserted,
            pg_stat_get_db_tuples_updated(D.oid) AS tup_updated,
            pg_stat_get_db_tuples_deleted(D.oid) AS tup_deleted,
            pg_stat_get_db_conflict_all(D.oid) AS conflicts,
            pg_stat_get_db_stat_reset_time(D.oid) AS stats_reset
    FROM pg_database D
	WHERE datallowconn
		  AND has_database_privilege(D.datname, 'CONNECT');

CREATE OR REPLACE VIEW pg_stat_database_conflicts AS
    SELECT
            D.oid AS datid,
            D.datname AS datname,
            pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
            pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
            pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
            pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
            pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
    FROM pg_database D
	WHERE datallowconn
		  AND has_database_privilege(D.datname, 'CONNECT');

