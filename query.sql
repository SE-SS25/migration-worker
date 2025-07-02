-- name: GetMigrationJob :one
SELECT * FROM db_migration
WHERE m_worker_id = $1;