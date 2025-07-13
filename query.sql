-- name: GetMigrationJob :one
SELECT * FROM db_migration
WHERE m_worker_id = $1
LIMIT 1;

-- name: GetDBMappings :many
SELECT * FROM db_mapping
ORDER BY "from" ASC;

-- name: CheckInTable :many
SELECT *
FROM migration_worker
WHERE id = $1;


-- name: Heartbeat :execresult
UPDATE migration_worker
SET last_heartbeat = $2, uptime = uptime + $3
WHERE id = $1;

-- name: ChangeJobStatus :execresult
UPDATE db_migration
SET status = $2
WHERE m_worker_id=$1;

-- name: DeleteJob :execresult
DELETE FROM db_migration
WHERE m_worker_id = $1;

-- name: DeleteWorkerJobJoin :execresult
DELETE
FROM migration_worker_jobs
WHERE migration_id = $1
   OR worker_id = $2;

-- name: AddDbMapping :execresult
INSERT INTO db_mapping (url, "from", size)
VALUES ($1, $2, 0);

-- name: DeleteDbMapping :execresult
DELETE FROM db_mapping
WHERE id = $1;

-- name: RemoveSelf :execresult
DELETE FROM migration_worker
WHERE id=$1;

