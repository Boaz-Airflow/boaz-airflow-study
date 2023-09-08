CREATE TABLE IF NOT EXISTS jd_database.jd_date AS
SELECT
  CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) AS date_str,
  COUNT(DISTINCT job_id) AS job_count
FROM rallit
GROUP BY year, month, day;