CREATE TABLE IF NOT EXISTS "jd_database"."jd_count"  AS 
SELECT "category", COUNT(DISTINCT "title") AS ct
FROM "jd_database"."rallit"
GROUP BY 1
ORDER BY 2 DESC
;