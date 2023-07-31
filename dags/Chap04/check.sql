SELECT x.pagename, x.hr AS "hour", x.average AS "average pageviews"
FROM (
    SELECT
        pagename,
        date_part('hour', execution_date) AS hr,
        AVG(pageviewcount) AS average,
        ROW_NUMBER() OVER (PARTITION BY pagename ORDER BY AVG(pageviewcount) DESC) AS row_number
    FROM boaz
    GROUP BY pagename, date_part('hour', execution_date)
) AS x
WHERE x.row_number = 1;