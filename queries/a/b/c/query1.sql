WITH 
    cte1 AS (
        SELECT id 
        FROM databaseA.tableA 
        GROUP BY 1
    ),

    cte2 AS (
        SELECT *
        FROM databaseB.tableB
    ), 

    cte3 AS (
        SELECT id, date,
               SUM(value) AS sum,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY SUM(sum) DESC) AS rank
        FROM databaseC.tableC
        GROUP BY 1, 2, 3
    )

INSERT OVERWRITE TABLE databaseD.tableD
PARTITION (id, date) 
SELECT 
*
FROM 
    cte1
INNER JOIN 
    cte2 ON cte1.id = cte2.id
INNER JOIN 
    cte3 ON cte1.id = cte3.id
INNER JOIN 
    databaseE.tableE table ON table.id = cte1.id
WHERE 
    cte3.rank = 1
    AND (table.id IS NULL AND cte1.id IS NULL)
GROUP BY 
    1, 2, 3;
