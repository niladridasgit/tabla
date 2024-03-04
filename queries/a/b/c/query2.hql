DROP TABLE database1.table1;

CREATE TABLE database1.table1
STORED AS ORC
AS
SELECT 
*
FROM 
    database2.table2 a
LEFT JOIN 
    database3.table3 b ON b.id = a.id
WHERE 
    date='${current_date}'
GROUP BY 
    1,2,3;
