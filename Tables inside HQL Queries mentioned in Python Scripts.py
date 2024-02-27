import os
import re
import pandas as pd

working_directory="C:/Users/NiladriDas/Git"
os.chdir(working_directory)

dags_directory="dags/"

# DAGs
dags=set()

with open("dags.txt", 'r') as dag_path:
    for i in dag_path.readlines():
        dag=i.replace('\\', '/').replace('\n', '')
        dags.add(dag)

# HQLs
hqls=set()

for file_name in dags:
    with open(dags_directory+file_name) as file:
        s="\n".join(file.readlines())[::-1]
        ns=""
        i=0
        flag=0
        while i<len(s)-3:
            if flag==0:
                if (s[i]+s[i+1]+s[i+2]+s[i+3])=="lqh." or (s[i]+s[i+1]+s[i+2]+s[i+3])=="lqs.":
                    flag=1
                    continue
            elif flag==1:
                ns+=s[i]
                if s[i]=='"' or s[i]=="'":
                    if "seitreporp." not in ns and "lqs.kraps" not in ns:
                        hqls.add((file_name, ns[::-1].strip('"').strip("'")))
                    flag=0
                    ns=""
            i+=1

hqls_directory="queries"

all_files=[]
for path, _, files in os.walk(hqls_directory):
    for file in files:
        all_files.append(os.path.join(path, file).replace('\\', '/'))

# TABLEs
tables=[]

table_pattern=r'(FROM|JOIN|INSERT INTO TABLE|INSERT INTO|INSERT OVERWRITE TABLE|INSERT OVERWRITE|CREATE TABLE IF NOT EXISTS|CREATE TABLE|ALTER TABLE|ALTER|DROP TABLE IF EXISTS|DROP TABLE)\s+([\S\.]+)'
cte_pattern1=r"(WITH|\,)\s+([\w_]+)\s+AS"
cte_pattern2=r"(\,)([\w_]+)\s+AS"

for hql in hqls:
    hql_path=[i for i in filter(lambda path: hql[1] in path, all_files)]
    if len(hql_path)==0:
        continue
    python_file=hql[0]
    hql_file=hql[1]
    try:
        with open(hql_path[0], "r", encoding="utf-8") as file:
            query="\n".join(file.readlines())
            query=re.sub(r'\s+', ' ', query)
            cte_names1=set([k[1] for k in set(re.findall(cte_pattern1, query.strip('\n').upper()))])
            cte_names2=set([k[1] for k in set(re.findall(cte_pattern2, query.strip('\n').upper()))])
            cte_names=cte_names1.union(cte_names2)
            table_names=set(re.findall(table_pattern, query.strip('\n').upper(), re.IGNORECASE))
            for i in table_names:
                if i[1] not in cte_names:
                    tables.append((python_file, hql_file, i[1], i[0]))
    except Exception as e:
        print(f"NOT ABLE TO FOUND THE HQL - {hql}")
        tables.append((python_file, hql_file, "NOT FOUND", "NOT FOUND"))

print("WRITING PROCESS IS ON - ", end='')

try:
    os.remove("final.csv")
except:
    pass
pd.DataFrame(tables).to_csv("final.csv", mode='a+', index=None)

print("* - WRITING PROCESS IS DONE")