# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Selects the sets from src dataset and inserts into the reporting."""

from google.cloud import bigquery

client = bigquery.Client()

def generate_hier(**kwargs):

    src_dataset = kwargs["src_dataset"]
    mandt = kwargs["mandt"]
    setname = kwargs["setname"]
    setclass = kwargs["setclass"]
    orgunit = kwargs["orgunit"]
    table = kwargs["table"]
    select_key = kwargs["select_key"]
    where_clause = kwargs["where_clause"]
    full_table = kwargs["full_table"]

    # CORTEX-CUSTOMER - this means the sets file has the root for
    # this specific table
    # and the whole hierarchy will be flattened each time
    # Uncomment if full datasets are created or implement merge
    # trunc_sql = "TRUNCATE TABLE {ft}".format(ft=full_table)
    # errors = client.query(trunc_sql)
    # if errors == []:
    #     print("Truncated table {}".format(full_table))
    # else:
    #     print("Encountered errors while attempting to
    #       truncate: {}".format(errors))

    get_nodes(src_dataset, mandt, setname,
             setclass, orgunit, table,
             select_key, where_clause, full_table)

def insert_rows(full_table, nodes):
    errors = client.insert_rows_json(full_table, nodes)
    if not errors:
        print(f"New rows added to table {full_table}")
    else:
        print(f"Encountered errors while inserting rows: {errors}")

def get_nodes(src_dataset, mandt, setname, setclass, org_unit,
              table, select_key, where_clause, full_table):
    query = f"""SELECT DISTINCT setname, setclass, subclass,
             lineid, subsetcls, subsetscls, subsetname
             FROM  `{src_dataset}.setnode`
             WHERE setname = '{setname}'
              AND setclass = '{setclass}'
              AND subclass = '{org_unit}'
              AND mandt = '{mandt}' """

    query_job = client.query(query)
    query_res = query_job.result()
    for setr in query_res:
        get_leafs_children(src_dataset, mandt, setr, table, select_key,
                            where_clause, full_table)

    if not query_res:
        print(f"Dataset {setname}  not found in SETNODES")

def get_leafs_children(src_dataset, mandt, row, table, field, where_clause,
                       full_table):
    node_list = []
    node = {}
    # TODO: would be nice to implement multithreaded calls

    node = {
        "mandt": mandt,
        "parent": row["setname"],
        "parent_org": row["subclass"],
        "child": row["subsetname"],
        "child_org": row["subsetscls"]
    }
    node_list.append(node)
    insert_rows(full_table, node_list)

    # Get values from setleaf (only lower child sets have these)
    query = f"""SELECT DISTINCT valsign, valoption, valfrom, valto
            FROM `{src_dataset}.setleaf`
            WHERE setname = '{row["subsetname"]}'
              AND setclass = '{row["subsetcls"]}'
              AND subclass = '{row["subsetscls"]}'
              AND mandt = '{mandt}' """

    qry_job = client.query(query)
    leafs = qry_job.result()
    # Get values from actual master data tables (e.g.,
    # Costs center, GL Accounts, etc)

    for setl in leafs:
        # Field = the key (e.g., profit center: CEPC-PRCTC)
        # Where clause parses additional filters: MANDT, Controlling Area,
        # valid-to date
        if setl["valoption"] == "EQ":
            where_cls = f" {field}  = '{setl['valfrom']}' "
        elif setl["valoption"] == "BT":
            # pylint: disable-next=line-too-long
            where_cls=f" {field} between '{setl['valfrom']}' and '{setl['valto']}' "
        for clause in where_clause:
            where_cls = where_cls + f" AND {clause} "

        query = f""" SELECT DISTINCT `{field}`
                    FROM `{src_dataset}.{table}`
                    WHERE mandt  = '{mandt}'
                      AND {where_cls}"""

        ranges = []
        job = client.query(query)
        ranges = job.result()
        for line in ranges:
            node_list = []
            node = {
                "mandt": mandt,
                "parent": row["setname"],
                "parent_org": row["subclass"],
                "child": row["subsetname"],
                "child_org": row["subsetscls"],
                field: line[field]
            }

            node_list.append(node)
            insert_rows(full_table, node_list)
    # Recursive call for child dataset
    get_nodes(src_dataset, mandt, row["subsetname"], row["subsetcls"],
              row["subsetscls"], table, field, where_clause, full_table)
