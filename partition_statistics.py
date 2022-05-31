from odbc_utils import *

def table_card(connection, tablename):
    q = f"Select count(*) from {tablename}"
    cur = execute(connection, q)
    row = cur.fetchone()
    total = row[0]
    cur.close()
    return total

def num_partitions(connection, tablename):
    q = f"Select count(distinct tid/10000000000000000) from {tablename}"
    cur = execute(connection, q)
    row = cur.fetchone()
    total = row[0]
    cur.close()
    return total

def partition_balance(connection, tablename):
    num_p = num_partitions(connection, tablename)
    total_card = table_card(connection, tablename)
    balanced = total_card / num_p

    q = "Select min(card), max(card) from ("\
        f"Select tid/10000000000000000 as part, count(*) as card from {tablename} "\
        f"group by 1) as t"
    cur = execute(connection, q)
    row = cur.fetchone()
    min = row[0]
    max = row[1]
    cur.close()
    return ((min - balanced) / balanced, (max - balanced) / balanced)

def partition_balance_factor(connection, tablename):
    q = "Select min(card), max(card) from ("\
        f"Select tid/10000000000000000 as part, count(*) as card from {tablename} "\
        f"group by 1) as t"
    cur = execute(connection, q)
    row = cur.fetchone()
    min = row[0]
    max = row[1]
    cur.close()
    return float(max)/float(min)

def num_exceptions(connection, tablename, columns):
    total_card = table_card(connection, tablename)

    exception_rates = []

    for c in columns: 
        keys_with_mult_parts = f"Select {c} as c from {tablename} group by 1 having count(distinct graph_partition) > 1"
        keys_with_neg_parts = f"Select {c} as c from {tablename} where graph_partition < 0"
        q = f"Select count(*) from {tablename} where {c} in ({keys_with_mult_parts}) or {c} in ({keys_with_neg_parts})"
        cur = execute(connection, q)
        row = cur.fetchone()
        num_exceptions = row[0]
        exception_rates.append(num_exceptions/total_card)

    return exception_rates