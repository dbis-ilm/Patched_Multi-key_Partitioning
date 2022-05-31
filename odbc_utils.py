import sys

def commit(connection):
    try:
        connection.commit()
    except Exception as e:
        print(f"Failed to commit session. Reason: {e}", file=sys.stderr)

def execute(connection, sql, verbose = False, mightFail = False):
    cursor = connection.cursor()
    if verbose: print(f"{sql};")
    try:
        cursor.execute(sql)
    except Exception as e:
        if not mightFail:
            print(f"Failed to execute query. Reason: {e}", file=sys.stderr)
            print(f"Query: {sql}", file=sys.stderr)
            return None
    
    return cursor  

def print_cursor(cur):
    rs = cur.fetchall()
    for i in range(0,len(rs)):
        print(rs[i])
    cur.close()