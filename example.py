from numpy import True_
import pyodbc as pdb
import pandas as pd
from partition_statistics import *
from timing import *
import os

DISTRIBUTEDNE_EXE = "/home/sklaebe/workspace/patched_partitioning_scripts/graph/DistributedNE/build/DistributedNE"

def run_graph_partitioner(connection, table, columns, num_partitions, mode):
    from GraphPartitioner import GraphPartitioner
    print(f"\nRun graph partitioner for {num_partitions} partitions on {columns} with mode {mode}")

    edgefile = "/tmp/edges"
    waittime = 10
    partitioner = GraphPartitioner(connection, table, columns, num_partitions, False, True, False, mode, waittime)
    partitioner.reset_partitioning()
    start = tik()
    partitioner.run(edgefile, DISTRIBUTEDNE_EXE)
    exectime = tok(start)
    return exectime - waittime

def graph_part_exp(connection, tablename, part_keys, num_partitions, mode):
    exectime = run_graph_partitioner(connection, tablename, part_keys, num_partitions, mode)
    balance_factor = partition_balance_factor(connection, tablename)
    exception_rates = num_exceptions(connection, tablename, part_keys)
    mode_str = "t_as_e" if mode == 0 else "t_as_v"
    result = ["Graph_" + mode_str] + part_keys + [exectime, balance_factor] + exception_rates
    return result

def run_update_query(query, con):
    cursor = con.cursor()
    cursor.execute(query)
    con.commit()
    cursor.close()

def vwload(dbname, tablename, filepath):
    import subprocess
    cmd = ["vwload", "--timing", "--cluster", "--table", tablename, "-n", "null", dbname, filepath] 
    subprocess.run(cmd)

def run_iterative_partitioner(con, tablename, num_partitions, part_cols, input_file, output_file):
    from IterativePartitioner import IterativePartitioner
    import os
    p = IterativePartitioner(con, num_partitions, False)

    print(f"\nRun iterative partitioner for {num_partitions} partitions on {part_cols}")

    create_q_graph_part = """
        CREATE TABLE {tablename}(
            Number_of_Records smallint NOT NULL,
            a_aid_acontid_agencyid varchar(4) NOT NULL,
            a_aid_acontid_piid varchar(50) NOT NULL,
            ag_name varchar(50) NOT NULL,
            agency_code varchar(2) NOT NULL,
            ase_rowid integer NOT NULL,
            award_type varchar(21) NOT NULL,
            award_type_code varchar(1) NOT NULL,
            award_type_key smallint NOT NULL,
            baseandalloptionsvalue float NOT NULL,
            baseandexercisedoptionsvalue float NOT NULL,
            bureau_code varchar(2) NOT NULL,
            bureau_name varchar(40) NOT NULL,
            cd_contactiontype varchar(1),
            co_name varchar(44),
            co_state varchar(2),
            code varchar(2) NOT NULL,
            contract_num varchar(50),
            contract_signeddate varchar(20) NOT NULL,
            contractingofficeagencyid varchar(4) NOT NULL,
            count_fetched integer NOT NULL,
            count_total integer NOT NULL,
            description varchar(40) NOT NULL,
            fk_epa_des_prod smallint,
            fk_rec_mat smallint,
            ftsdollar float NOT NULL,
            funding_agency varchar(4),
            funding_agency_key integer NOT NULL,
            funding_agency_name varchar(40) NOT NULL,
            gsadollar float NOT NULL,
            gsaotherdollar float NOT NULL,
            gwacs float NOT NULL,
            level1_category varchar(37) NOT NULL,
            level2_category varchar(57) NOT NULL,
            naics_code varchar(6) NOT NULL,
            naics_name varchar(35) NOT NULL,
            nongsadollar float NOT NULL,
            obligatedamount float NOT NULL,
            obligatedamount_1 decimal(1, 0),
            pbsdollar float NOT NULL,
            primary_contract_piid varchar(51) NOT NULL,
            prod_or_serv_code varchar(4) NOT NULL,
            prod_or_serv_code_desc varchar(35),
            psc_code varchar(4) NOT NULL,
            psc_code_description varchar(100) NOT NULL,
            psc_key integer NOT NULL,
            quarter varchar(1) NOT NULL,
            refidvid_agencyid varchar(4) NOT NULL,
            refidvid_piid varchar(34),
            short_name varchar(11) NOT NULL,
            signeddate varchar(19) NOT NULL,
            vend_contoffbussizedeterm varchar(1) NOT NULL,
            vend_dunsnumber varchar(9) NOT NULL,
            vend_vendorname varchar(108),
            whocanuse varchar(50),
            year varchar(4) NOT NULL,
            graph_partition bigint NOT NULL
            ) with partition=(hash on graph_partition {num_partitions} partitions);
    """
    
    drop_q = "Drop table if exists {tablename}"
    
    commongovernment_cols = ["Number_of_Records",  "a_aid_acontid_agencyid",  "a_aid_acontid_piid" ,  "ag_name",  "agency_code",  "ase_rowid",  "award_type",  "award_type_code",  "award_type_key",  "baseandalloptionsvalue",  "baseandexercisedoptionsvalue",  "bureau_code",  "bureau_name",  "cd_contactiontype",  "co_name",
        "co_state",  "code",  "contract_num",  "contract_signeddate",  "contractingofficeagencyid",  "count_fetched",  "count_total",  "description",  "fk_epa_des_prod",  "fk_rec_mat",  "ftsdollar",  "funding_agency",  "funding_agency_key",  "funding_agency_name",  "gsadollar",  "gsaotherdollar",
        "gwacs",  "level1_category",  "level2_category",  "naics_code",  "naics_name",  "nongsadollar",  "obligatedamount",  "obligatedamount_1",  "pbsdollar",  "primary_contract_piid",  "prod_or_serv_code",  "prod_or_serv_code_desc",  "psc_code",  "psc_code_description",  "psc_key",  "quarter",
        "refidvid_agencyid",  "refidvid_piid",  "short_name",  "signeddate",  "vend_contoffbussizedeterm",  "vend_dunsnumber",  "vend_vendorname",  "whocanuse",  "year"]
    commongovernment_col_types = ["smallint" ,  "varchar(4)" ,  "varchar(50)" ,  "varchar(50)" ,  "varchar(2)" ,  "integer" ,  "varchar(21)" ,  "varchar(1)" ,  "smallint" ,  "float" ,  "float" ,  "varchar(2)" ,  "varchar(40)" ,  "varchar(1)",  "varchar(44)",  "varchar(2)",  "varchar(2)" ,  "varchar(50)",
        "varchar(20)" ,  "varchar(4)" ,  "integer" ,  "integer" , "varchar(40)" ,  "smallint",  "smallint", "float" ,  "varchar(4)",  "integer" , "varchar(40)" ,  "float" ,  "float" ,  "float" ,  "varchar(37)" ,  "varchar(57)" ,  "varchar(6)" ,  "varchar(35)" ,  "float" ,  "float" ,
        "decimal(1, 0)",  "float" ,  "varchar(51)" ,  "varchar(4)" ,  "varchar(35)",  "varchar(4)" ,  "varchar(100)" ,  "integer" ,  "varchar(1)" ,  "varchar(4)" ,  "varchar(34)",  "varchar(11)" ,  "varchar(19)" ,  "varchar(1)" ,  "varchar(9)" ,  "varchar(108)",  "varchar(50)",  "varchar(4)" ]

    part_col_offs = []
    for c in part_cols:
        idx = commongovernment_cols.index(c)
        assert(idx != ValueError)
        part_col_offs.append(idx)

    p.new_fact_table(tablename, commongovernment_cols, commongovernment_col_types, None, part_col_offs)
    for c in part_cols:
        p.add_partition_key(tablename, c)
    part_filename = p.partitioned_file_from_file(tablename, input_file, True)
    p.print_statistics()
    os.rename(part_filename, output_file)

    run_update_query(drop_q.format(tablename = tablename), con)
    run_update_query(create_q_graph_part.format(tablename = tablename, num_partitions = num_partitions), con)
    vwload(dbname, tablename, output_file)
    os.remove(output_file)


def iterative_part_exp(connection, tablename, part_keys, num_partitions, flat_file):
    partitioned_flat_file = "/tmp/part_flat_file.csv"
    start = tik()
    run_iterative_partitioner(connection, tablename, num_partitions, part_keys, flat_file, partitioned_flat_file)
    exectime = tok(start)
    balance_factor = partition_balance_factor(connection, tablename)
    exception_rates = num_exceptions(connection, tablename, part_keys)
    result = ["Iterative"] + part_keys + [exectime, balance_factor] + exception_rates
    return result

if __name__ == "__main__":

    os.system("ingstart")
    dbname = "testdb"
    connection_string = f"Driver=Ingres;Server=(local);Database={dbname}"

    # The table name to run graph partitioning on
    fact_table = "temp"
    # The flat file to run the iterative partitioner on 
    fact_file = "/tmp/tempfile.csv"

    num_partitions = 4
    runs = 1

    two_col_combs = [
        ["ag_name", "bureau_name"],
        ["ag_name", "co_name"],
        ["bureau_name", "co_name"],
        ["vend_dunsnumber", "vend_vendorname"]
    ]

    three_col_combs = [
        ["ag_name", "bureau_name", "co_name"],
        ["ag_name", "bureau_name", "co_state"]
    ]

    two_col_results = []
    for comb in two_col_combs:
        for r in range(runs):
            connection = pdb.connect(connection_string)
            two_col_results.append(graph_part_exp(connection, fact_table, comb, num_partitions, 0))
            connection.close()

            connection = pdb.connect(connection_string)
            two_col_results.append(graph_part_exp(connection, fact_table, comb, num_partitions, 1))
            connection.close()

            connection = pdb.connect(connection_string)
            test_table = "temptable"
            two_col_results.append(iterative_part_exp(connection, test_table, comb, num_partitions, fact_file))
            connection.close()
    
    df = pd.DataFrame(two_col_results, columns = ['Partitioner', 'Col1', 'Col2', 'Runtime', 'Partition_balance_factor', 'Exceptions1', 'Exceptions2'])
    print(df)
    df.to_csv("/tmp/two_cols.csv")

    three_col_results = []
    for comb in three_col_combs:
        for r in range(runs):
            connection = pdb.connect(connection_string)
            three_col_results.append(graph_part_exp(connection, fact_table, comb, num_partitions, 0))
            connection.close()
            
            connection = pdb.connect(connection_string)
            three_col_results.append(graph_part_exp(connection, fact_table, comb, num_partitions, 1))
            connection.close()

            connection = pdb.connect(connection_string)
            test_table = "temptable"
            three_col_results.append(iterative_part_exp(connection, test_table, comb, num_partitions, fact_file))
            connection.close()
    
    df = pd.DataFrame(three_col_results, columns = ['Partitioner', 'Col1', 'Col2', 'Col3', 'Runtime', 'Partition_balance_factor', 'Exceptions1', 'Exceptions2', 'Exceptions3'])
    print(df)
    df.to_csv("/tmp/three_cols.csv")