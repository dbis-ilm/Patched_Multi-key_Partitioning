import os
import itertools
import subprocess
from odbc_utils import *
from timing import tik, tok

os.environ["II_ODBC_WCHAR_SIZE"] = "2"

def find_in_path(executable):
    for path in os.environ["PATH"].split(os.pathsep):
        exe_file = os.path.join(path, executable)
        if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
            return True
    return False

class GraphPartitioner:
    from identifier_mapping_dict import partition_identifier_mapping_dict as id_map

    def __init__(self, connection, table, cols, partitions, verbose = False, apply = True, print_stats = False, mode = 0, waittime = 0):
        self.table = table
        self.cols = cols
        self.partitions = partitions
        self.verbose = verbose
        self.apply = apply
        self.print_stats = print_stats
        self.partition_identifier_mapping = None
        self.connection = connection
        self.waittime = waittime
        if mode == 0: # tuples as edges
            self.table_to_graph = self.table_to_graph_t_as_e
            self.update_table_with_graph_part = self._update_table_with_graph_part_t_as_e
        elif mode == 1: # tuples as vertices
            self.table_to_graph = self.table_to_graph_t_as_v
            self.update_table_with_graph_part = self._update_table_with_graph_part_t_as_v
        else:
            assert False, "Mode must be 0 for tuples as edges or 1 for tuples as vertices"
        assert len(cols) <= 3, "Currently only implemented graph-table mappings for at most 3 columns"

    def _get_partition_multiplier(self):
        q = f"select top 1 tid/10000000000000000, count(*) as part from {self.table} " \
            "group by tid/10000000000000000 order by count(*) desc"
        cur = execute(self.connection, q, self.verbose)
        return cur.fetchone()[1]

    def _declare_id_mappings(self):
        
        assert(hasattr(self, "col_id_offsets"))
        
        tid_normalization = f"mod(tid, 10000000000000000) + ({self.partition_multiplier} * (tid/10000000000000000))"

        mappings = []
        cum_off = [0]
        for i in range(1, len(self.cols)): cum_off.append(cum_off[i-1] + self.col_id_offsets[i])

        for i in range(len(self.cols)):
            map_name = f"{self.cols[i]}_mapping"
            off_str = "+ {}".format(cum_off[i]) if cum_off[i] > 0 else ""
            q = f"DECLARE GLOBAL TEMPORARY TABLE {map_name} as " \
                f"select {self.cols[i]}, min({tid_normalization}) {off_str} as id from {self.table} group by {self.cols[i]}" 
            cur = execute(self.connection, q, self.verbose)
            cur.close()
            mappings.append(map_name)
        return mappings


    def _get_column_id_offsets(self):
        col_id_offsets = [0] # First col has no offset

        tid_normalization = f"mod(tid, 10000000000000000) + ({self.partition_multiplier} * (tid/10000000000000000))"

        for i in range(len(self.cols) - 1):
            value_to_id = "with value_to_id as "\
                f"(select {self.cols[i]}, min({tid_normalization}) as id from {self.table} group by {self.cols[i]})"
            q = f"{value_to_id} select max(id) from value_to_id"
            cur = execute(self.connection, q, self.verbose)
            col_id_offsets.append(cur.fetchone()[0])
        
        return col_id_offsets

    def _create_partition_identifier_mapping(self):
        '''
        Graph partitions have identifiers from 0 to self.partitions-1 after running the partitioning. 
        However, when using a DBMS's hash partitioning, this does not neccessarily lead to n balanced
        partitions. Therefore, we replace identifiers 10..n-1 with identifiers that lead to a balanced partitioning.
        As the inverse function of the hash function does not exist, we determine the replacement
        by trying different arguments until we get an identifier for each of the n buckets. 
        NOTE: This assumes that the hash function used for partitioning is accessible.
        '''

        self.partition_identifier_mapping = self.id_map.get(self.partitions)

        if self.partition_identifier_mapping is not None: 
            if self.verbose: print("Using existing partition identifier mapping from the global dictionary.")
            return

        self.partition_identifier_mapping = [0] * self.partitions
        filled = 0
        i = self.partitions + 1 # Start with higher id to avoid conflicts when replacing
        while filled != self.partitions:
            cur = execute(self.connection, f"Select top 1 mod(hash({i}), {self.partitions}) from {self.table}", self.verbose)
            bucket = cur.fetchone()[0]
            if (self.partition_identifier_mapping[bucket] == 0):
                self.partition_identifier_mapping[bucket] = i
                filled += 1
            cur.close()
            i += 1
            assert i < self.random_min, "Cannot find an partition id mapping "\
                "with values less than the current minimum for random assignment "\
                f"to exceptions. (Current min is {self.random_min}."
        print(self.partition_identifier_mapping)

    def table_to_graph_t_as_e(self, filename):
        num_cols = len(self.cols)
        combs = list(itertools.combinations(range(0,num_cols), 2))

        self.partition_multiplier = self._get_partition_multiplier()
        self.col_id_offsets = self._get_column_id_offsets()
        self.col_id_mappings = self._declare_id_mappings()
        q = f"select x.id as id1, y.id as id2 from {self.table} as t " \
            f"INNER JOIN {self.col_id_mappings[combs[0][0]]} as x on (t.{self.cols[combs[0][0]]} = x.{self.cols[combs[0][0]]} " \
            f"OR t.{self.cols[combs[0][0]]} is null and x.{self.cols[combs[0][0]]} is null) " \
            f"INNER JOIN {self.col_id_mappings[combs[0][1]]} as y on (t.{self.cols[combs[0][1]]} = y.{self.cols[combs[0][1]]} " \
            f"OR t.{self.cols[combs[0][1]]} is null and y.{self.cols[combs[0][1]]} is null) "
        
        for i in range(1, len(combs)):
            q += f"UNION ALL select x.id as id1, y.id as id2 from {self.table} as t " \
                f"INNER JOIN {self.col_id_mappings[combs[i][0]]} as x on (t.{self.cols[combs[i][0]]} = x.{self.cols[combs[i][0]]} " \
                f"OR t.{self.cols[combs[i][0]]} is null and x.{self.cols[combs[i][0]]} is null) " \
                f"INNER JOIN { self.col_id_mappings[combs[i][1]]} as y on (t.{self.cols[combs[i][1]]} = y.{self.cols[combs[i][1]]} " \
                f"OR t.{self.cols[combs[i][1]]} is null and y.{self.cols[combs[i][1]]} is null) " 
        
        cur = execute(self.connection, q, self.verbose)
        f = open(filename, "w")
        for row in cur:
            f.write(f"{row[0]} {row[1]}\n")
        f.close()
        self.edgefile = filename

    def table_to_graph_t_as_v(self, filename):
        self.partition_multiplier = self._get_partition_multiplier()

        tid_normalization = "mod({}, 10000000000000000) + ({} * ({}/10000000000000000))"

        q = f"select t1.tid as id1, t2.tid as id2 from {self.table} as t1, {self.table} as t2 " \
            f"where t1.{self.cols[0]} = t2.{self.cols[0]} and t1.tid < t2.tid "
        for i in range(1, len(self.cols)):
            q += f"UNION ALL select t1.tid as id1, t2.tid as id2 from {self.table} as t1, {self.table} as t2 " \
                f"where t1.{self.cols[i]} = t2.{self.cols[i]} and t1.tid < t2.tid "

        query = f"select {tid_normalization.format('id1', self.partition_multiplier, 'id1')} as id1, "\
            f"{tid_normalization.format('id2', self.partition_multiplier, 'id2')} as id2 from ({q}) t"

        cur = execute(self.connection, query, self.verbose)
        f = open(filename, "w")
        for row in cur:
            f.write(f"{row[0]} {row[1]}\n")
        f.close()
        self.edgefile = filename

    def run_partitioning(self, exe_path, options:str = None):
        # Check for DistributedNE executable
        if not (os.path.isfile(exe_path) and os.access(exe_path, os.X_OK)):
            print("Could not find DistributedNE executable")
            return

        # Check for MPI in path      
        if not find_in_path("mpirun"): 
            print("Could not find MPI executables")
            return
        
        if not hasattr(self, "edgefile") or not os.path.isfile(self.edgefile):
            print("Could not find edgefile, please run table_to_graph first")
            return

        option_str = options if options else ""
        cmd = f"mpirun -n {self.partitions} {exe_path} {option_str}"\
            f" {self.edgefile} {self.partitions}"\
            .split()
        if self.verbose:
            print("\n===================================\n Distributed NE Log:\n")
            subprocess.run(cmd)
            print("\n===================================\n")
        else:
            subprocess.run(cmd, stdout=subprocess.DEVNULL)

        return f"{self.edgefile}.{self.partitions}.pedges"

    def _graph_to_table_mapping_table_t_as_e_2_cols(self):
        num_cols = len(self.cols)
        combs = list(itertools.combinations(range(0,num_cols), 2))
        col_list = ", ".join(self.cols)

        dggt_results = "DECLARE GLOBAL TEMPORARY TABLE f as\n"\
            f"select {col_list}, part from graph_part p " \
            f"INNER JOIN {self.col_id_mappings[combs[0][0]]} as x on p.t0 = x.id " \
            f"INNER JOIN {self.col_id_mappings[combs[0][1]]} as y on p.t1 = y.id "
        
        for i in range(1, len(combs)):
            dggt_results += f"UNION ALL select {col_list}, part from graph_part p " \
                f"INNER JOIN {self.col_id_mappings[combs[i][0]]} as x on p.t0 = x.id " \
                f"INNER JOIN {self.col_id_mappings[combs[i][1]]} as y on p.t1 = y.id "
        
        dggt_results += "\nON COMMIT PRESERVE ROWS WITH NORECOVERY"
        return dggt_results

    def _graph_to_table_mapping_table_t_as_e_3_cols(self):
        num_cols = len(self.cols)
        combs = list(itertools.combinations(range(0,num_cols), 2))
        col_list = ", ".join(self.cols)
        cum_off = [0]
        for i in range(1, len(self.cols)): cum_off.append(cum_off[i-1] + self.col_id_offsets[i])

        queries = []
        for i,x in enumerate(combs):
            for y in combs[i+1:]:
                joincols = None
                p2_datacol = None
                if (x[0] == y[0]): 
                    joincols = [0,0]
                    p2_datacol = 1
                elif (x[0] == y[1]): 
                    joincols = [0,1]
                    p2_datacol = 0
                elif (x[1] == y[0]): 
                    joincols = [1,0]
                    p2_datacol = 1
                elif (x[1] == y[1]): 
                    joincols = [1,1]
                    p2_datacol = 0

                p1_t0_cond = f" between {cum_off[x[0]]} and {cum_off[x[0] + 1]} " if x[0] < num_cols - 1 else f" >= {cum_off[x[0]]}"
                p1_t1_cond = f" between {cum_off[x[1]]} and {cum_off[x[1] + 1]} " if x[1] < num_cols - 1 else f" >= {cum_off[x[1]]}"
                p2_t0_cond = f" between {cum_off[y[0]]} and {cum_off[y[0] + 1]} " if y[0] < num_cols - 1 else f" >= {cum_off[y[0]]}"
                p2_t1_cond = f" between {cum_off[y[1]]} and {cum_off[y[1] + 1]} " if y[1] < num_cols - 1 else f" >= {cum_off[y[1]]}"

                if joincols:
                    q = f"select {col_list}, CASE WHEN part1=part2 THEN part1 ELSE RANDOM(-32768,-1) END AS part from ("\
                        f"Select p1.t0 as c0, p1.t1 as c1, p2.t{p2_datacol} as c2, p1.part as part1, p2.part as part2 from graph_part p1, graph_part p2 "\
                        f"where p1.t0 {p1_t0_cond} and p1.t1 {p1_t1_cond} and p2.t0 {p2_t0_cond} and p2.t1 {p2_t1_cond} "\
                        f"and p1.t{joincols[0]} = p2.t{joincols[1]}) t "\
                        f"INNER JOIN {self.col_id_mappings[x[0]]} as x on t.c0 = x.id " \
                        f"INNER JOIN {self.col_id_mappings[x[1]]} as y on t.c1 = y.id " \
                        f"INNER JOIN {self.col_id_mappings[y[p2_datacol]]} as z on t.c2 = z.id"
                    queries.append(q)      
        q_str = "\n UNION ALL \n".join(queries)

        dggt_results = f"DECLARE GLOBAL TEMPORARY TABLE f as\n select {col_list}, CASE when minpart = maxpart THEN minpart ELSE RANDOM(-32768,-1) END as part from (\n"\
            f"select {col_list}, min(part) as minpart, max(part) as maxpart from (\n{q_str} \n) t group by {col_list}) t \n ON COMMIT PRESERVE ROWS WITH NORECOVERY"
        
        return dggt_results

    def _load_part_edgefile(self, part_edgefile):
        if  not os.path.isfile(part_edgefile):
            print("Could not find partitioned edgefile, please run partitioning first")
            return False
        
        # Check for vwload in path      
        if not find_in_path("vwload"): 
            print("Could not find vwload executable")
            return False

        # TODO: Consider using external table
        execute(self.connection, "Drop table if exists graph_part", self.verbose).close()
        q = "Create table graph_part(t0 BIGINT, t1 BIGINT, part INT)"
        execute(self.connection, q, self.verbose).close()

        path, file = os.path.split(part_edgefile)
        load_q = f"COPY graph_part() VWLOAD FROM '{file}' "\
            f"WITH FDELIM=' ',insertmode ='Bulk', header, WORK_DIR = '{path}'"
        execute(self.connection, load_q, self.verbose)

        return True

    def _create_graph_part_col(self):
        # (Re)create graph_partition column
        # TODO: This is dangerous if graph_partition is a user column
        # TODO: Is it really needed to empty the graph partition column? Simply overwrite...
        execute(self.connection, f"Modify {self.table} to combine", self.verbose).close()
        execute(self.connection, f"Alter table {self.table} drop graph_partition restrict", self.verbose, True).close()
        res = execute(self.connection, f"Alter table {self.table} add column graph_partition SMALLINT", self.verbose)
        if (res is None):
            print("Can not (re)create column graph_partition. Is this already the partition key?")
            return False
        res.close()
        return True

    def _update_table_with_graph_part_t_as_e(self):
        # Prepare and run the temporary table definition containing the assignments. Currently for 2 or 3 keys.
        if (len(self.cols) == 2): dggt_results = self._graph_to_table_mapping_table_t_as_e_2_cols()
        if (len(self.cols) == 3): dggt_results = self._graph_to_table_mapping_table_t_as_e_3_cols()
        execute(self.connection, dggt_results, self.verbose).close()

        # Prepare and run the update statement
        update_q = f"update {self.table} t from f set t.graph_partition = f.part "\
            f"where (f.{self.cols[0]} = t.{self.cols[0]} or f.{self.cols[0]} is null and t.{self.cols[0]} is null) "
        for i in range(1, len(self.cols)):
            update_q += f"and (f.{self.cols[i]} = t.{self.cols[i]} or f.{self.cols[i]} is null and t.{self.cols[i]} is null) "   
        
        execute(self.connection, update_q, self.verbose).close()   

        # Tuples without edges never occured in the edge file, because their partition key values only occur once. 
        # Simply give them a non-exception partition identifier
        update_q = f"update {self.table} set graph_partition = random(1,32000) where graph_partition is null"
        execute(self.connection, update_q, self.verbose).close() 

    def _update_table_with_graph_part_t_as_v(self):
        # Prepare and run the temporary table definition containing the assignments. Currently for 2 or 3 keys.
        dggt_results = f"DECLARE GLOBAL TEMPORARY TABLE f as\n select id, CASE when minpart = maxpart THEN minpart ELSE RANDOM(-32768,-1) END as part from (\n"\
            f"select id, min(part) as minpart, max(part) as maxpart from (select t0 as id, part from graph_part UNION select t1 as id, part from graph_part) t group by id) t \n ON COMMIT PRESERVE ROWS WITH NORECOVERY"
        execute(self.connection, dggt_results, self.verbose).close()

        # Prepare and run the update statement
        update_q = f"update {self.table} t from f set t.graph_partition = f.part where f.id =  mod(t.tid, 10000000000000000) + ({self.partition_multiplier} * (t.tid/10000000000000000))"
        execute(self.connection, update_q, self.verbose).close()   

        # Tuples without edges never occured in the edge file, because their partition key values only occur once. 
        # Simply give them a non-exception partition identifier
        update_q = f"update {self.table} set graph_partition = random(1,32000) where graph_partition is null"
        execute(self.connection, update_q, self.verbose).close() 

    def graph_to_table(self, part_edgefile):
        if not self._load_part_edgefile(part_edgefile):
            return False

        if not self._create_graph_part_col():
            return False
        
        self.update_table_with_graph_part()
        
        # Cleanup and commit
        execute(self.connection, "Drop table graph_part", self.verbose).close()
        commit(self.connection)

        return True

    def _apply_partition_identifier_mapping(self):
        '''
        Replace graph partition identifiers 0..n-1 to identifiers that lead to a 
        balanced hash partitioning.
        '''
        import time

        assert(self.partition_identifier_mapping is not None)

        time.sleep(self.waittime)
        for i in range(len(self.partition_identifier_mapping)):
            execute(self.connection, f"Update {self.table} set graph_partition = {self.partition_identifier_mapping[i]} "\
            f"where graph_partition = {i}", self.verbose)\
            .close()  

    def apply_partitioning(self):
        self._create_partition_identifier_mapping()
        self._apply_partition_identifier_mapping()
        
        q = f"Modify {self.table} to reconstruct with partition="\
            f"(hash on graph_partition {self.partitions} partitions)"
        execute(self.connection, q, self.verbose).close()
        commit(self.connection)

    def print_statistics(self):
        print(f"\nTable: {self.table}\nColumns: {self.cols})")
        cur = execute(self.connection, f"Select count(*) from {self.table}")
        total_tuples = cur.fetchone()[0]
        cur.close()
        print(f"Total tuples: {total_tuples}")

        for col in self.cols:
            cur = execute(self.connection, f"Select count(distinct {col}) from {self.table}")
            t = cur.fetchone()[0]
            cur.close()
            print(f"Distinct values of {col}: {t}")

        for col in self.cols:
            cur = execute(self.connection, f"select count(*) from {self.table} where {col} in "\
                f"(select {col} from {self.table} group by 1 having count(distinct graph_partition)>1)")
            num_exceptions = cur.fetchone()[0]
            cur.close()
            print(f"Exception rate in column {col}: {(num_exceptions/total_tuples) * 100:6.3f}%")
        
        equal_part_size = total_tuples / self.partitions

        cur = execute(self.connection, f"select max(cnt) from ("\
            f"select graph_partition, count(*) as cnt from {self.table} "\
            f"group by 1) as t")
        max_graph_part = cur.fetchone()[0]
        cur.close()

        cur = execute(self.connection, f"select min(cnt) from ("\
            f"select graph_partition, count(*) as cnt from {self.table} "\
            f" group by 1) as t")
        min_graph_part = cur.fetchone()[0]
        cur.close()

        print(f"Graph partition sizes:\n"\
            f" Min: {min_graph_part} ({(min_graph_part - equal_part_size) / equal_part_size * 100:+6.3f}% from balanced partitioning)\n"\
            f" Max: {max_graph_part} ({(max_graph_part - equal_part_size) / equal_part_size * 100:+6.3f}% from balanced partitioning)\n")

        if (self.apply):
            cur = execute(self.connection, f"select max(cnt) from ("\
                f"select tid/10000000000000000, count(*) as cnt from {self.table} "\
                f"group by 1) as t")
            max_actual_part = cur.fetchone()[0]
            cur.close()

            cur = execute(self.connection, f"select min(cnt) from ("\
                f"select tid/10000000000000000, count(*) as cnt from {self.table} "\
                f"group by 1) as t")
            min_actual_part = cur.fetchone()[0]
            cur.close()

            print(f"Actual partition sizes:\n"\
                f" Min: {min_actual_part} ({(min_actual_part - equal_part_size) / equal_part_size * 100:+6.3f}% from balanced partitioning)\n"\
                f" Max: {max_actual_part} ({(max_actual_part - equal_part_size) / equal_part_size * 100:+6.3f}% from balanced partitioning)\n")
            

    def reset_partitioning(self):
        q = f"Modify {self.table} to reconstruct with partition="\
            f"(hash on {self.cols[0]} {self.partitions} partitions)"
        execute(self.connection, q, self.verbose).close()
        commit(self.connection)

    def run(self, graph_file, distributedNE_bin):
        start = tik()   
        self.table_to_graph(graph_file)
        tok(start, "### Table to graph runtime:")
        start = tik()
        part_edgefile = self.run_partitioning(distributedNE_bin)
        if not part_edgefile:
            print("Calling the partitioner failed. Exiting.")
            exit()
        tok(start, "### Run partitioning runtime:")
        start = tik()
        if not self.graph_to_table(part_edgefile): 
            tok(start,"### Graph to table failed after")
            return
        tok(start, "### Graph to table runtime:")
        if self.apply: 
            start = tik()
            self.apply_partitioning()
            tok(start, "### Apply partitioning runtime:")
        if self.print_stats:
            self.print_statistics()
        os.remove(graph_file)