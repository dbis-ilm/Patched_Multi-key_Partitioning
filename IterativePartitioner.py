class IterativePartitioner:
    from identifier_mapping_dict import partition_identifier_mapping_dict as id_map
    EXCEPTION_MARKER = -1
    UNKNOWN_OFFSET = -1

    class Table:
        name = None
        colnames = None
        coltypes = None
        # Single column keys for now
        primary_key_offset = 0
        partition_key_offs = None
        is_fact_table = False
        string_col_offs = None

        def __init__(self, tablename, colnames, coltypes, is_fact_table, primary_key_offset = None, partition_key_offs = None):
            self.name = tablename
            self.colnames = colnames
            self.coltypes = coltypes
            self.is_fact_table = is_fact_table
            self.primary_key_offset = primary_key_offset
            self.partition_key_offs = partition_key_offs

            self.string_col_offs = []
            for i,t in enumerate(coltypes):
                if "char" in t.lower():
                    self.string_col_offs.append(i)
    
    class ForeignKey:
        pk_table = None
        fk_table = None
        # Single column keys for now
        pk_col = None
        fk_col = None

        def __init__(self, fk_table, fk_col, pk_table, pk_col):
            self.fk_table = fk_table
            self.fk_col = fk_col
            self.pk_table = pk_table
            self.pk_col = pk_col

    def __init__(self, con, num_partitions, trace_partitions = False):
        self.last_choice = 0
        self.con = con
        self.num_partitions = num_partitions
        self.part_id_map = self.id_map[self.num_partitions]
        assert self.part_id_map != None, "Partition identifier mapping does not exist"
        self.trace_partitions = trace_partitions
        self.partition_counts = [0] * num_partitions
        self.mappings = []
        self.partitions = []
        self.key_map = []
        self.num_keys = 0
        self.tables = []
        self.fks = []
        self.time = 0.0

    def _run_query(self, query, should_print = True):
        cursor = self.con.cursor()
        cursor.execute(query)
        rs = cursor.fetchall()
        if not rs:
            print("Query result is empty")
        colnames = [desc[0] for desc in cursor.description]
        if should_print:
            print(colnames)
            for res in rs:
                print(res)
        cursor.close()

    def _run_update_query(self, query):
        cursor = self.con.cursor()
        cursor.execute(query)
        self.con.commit()
        cursor.close()

    def _get_table_off_by_name(self, tablename):
        for i, t in enumerate(self.tables):
            if t.name == tablename:
                return i
        return self.UNKNOWN_OFFSET
    
    def _get_col_off_by_name(self, tab_off, colname):
        assert(tab_off < len(self.tables))
        for i, c in enumerate(self.tables[tab_off].colnames):
            if c == colname:
                return i
        return self.UNKNOWN_OFFSET

    def _get_fk_off_by_pk_table(self, pk_table):
        for i, t in enumerate(self.fks):
            if t.pk_table == pk_table:
                return i
        return self.UNKNOWN_OFFSET

    def _hash_fact(self, row):
        part = self.partition_counts.index(min(self.partition_counts))
        return (part, self.part_id_map[part])

    def _hash_dim(self, row, key_idx):
        part = hash(row[key_idx]) % self.num_partitions
        return (part, self.part_id_map[part])

    def _get_dim_partition(self, row, fact_key_off, dim_key_off):
        part = self.mappings[fact_key_off].get(row[dim_key_off])
        #if (part == self.EXCEPTION_MARKER):
        #    print(f"Value {row[dim_key_off]} is an exception")
        if part == self.EXCEPTION_MARKER or part == None:
            part = row[dim_key_off]
        return part

    def _get_fact_partition(self, row):
        lookups = [None] * self.num_keys
        for cnt, idx in enumerate(self.key_map):
            lookups[cnt] = self.mappings[cnt].get(row[idx])

        # All lookups are None or Exceptions, so no decicion enforced for this tuple
        if lookups.count(None) + lookups.count(self.EXCEPTION_MARKER) == len(lookups):
            part_off, part = self._hash_fact(row)
            for cnt, idx in enumerate(self.key_map):
                if lookups[cnt] is None:
                    self.mappings[cnt][row[idx]] = part

        # We need to derive the partition from the already observed tuples
        else:
            part = None
            for l in lookups:
                # Get the first assignment in the list
                if l is not None and l != self.EXCEPTION_MARKER and part is None:
                    part = l
                # If we encounter a second, different assignment in list, 
                # we enforce partitioning according to one column value, making the other to exceptions.
                # The chosen column varies in round robin manner to balance exceptions on all columns
                elif l is not None and l != self.EXCEPTION_MARKER and l != part:
                    choice = (self.last_choice + 1) % self.num_keys
                    # We have at least two different part_ids, so it is ensured that this is no infinite loop
                    while lookups[choice] is None or lookups[choice] == self.EXCEPTION_MARKER:
                        choice = (choice + 1) % self.num_keys
                    part = lookups[choice]
                    self.last_choice = choice
                    break
            
            # Now actually 
            for cnt, l in enumerate(lookups):
                # Co-partition if no assignment yet
                if l is None: 
                    self.mappings[cnt][row[self.key_map[cnt]]] = part
                # If there is an assignment that differs from the chosen one, the column value
                # is now an exception
                elif l != part:
                    self.mappings[cnt][row[self.key_map[cnt]]] = self.EXCEPTION_MARKER
            part_off = self.part_id_map.index(part)

        if self.trace_partitions:
            for cnt, idx in enumerate(self.key_map):
                self.partitions[cnt][part_off].append(row[idx])

        self.partition_counts[part_off] += 1
        return part

    def _create_table(self, table_off):
        assert(table_off < len(self.tables))
        t = self.tables[table_off]
        cols_zipped = list(zip(t.colnames, t.coltypes))
        col_list = []
        for i, c in enumerate(cols_zipped):
            pk_str = ""
            if t.primary_key_offset == i:
                pk_str = " primary key"
            col_list.append(f"{c[0]} {c[1]}{pk_str}")
        
        q = f"Create table {t.name}({','.join(col_list)}) with partition=(hash on p {self.num_partitions} partitions)"
        #self._run_update_query(q)

    def _create_fk(self, fk_off):
        assert(fk_off < len(self.fks))
        fk = self.fks[fk_off]
        q = f"Alter table {fk.fk_table} add constraint fk_{fk.fk_table}_{fk.pk_table} FOREIGN KEY"\
            f"({fk.fk_col}) REFERENCES {fk.pk_table} ({fk.pk_col}) "
        self._run_update_query(q)

    def new_fact_table(self, tablename, colnames, coltypes, primary_key_offset = None, partition_key_offsets = None):
        if self._get_table_off_by_name(tablename) != self.UNKNOWN_OFFSET:
            print(f"Table {tablename} already exists")
            return
        
        if primary_key_offset and coltypes[primary_key_offset].tolower() != "int":
            print("Currently only allowing integer primary keys")
        
        colnames.append("p")
        coltypes.append("int")

        t = self.Table(tablename, colnames, coltypes, True, primary_key_offset, partition_key_offsets)
        self.tables.append(t)

        self._create_table(len(self.tables) - 1)

    def new_dim_table(self, tablename, colnames, coltypes, primary_key_offset, partition_key_offsets = None):
        if self._get_table_off_by_name(tablename) != self.UNKNOWN_OFFSET:
            print(f"Table {tablename} already exists")
            return

        if coltypes[primary_key_offset].lower() != "int":
            print("Currently only allowing integer primary keys")
        
        colnames.append("p")
        coltypes.append(coltypes[primary_key_offset])

        t = self.Table(tablename, colnames, coltypes, False, primary_key_offset, partition_key_offsets)
        self.tables.append(t)

        self._create_table(len(self.tables) - 1)

    def new_foreign_key(self, fk_table, fk_col, pk_table, pk_col):
        pk_t_off = self._get_table_off_by_name(pk_table)
        if pk_t_off == self.UNKNOWN_OFFSET:
            print(f"Table {pk_table} does not exist")
            return

        fk_t_off = self._get_table_off_by_name(fk_table)
        if fk_t_off == self.UNKNOWN_OFFSET:
            print(f"Table {fk_table} does not exist")
            return

        pk_col_off = self._get_col_off_by_name(pk_t_off, pk_col)
        if pk_col_off == self.UNKNOWN_OFFSET:
            print(f"Column {pk_col} does not exist in table {pk_table}")
            return

        fk_col_off = self._get_col_off_by_name(fk_t_off, fk_col)
        if fk_col_off == self.UNKNOWN_OFFSET:
            print(f"Column {fk_col} does not exist in table {fk_table}")
            return

        fk = self.ForeignKey(fk_table, fk_col, pk_table, pk_col)
        self.fks.append(fk)

        self.key_map.append(fk_col_off)
        self.num_keys += 1
        self.mappings.append({})
        if self.trace_partitions:
            self.partitions.append([])
            for p in range(self.num_partitions):
                self.partitions[len(self.partitions) - 1].append([])
    
    def add_partition_key(self, tablename, columnname):
        t_off = self._get_table_off_by_name(tablename)

        if t_off == self.UNKNOWN_OFFSET:
            print(f"Table {tablename} does not exist")
            return

        col_off = self._get_col_off_by_name(t_off, columnname)
        if col_off == self.UNKNOWN_OFFSET:
            print(f"Column {columnname} does not exist in table {tablename}")
            return

        self.key_map.append(col_off)
        self.num_keys += 1
        self.mappings.append({})
        if self.trace_partitions:
            self.partitions.append([])
            for p in range(self.num_partitions):
                self.partitions[len(self.partitions) - 1].append([])

    def insert_fact_rows(self, tablename, rows):
        t_off = self._get_table_off_by_name(tablename) 
        if t_off == self.UNKNOWN_OFFSET:
            print(f"Table {tablename} does not exist")
            return

        inserts = []
        for r in rows:
            for i in self.tables[t_off].string_col_offs:
                r[i] = f"'{r[i]}'"
            r.append(self._get_fact_partition(r))
            inserts.append(f"({','.join(str(x) for x in r)})")
        import time
        start = time.perf_counter()
        self._run_update_query(f"Insert into {tablename} values {','.join(inserts)}")
        end = time.perf_counter()
        self.time += end - start

    def _pk_fk_column_offsets_by_pk_table(self, tablename):
        dim_t_off = self._get_table_off_by_name(tablename)
        if dim_t_off == self.UNKNOWN_OFFSET:
            print(f"Table {tablename} does not exist")
            return (self.UNKNOWN_OFFSET, self.UNKNOWN_OFFSET)

        fk_off = self._get_fk_off_by_pk_table(tablename)
        if fk_off == self.UNKNOWN_OFFSET:
            return (self.UNKNOWN_OFFSET, self.UNKNOWN_OFFSET)
         
        fk = self.fks[fk_off]

        pk_off = self._get_col_off_by_name(dim_t_off, fk.pk_col)
        fact_t_off = self._get_table_off_by_name(fk.fk_table)
        fk_off = self._get_col_off_by_name(fact_t_off, fk.fk_col)

        return (pk_off, fk_off)

    def insert_dim_rows(self, tablename, rows):
        dim_t_off = self._get_table_off_by_name(tablename)
        if dim_t_off == self.UNKNOWN_OFFSET:
            print(f"Table {tablename} does not exist")
            return
        
        pk_off, fk_off = self._pk_fk_column_offsets_by_pk_table(tablename)
        if (pk_off == self.UNKNOWN_OFFSET or fk_off == self.UNKNOWN_OFFSET):
            return

        inserts = []
        for r in rows:
            for i in self.tables[dim_t_off].string_col_offs:
                r[i] = f"'{r[i]}'"
            r.append(self._get_dim_partition(r, fk_off, pk_off))
            inserts.append(f"({','.join(str(x) for x in r)})")
        import time
        start = time.perf_counter()
        self._run_update_query(f"Insert into {tablename} values {','.join(inserts)}")
        end = time.perf_counter()
        self.time += end - start

    def load_table_from_file(self, tablename, filename, is_fact_table, limit = 0):
        import time
        from csv import reader
        start = time.perf_counter()
        with open(filename, 'r') as read_obj:
            csv_reader = reader(read_obj, delimiter='|')
            total = 0
            batch_size = 1000
            batch = []
            count = 0

            for row in csv_reader:
                batch.append(row)
                count += 1

                if count >= batch_size:
                    if is_fact_table:
                        self.insert_fact_rows(tablename, batch)
                    else:
                        self.insert_dim_rows(tablename, batch)
                    batch = []
                    count = 0
                    total += batch_size
                    print(f"Finished {total} Tuples of {filename}",  end = "\r")

                if (limit > 0 and total >= limit): break
        
        if batch:
            if is_fact_table:
                self.insert_fact_rows(tablename, batch)
            else:
                self.insert_dim_rows(tablename, batch)
            total += len(batch)
            print(f"Finished {total} Tuples of {filename}",  end = "\r")
        end = time.perf_counter()
        print("")
        print(f"Loaded {filename} in {end - start:0.4f} seconds")

    def partitioned_file_from_file(self, tablename, filename, is_fact_table):
        import time
        from csv import reader
        parts = filename.split(".")
        extension = parts[-1]
        part_filename = ".".join(parts[:-1]) + "_partitioned" + "." + extension
        write_obj = open(part_filename, 'w')

        start = time.perf_counter()
        with open(filename, 'r') as read_obj:
            csv_reader = reader(read_obj, delimiter='|')
            total = 0
            batch_size = 1000
            batch = ""
            count = 0

            for row in csv_reader:
                if is_fact_table:
                    part = self._get_fact_partition(row)
                else:
                    pk_off, fk_off = self._pk_fk_column_offsets_by_pk_table(tablename)
                    part = self._get_dim_partition(row, fk_off, pk_off)

                batch += ('|'.join(row + [str(part)])) + "\n"
                count += 1

                if count >= batch_size:
                    write_obj.write(batch)
                    batch = ""
                    count = 0
                    total += batch_size
        
        if batch != "":
            write_obj.write(batch)
            total += count
        end = time.perf_counter()
        print(f"Wrote {part_filename} in {end - start:0.4f} seconds")
        return part_filename

    def finish_loading(self):
        print(f"Insert queries took {self.time} seconds accumulated")
        for t in self.tables:
           self._run_update_query(f"Modify {t.name} to combine")
        for i in range(len(self.fks)):
            self._create_fk(i)

    def verify_partitioning(self):
        if not self.trace_partitions:
            print("Tracing was not enabled, cannot verify")
            return
        
        errors = 0
        print("Verifying")
        for map_cnt, map in enumerate(self.mappings):
            for key, part in map.items():
                for part_cnt, p in enumerate(self.partitions[map_cnt]):
                    if part != self.EXCEPTION_MARKER and part_cnt != part:
                        if key in p:
                            print(f"Found key {key} in partition {part_cnt} although it should be in {part} (Mapping {map_cnt})")
                            errors += 1
        if errors == 0:
            print("Verified")
        else:
            print("fNot verified, {errors} Errors")
    
    def print_statistics(self):
        if not self.trace_partitions:
            print("Tracing was not enabled, cannot gather statistics")
            return

        for part in range(self.num_partitions):
            line = self.key_map + [part] + [len(self.partitions[0][part])]
            for key_idx, partitioning in enumerate(self.partitions):
                exceptions = 0
                for value in partitioning[part]:
                    if self.mappings[key_idx][value] == -1:
                        exceptions += 1
                exp_perc = None
                if len(partitioning[part]) > 0:
                    exp_perc = exceptions/len(partitioning[part])*100
                line.append(exp_perc)
            strings = [str(round(x,2)) if x is not None else "NaN" for x in line] 
            print("|".join(strings))
