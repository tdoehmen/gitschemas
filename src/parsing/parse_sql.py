#%pip install pglast

import os
import json
import pandas as pd
import pglast

def parse_files(paths):
    total = len(file_paths)
    err = 0

    metadata = {}
    for i in range(0, total):
        if i % 10 == 0:
            print(f"{i}/{total} ({err} errors)", end = "\r")
        try:
            f = open(path+file_paths[i], encoding='utf-8')
            query = f.read()
            f.close()
            statement = pglast.parser.parse_sql(query)

            dataset = file_paths[i]
            metadata[dataset] = {}
            metadata[dataset]["INFO"] = {}
            metadata[dataset]["TABLES"] = {}

            for s in statement:
                if type(s.stmt) == pglast.ast.CreateStmt:
                    table = s.stmt.relation.relname

                    metadata[dataset]["TABLES"][table] = {}
                    metadata[dataset]["TABLES"][table]["COLUMNS"] = []
                    metadata[dataset]["TABLES"][table]["PRIMARY_KEYS"] = []
                    metadata[dataset]["TABLES"][table]["FOREIGN_KEYS"] = []
                    for token in s.stmt.tableElts:
                        if type(token) == pglast.ast.ColumnDef:
                            col = token.colname
                            metadata[dataset]["TABLES"][table]["COLUMNS"].append([col, token.typeName.names[-1].val])
                        elif type(token) == pglast.ast.Constraint:
                            if token.contype == pglast.enums.parsenodes.ConstrType.CONSTR_PRIMARY:
                                pks = [k.val for k in token.keys]
                                metadata[dataset]["TABLES"][table]["PRIMARY_KEYS"] = pks
                            if token.contype == pglast.enums.parsenodes.ConstrType.CONSTR_FOREIGN:
                                fks = [k.val for k in token.fk_attrs]
                                rt = token.pktable.relname
                                rk = [k.val for k in token.pk_attrs]
                                metadata[dataset]["TABLES"][table]["FOREIGN_KEYS"].append({'FOREIGN_KEY': fks,
                                                                                           'REFERENCE_TABLE': rt,
                                                                                           'REFERENCE_COLUMN': rk})

        except Exception as e:
            err += 1

    return metadata


def parse_url_info(metadata):
    count = 0
    with open("../../data/sqlfiles_urls.csv", "r") as f:
        while line := f.readline():
            info = line.split("/")
            user = info[3]
            project = info[4]
            filename = info[-1].replace("?raw=true\n","")
            local_filename = line[0:6]+"_"+filename

            if local_filename in metadata:
                filesize = os.path.getsize(path+local_filename)
                metadata[local_filename]["INFO"]["user"] = user
                metadata[local_filename]["INFO"]["url"] = line[7:]
                metadata[local_filename]["INFO"]["filename"] = filename
                metadata[local_filename]["INFO"]["project"] = project
                metadata[local_filename]["INFO"]["filesize"] = filesize

                count += 1
                if count % 10 == 0:
                    print(f"{count}/{len(metadata)}", end = "\r")

def create_training_dataset(metadata):
    columns = ["dataset", "table_name_a", "table_name_b", "columns_a", "columns_b", "primary_keys_a", "primary_keys_b", "key_a", "key_b"]
    rows = []
    for dataset in metadata.keys():
        for table_name_a in metadata[dataset]["TABLES"].keys():
            columns_a = [col[0] for col in metadata[dataset]["TABLES"][table_name_a]["COLUMNS"]]
            for fks in metadata[dataset]["TABLES"][table_name_a]["FOREIGN_KEYS"]:
                keys_a = fks["FOREIGN_KEY"]
                keys_b = fks["REFERENCE_COLUMN"]
                # skip composite keys
                if len(keys_a) != 1 or len(keys_b) != 1:
                    continue
                key_a = keys_a[0]
                key_b = keys_b[0]
                table_name_b = fks["REFERENCE_TABLE"]
                primary_keys_a = metadata[dataset]["TABLES"][table_name_a]["PRIMARY_KEYS"]
                columns_b = []
                primary_keys_b = []
                if table_name_b in metadata[dataset]["TABLES"]:
                    columns_b = [col[0] for col in metadata[dataset]["TABLES"][table_name_b]["COLUMNS"]]
                    primary_keys_b = metadata[dataset]["TABLES"][table_name_b]["PRIMARY_KEYS"]
                    # skip composite keys
                    if len(primary_keys_a) > 1 or len(primary_keys_b) > 1:
                        continue

                primary_key_a = primary_keys_a[0] if len(primary_keys_a) != 0 else ""
                primary_key_b = primary_keys_b[0] if len(primary_keys_b) != 0 else ""
                row = [dataset, table_name_a, table_name_b, columns_a, columns_b, primary_key_a, primary_key_b, key_a, key_b]
                rows.append(row)

    return pd.DataFrame(rows, columns=columns)

if __name__ == "main":

    path = "../../data/sqlfiles/"
    file_paths = os.listdir(path)
    metadata = parse_files(file_paths)
    parse_url_info(metadata)

    with open(path+'../../data/metadata_postgres.json', 'w') as f:
        json.dump(metadata, f)

    df = create_training_dataset(metadata)
    df.to_csv("../../data/fk_detection_dataset.csv",index=False)
    df.to_parquet("../../data/fk_detection_dataset.parquet")



