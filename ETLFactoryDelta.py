import time
from typing import Union, Dict, List, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, col, lit
import time
import logging
from typing import Union, Dict, List, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, col, lit, current_timestamp, when, uuid, rand
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# Conditional import for Delta Table to avoid errors if library is missing in non-delta envs
try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None

    
class TableInputNode:
    def __init__(self, sql: str = None, connection: str = None, properties: dict = None, **kwargs):
        """
        :param sql: SQL query string. Can be top-level key or inside properties['step']['sql'].
        """
        # Handle sql location variance in JSON
        self.sql = sql
        if not self.sql and properties and 'step' in properties:
            step_props = properties.get('step', {})
            self.sql = step_props.get('sql', [None])[0] if isinstance(step_props.get('sql'), list) else step_props.get('sql')

        if not self.sql:
            # Fallback for nodes that might act as dummy inputs or missing SQL
            self.sql = "SELECT 1" 
            
        self.connection = connection

    def run(self, spark: SparkSession, input_df: DataFrame = None) -> DataFrame:
        # Resolve variables in SQL if present (Basic resolution)
        # Real implementation would need a full variable resolver from context
        return spark.sql(self.sql)


class TableOutputNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.table = None
        self.truncate = False
        
        if properties and 'step' in properties:
            step = properties['step']
            # Parse table name (often a list in JSON)
            raw_table = step.get('table')
            self.table = raw_table[0] if isinstance(raw_table, list) else raw_table
            
            # Parse truncate flag
            raw_trunc = step.get('truncate')
            trunc_val = raw_trunc[0] if isinstance(raw_trunc, list) else raw_trunc
            self.truncate = (trunc_val == 'Y')
            
        if not self.table:
            # Fallback if table not explicitly found, might be derived
            pass

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        if not self.table:
            raise ValueError("TableOutputNode requires 'table' name in properties.")
            
        writer = input_df.write.format("delta")
        
        if self.truncate:
            writer = writer.mode("overwrite")
        else:
            writer = writer.mode("append")
            
        writer.saveAsTable(self.table)
        
        # Return input_df to allow chain continuation if needed
        return input_df

from pyspark.sql.functions import expr

class FilterRowsNode:
    def __init__(self, properties=None, send_true_to=None, send_false_to=None, condition=None, **kwargs):
        self.send_true_to = send_true_to
        self.send_false_to = send_false_to
        self.condition_str = None
        self.conditions_list = None

        # Get send_true_to / send_false_to from properties (Pentaho style)
        if properties and 'step' in properties:
            step = properties['step']
            
            # send_true_to
            stt = step.get('send_true_to')
            if stt:
                self.send_true_to = stt[0] if isinstance(stt, list) else stt
            
            # send_false_to
            sft = step.get('send_false_to')
            if sft:
                self.send_false_to = sft[0] if isinstance(sft, list) else sft

            # Store the conditions
            if 'compare' in step and 'condition' in step['compare']:
                self.conditions_list = step['compare']['condition']

        # Allow override via constructor argument (your pipeline passes condition=...)
        if condition is not None:
            self.conditions_list = condition

    @staticmethod
    def _pentaho_condition_to_sql(conditions):
        """Convert list of Pentaho condition dicts to Spark SQL string"""
        if not conditions:
            return "TRUE"  # or raise error - depends on your needs

        sql_parts = []

        for cond in conditions:
            negated = cond.get("negated", ["N"])[0] == "Y"
            field = cond["leftvalue"][0]
            operator = cond["function"][0]
            value = cond["rightvalue"][0]

            # Handle quoting for non-numeric values
            if not str(value).replace(".", "", 1).replace("-", "", 1).isdigit():
                value = f"'{value}'"

            clause = f"{field} {operator} {value}"

            if negated:
                clause = f"NOT ({clause})"

            sql_parts.append(clause)

        return " AND ".join(sql_parts)

    def run(self, spark, input_df):
        if input_df is None:
            raise ValueError("FilterRowsNode received None as input_df")

        if self.conditions_list is None:
            raise ValueError("No filter conditions provided")

        # Build SQL condition string once
        if self.condition_str is None:
            self.condition_str = self._pentaho_condition_to_sql(self.conditions_list)

        # Apply the filter
        df_true = input_df.filter(expr(self.condition_str))

        # If there's a false branch, also compute the complement
        if self.send_false_to:
            df_false = input_df.filter(expr(f"NOT ({self.condition_str})"))
            return {
                self.send_true_to: df_true,
                self.send_false_to: df_false
            }

        # Normal case: only true branch
        return df_true
        
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

class SortRowsNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.sort_fields = []
        
        if properties and 'step' in properties:
            step = properties['step']
            fields = step.get('fields', {})
            
            # Case 1: Your current format (most common in your pipeline)
            if isinstance(fields, dict) and 'fieldname' in fields:
                field_name = fields['fieldname']
                ascending = fields.get('ascending', 'Y') == 'Y'   # convert to boolean
                if field_name:
                    self.sort_fields.append((field_name, ascending))
            
            # # Case 2: Support classic Pentaho nested format (optional, for future compatibility)
            # elif isinstance(fields, dict) and 'field' in fields:
            #     field_list = fields['field']
            #     if not isinstance(field_list, list):
            #         field_list = [field_list]
                    
            #     for f in field_list:
            #         if isinstance(f, dict):
            #             name_list = f.get('name', [None])
            #             asc_list = f.get('ascending', ['Y'])
            #             fname = name_list[0] if name_list else None
            #             asc = asc_list[0] == 'Y' if asc_list else True
            #             if fname:
            #                 self.sort_fields.append((fname, asc))
            
            # # Case 3: If fields is already a list (rare)
            # elif isinstance(fields, list):
            #     for f in fields:
            #         if isinstance(f, dict) and 'fieldname' in f:
            #             fname = f['fieldname']
            #             asc = f.get('ascending', 'Y') == 'Y'
            #             if fname:
            #                 self.sort_fields.append((fname, asc))

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        if not self.sort_fields:
            return input_df
            
        sort_cols = []
        for field_name, ascending in self.sort_fields:
            if ascending:
                sort_cols.append(col(field_name).asc())
            else:
                sort_cols.append(col(field_name).desc())
                
        return input_df.orderBy(*sort_cols)

class SET_VARIABLESNode:
    """
    Handles 'SET_VARIABLES' type. Sets variables from a fixed configuration (e.g. at job start).
    """
    def __init__(self, properties: dict = None, **kwargs):
        self.variables = {}
        if properties:
            # Check 'entry' or 'step'
            root = properties.get('entry') or properties.get('step')
            if root:
                fields = root.get('fields', [])
                if isinstance(fields, list) and fields:
                    item_list = fields[0].get('field', [])
                    for f in item_list:
                        name = f.get('variable_name', [None])[0]
                        value = f.get('variable_value', [None])[0]
                        if name:
                            self.variables[name] = value

    def run(self, spark: SparkSession, input_df: DataFrame = None) -> DataFrame:
        # Set variables in Spark Configuration
        for k, v in self.variables.items():
            if v is not None:
                # Resolve internal variables if present
                spark.conf.set(k, v)
        
        return input_df if input_df is not None else spark.createDataFrame([], [])


class WriteToLogNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.log_level = 'Basic'
        self.fields = []
        
        if properties and 'step' in properties:
            step = properties['step']
            self.log_level = step.get('loglevel', ['Basic'])[0]
            
            # Extract fields to log
            fields_data = step.get('fields', [])
            if fields_data and 'field' in fields_data[0]:
                for f in fields_data[0]['field']:
                    self.fields.append(f['name'][0])

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        # PySpark is lazy. We cannot easily print per-row logs without an action.
        # We will print to the driver based on the dataframe schema/metadata.
        print(f"--- WriteToLog ({self.log_level}) ---")
        print(f"Logging fields: {self.fields}")
        return input_df

class SIMPLE_EVALNode:
    """
    Handles 'SIMPLE_EVAL' (Simple Evaluation) Job Entry.
    Evaluates a variable against a value or condition.
    """
    def __init__(self, properties: dict = None, **kwargs):
        self.variable_name = None
        self.compare_value = None
        self.success_condition = 'equal'
        
        if properties and 'entry' in properties:
            entry = properties['entry']
            self.variable_name = entry.get('variablename', [None])[0]
            self.compare_value = entry.get('comparevalue', [None])[0]
            self.success_condition = entry.get('successcondition', ['equal'])[0]

    def run(self, spark: SparkSession, input_df: DataFrame) -> Dict[str, DataFrame]:
        if not self.variable_name:
            # Default to true path if not configured
            return {'true': input_df, 'false': spark.createDataFrame([], input_df.schema)}

        # Clean variable syntax ${VAR} -> VAR
        var_key = self.variable_name.replace('${', '').replace('}', '')
        curr_val = spark.conf.get(var_key, None)
        
        # Compare logic
        is_success = False
        str_curr = str(curr_val) if curr_val is not None else ""
        str_comp = str(self.compare_value) if self.compare_value is not None else ""
        
        if self.success_condition == 'equal':
            is_success = (str_curr == str_comp)
        elif self.success_condition == 'different':
            is_success = (str_curr != str_comp)
        # Add more conditions as needed
        else:
            is_success = (str_curr == str_comp)

        empty_df = spark.createDataFrame([], input_df.schema)
        
        # Return dict for branching flow
        if is_success:
            return {'true': input_df, 'false': empty_df}
        else:
            return {'true': empty_df, 'false': input_df}


class RandomValueNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.fields = []
        if properties and 'step' in properties:
            f_list = properties['step'].get('fields', [])
            if f_list:
                for f in f_list[0].get('field', []):
                    name = f.get('name', [None])[0]
                    ftype = f.get('type', [None])[0]
                    self.fields.append((name, ftype))

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        for name, ftype in self.fields:
            if ftype and 'uuid' in ftype.lower():
                input_df = input_df.withColumn(name, expr("uuid()"))
            else:
                input_df = input_df.withColumn(name, expr("rand()"))
        return input_df


class SystemInfoNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.fields = []
        if properties and 'step' in properties:
            f_list = properties['step'].get('fields', [])
            if f_list:
                for f in f_list[0].get('field', []):
                    name = f.get('name', [None])[0]
                    self.fields.append(name)

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        for name in self.fields:
            input_df = input_df.withColumn(name, current_timestamp())
        return input_df


class GetVariableNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.var_map = {}
        if properties and 'step' in properties:
            f_list = properties['step'].get('fields', [])
            if f_list:
                for f in f_list[0].get('field', []):
                    name = f.get('name', [None])[0]
                    var_str = f.get('variable', [None])[0]
                    if name and var_str:
                        self.var_map[name] = var_str

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        for col_name, var_raw in self.var_map.items():
            # Remove Pentaho syntax
            key = var_raw.replace('${', '').replace('}', '')
            # Get from conf, default to null string if missing
            val = spark.conf.get(key, None)
            input_df = input_df.withColumn(col_name, lit(val))
        return input_df


class InsertUpdateNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.connection = None
        self.schema = None
        self.table = None
        self.lookup_keys = []
        self.update_fields = []
        
        if properties and 'step' in properties:
            step = properties['step']
            self.connection = step.get('connection', [None])[0]
            
            # Identify target table
            lookup_cfg = step.get('lookup', [])
            if lookup_cfg:
                lk = lookup_cfg[0]
                self.schema = lk.get('schema', [None])[0]
                self.table = lk.get('table', [None])[0]
                
                # Keys for matching
                for k in lk.get('key', []):
                    self.lookup_keys.append({
                        'stream': k.get('name', [None])[0],
                        'table': k.get('field', [None])[0],
                        'cond': k.get('condition', ['='])[0]
                    })
                
                # Values to insert/update
                for v in lk.get('value', []):
                    self.update_fields.append({
                        'table': v.get('name', [None])[0],
                        'stream': v.get('rename', [None])[0],
                        'do_update': v.get('update', ['Y'])[0] == 'Y'
                    })

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        """
        Performs a Delta Lake Merge.
        """
        full_table_name = f"{self.schema}.{self.table}" if self.schema else self.table
        
        if DeltaTable is None:
            raise ImportError("DeltaTable library is required for InsertUpdateNode but not found.")

        # Build condition string: target.col = source.col
        condition_parts = []
        for k in self.lookup_keys:
            # Handle standard equality
            op = k['cond'] if k['cond'] == '=' else '==' # Spark SQL syntax
            condition_parts.append(f"target.{k['table']} {op} source.{k['stream']}")
        
        condition = " AND ".join(condition_parts)
        
        # Build Update/Insert maps
        update_map = { f"target.{f['table']}": f"source.{f['stream']}" for f in self.update_fields if f['do_update'] }
        insert_map = { f"target.{f['table']}": f"source.{f['stream']}" for f in self.update_fields }

        if DeltaTable.isDeltaTable(spark, full_table_name):
            delta_table = DeltaTable.forName(spark, full_table_name)
            
            merge_builder = delta_table.alias("target").merge(
                input_df.alias("source"),
                condition
            )
            
            if update_map:
                merge_builder = merge_builder.whenMatchedUpdate(set=update_map)
            
            merge_builder = merge_builder.whenNotMatchedInsert(values=insert_map)
            merge_builder.execute()
        else:
            # Fallback if table doesn't exist, just save it (create)
            input_df.write.format("delta").mode("append").saveAsTable(full_table_name)

        return input_df


class TypeExitExcelWriterStepNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.path = None
        self.sheet = "Sheet1"
        if properties and 'step' in properties:
            step = properties['step']
            files = step.get('file', [])
            if files:
                f = files[0]
                name = f.get('name', [''])[0]
                ext = f.get('extention', ['xlsx'])[0]
                self.path = f"{name}.{ext}"
                self.sheet = f.get('sheetname', ['Sheet1'])[0]

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        if self.path:
            # Attempt to write using spark-excel if available, otherwise fallback or error gracefully
            try:
                input_df.write \
                    .format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("dataAddress", f"'{self.sheet}'!A1") \
                    .mode("overwrite") \
                    .save(self.path)
            except Exception as e:
                # If the specific Excel library is not loaded, fallback to CSV with a warning
                print(f"Warning: Excel writer failed (Library missing?). Writing as CSV to {self.path}. Error: {e}")
                input_df.write.csv(self.path, header=True, mode='overwrite')
        
        return input_df


class StepsMetricsNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.metric_cols = {}
        if properties and 'step' in properties:
            step = properties['step']
            # Map Pentaho internal metric field names to output columns
            fields = ['stepnamefield', 'stepidfield', 'steplinesinputfield', 'steplinesoutputfield', 
                    'steplinesreadfield', 'steplinesupdatedfield', 'steplineswrittentfield', 
                    'steplineserrorsfield', 'stepsecondsfield']
            for f in fields:
                val = step.get(f)
                if val:
                    self.metric_cols[f] = val[0]

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        # Generates a DataFrame matching the schema of requested metrics.
        # In a real Spark engine, these would come from the ListenerBus.
        # Here we provide a compliant 1-row DataFrame.
        selects = []
        for k, col_name in self.metric_cols.items():
            if 'name' in k:
                selects.append(lit("UnknownStep").alias(col_name))
            else:
                selects.append(lit(0).alias(col_name))
        
        if selects:
            return spark.createDataFrame([{}], schema=StructType([])).select(*selects)
        return input_df


class PGBulkLoaderNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.schema = None
        self.table = None
        self.mode = 'append'
        
        if properties and 'step' in properties:
            step = properties['step']
            self.schema = step.get('schema', [None])[0]
            self.table = step.get('table', [None])[0]
            action = step.get('load_action', ['INSERT'])[0]
            if action == 'TRUNCATE':
                self.mode = 'overwrite'
            else:
                self.mode = 'append'

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        full_table = f"{self.schema}.{self.table}" if self.schema else self.table
        
        # Write using JDBC format (Assuming Postgres Driver is loaded in Spark)
        # Credentials should be handled via Spark Conf or Options in a real env
        try:
            input_df.write \
                .format("jdbc") \
                .option("dbtable", full_table) \
                .mode(self.mode) \
                .save()
        except Exception as e:
            print(f"PGBulkLoaderNode: JDBC save attempted. ensure DB configs are set. Error: {e}")
            
        return input_df


class ScriptValueModNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.rename_fields = []
        if properties and 'step' in properties:
            # Check if there are simple renames/selects in fields
            f_list = properties['step'].get('fields', [])
            if f_list:
                for f in f_list[0].get('field', []):
                    name = f.get('name', [None])[0]
                    rename = f.get('rename', [None])[0]
                    if name and rename and name != rename:
                        self.rename_fields.append((name, rename))
                        
    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        # Perform field mapping
        for name, rename in self.rename_fields:
            input_df = input_df.withColumnRenamed(name, rename)
            
        return input_df


class SelectValuesNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.select_fields = []
        self.remove_fields = []
        self.meta_fields = []

        if properties and 'step' in properties:
            step = properties['step']
            fields = step.get('fields', [])
            if fields:
                # Select/Rename
                f_data = fields[0].get('field', [])
                for f in f_data:
                    name = f.get('name', [None])[0]
                    rename = f.get('rename', [None])[0]
                    if name:
                        self.select_fields.append((name, rename))
                
                # Remove
                r_data = fields[0].get('remove', [])
                for f in r_data:
                    self.remove_fields.append(f.get('name', [None])[0])

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        # 1. Select and Rename
        if self.select_fields:
            cols = []
            for name, rename in self.select_fields:
                if rename:
                    cols.append(col(name).alias(rename))
                else:
                    cols.append(col(name))
            input_df = input_df.select(*cols)

        # 2. Remove fields
        if self.remove_fields:
            input_df = input_df.drop(*self.remove_fields)
            
        return input_df


class SetVariableNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.mapping = []
        if properties and 'step' in properties:
            f_list = properties['step'].get('fields', [])
            if f_list:
                for f in f_list[0].get('field', []):
                    field_name = f.get('field_name', [None])[0]
                    var_name = f.get('variable_name', [None])[0]
                    if field_name and var_name:
                        self.mapping.append((field_name, var_name))

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        # Requires materialization to read data into Driver
        if input_df and self.mapping:
            try:
                row = input_df.first()
                if row:
                    for field, var in self.mapping:
                        if field in row:
                            val = str(row[field])
                            spark.conf.set(var, val)
            except Exception as e:
                print(f"SetVariableNode Error: {e}")
                
        return input_df


class SQLNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.sql_script = None
        
        # Check 'entry' (Job) or 'step' (Trans)
        root = properties.get('entry') or properties.get('step')
        
        if root:
            # Direct SQL field or 'sql' property
            self.sql_script = root.get('sql')
            if isinstance(self.sql_script, list):
                self.sql_script = self.sql_script[0]
            
            # Allow fallback to top-level property
            if not self.sql_script:
                self.sql_script = properties.get('sql')

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        if self.sql_script:
            # Split by semicolon for multiple statements
            statements = [s.strip() for s in self.sql_script.split(';') if s.strip()]
            for stmt in statements:
                try:
                    spark.sql(stmt)
                except Exception as e:
                    print(f"SQLNode Execution Error: {e}")
                    
        return input_df


# class SortRowsNode:
#     def __init__(self, properties: dict = None, **kwargs):
#         self.sort_fields = []
        
#         if properties and 'step' in properties:
#             fields = properties['step'].get('fields', {})
#             # Fields can be a dict (single field) or list of dicts
#             if isinstance(fields, dict):
#                 fields = [fields]
            
#             for f in fields:
#                 if isinstance(f, dict) and 'field' in f:
#                     # Nested field structure handling
#                     f_list = f['field']
#                     for sub_f in f_list:
#                         fname = sub_f.get('name', [None])[0]
#                         asc = sub_f.get('ascending', ['Y'])[0]
#                         if fname: self.sort_fields.append((fname, asc))

#     def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
#         if not self.sort_fields:
#             return input_df
            
#         cols_to_sort = []
#         for fname, asc in self.sort_fields:
#             if asc == 'Y':
#                 cols_to_sort.append(col(fname).asc())
#             else:
#                 cols_to_sort.append(col(fname).desc())
                
#         return input_df.orderBy(*cols_to_sort)


class SET_VARIABLESNode:
    """
    Handles 'SET_VARIABLES' type. Sets variables from a fixed configuration (e.g. at job start).
    """
    def __init__(self, properties: dict = None, **kwargs):
        self.variables = {}
        if properties:
            # Check 'entry' or 'step'
            root = properties.get('entry') or properties.get('step')
            if root:
                fields = root.get('fields', [])
                if isinstance(fields, list) and fields:
                    item_list = fields[0].get('field', [])
                    for f in item_list:
                        name = f.get('variable_name', [None])[0]
                        value = f.get('variable_value', [None])[0]
                        if name:
                            self.variables[name] = value

    def run(self, spark: SparkSession, input_df: DataFrame = None) -> DataFrame:
        # Set variables in Spark Configuration
        for k, v in self.variables.items():
            if v is not None:
                # Resolve internal variables if present
                spark.conf.set(k, v)
        
        return input_df if input_df is not None else spark.createDataFrame([], [])


class WriteToLogNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.log_level = 'Basic'
        self.fields = []
        
        if properties and 'step' in properties:
            step = properties['step']
            self.log_level = step.get('loglevel', ['Basic'])[0]
            
            # Extract fields to log
            fields_data = step.get('fields', [])
            if fields_data and 'field' in fields_data[0]:
                for f in fields_data[0]['field']:
                    self.fields.append(f['name'][0])

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        # PySpark is lazy. We cannot easily print per-row logs without an action.
        # We will print to the driver based on the dataframe schema/metadata.
        print(f"--- WriteToLog ({self.log_level}) ---")
        print(f"Logging fields: {self.fields}")
        return input_df


# class BlockUntilStepsFinishNode:
#     def __init__(self, properties: dict = None, **kwargs):
#         self.steps_to_wait = []
#         if properties and 'step' in properties:
#             step_info = properties['step'].get('steps', [])
#             if step_info:
#                 for s in step_info[0].get('step', []):
#                     self.steps_to_wait.append(s.get('name', [None])[0])

#     def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
#         # Simulate blocking by forcing materialization of previous steps if necessary
#         # In a real DAG, the orchestrator handles this dependency.
#         time.sleep(1) 
#         return input_df
from pyspark.sql import SparkSession, DataFrame
import time

class BlockUntilStepsFinishNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.steps_to_wait = []
        if properties and 'step' in properties:
            step_info = properties['step'].get('steps', [])
            if step_info and 'step' in step_info[0]:
                for s in step_info[0]['step']:
                    name = s.get('name', [None])[0]
                    if name:
                        self.steps_to_wait.append(name)

    def run(self, spark: SparkSession, input_df: DataFrame, node_outputs: dict, poll_interval: float = 3.0, timeout: int = 900) -> DataFrame:
        if not self.steps_to_wait:
            return input_df if input_df is not None else spark.createDataFrame([], "dummy STRING")

        start_time = time.time()
        waited_names = self.steps_to_wait  # usually just one in your case

        print(f"[BLOCK] Waiting for: {waited_names}")

        while True:
            all_done = True

            for target_name in waited_names:
                found = False
                for key, df_result in node_outputs.items():
                    if target_name in key:
                        if df_result is not None:
                            print(f"[BLOCK] {key} completed → good")
                        else:
                            raise RuntimeError(f"Waited-for step failed: {key}")
                        found = True
                        break

                if not found:
                    all_done = False
                    print(f"[BLOCK] Still waiting for '{target_name}'...")

            if all_done:
                elapsed = time.time() - start_time
                print(f"[BLOCK] All conditions satisfied after {elapsed:.1f} seconds")
                return input_df if input_df is not None else spark.createDataFrame([], "dummy STRING")

            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for {waited_names}")

            time.sleep(poll_interval)

class SIMPLE_EVALNode:
    """
    Handles 'SIMPLE_EVAL' (Simple Evaluation) Job Entry.
    Evaluates a variable against a value or condition.
    """
    def __init__(self, properties: dict = None, **kwargs):
        self.variable_name = None
        self.compare_value = None
        self.success_condition = 'equal'
        
        if properties and 'entry' in properties:
            entry = properties['entry']
            self.variable_name = entry.get('variablename', [None])[0]
            self.compare_value = entry.get('comparevalue', [None])[0]
            self.success_condition = entry.get('successcondition', ['equal'])[0]

    def run(self, spark: SparkSession, input_df: DataFrame) -> Dict[str, DataFrame]:
        if not self.variable_name:
            # Default to true path if not configured
            return {'true': input_df, 'false': spark.createDataFrame([], input_df.schema)}

        # Clean variable syntax ${VAR} -> VAR
        var_key = self.variable_name.replace('${', '').replace('}', '')
        curr_val = spark.conf.get(var_key, None)
        
        # Compare logic
        is_success = False
        str_curr = str(curr_val) if curr_val is not None else ""
        str_comp = str(self.compare_value) if self.compare_value is not None else ""
        
        if self.success_condition == 'equal':
            is_success = (str_curr == str_comp)
        elif self.success_condition == 'different':
            is_success = (str_curr != str_comp)
        # Add more conditions as needed
        else:
            is_success = (str_curr == str_comp)

        empty_df = spark.createDataFrame([], input_df.schema)
        
        # Return dict for branching flow
        if is_success:
            return {'true': input_df, 'false': empty_df}
        else:
            return {'true': empty_df, 'false': input_df}


class RandomValueNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.fields = []
        if properties and 'step' in properties:
            f_list = properties['step'].get('fields', [])
            if f_list:
                for f in f_list[0].get('field', []):
                    name = f.get('name', [None])[0]
                    ftype = f.get('type', [None])[0]
                    self.fields.append((name, ftype))

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        for name, ftype in self.fields:
            if ftype and 'uuid' in ftype.lower():
                input_df = input_df.withColumn(name, expr("uuid()"))
            else:
                input_df = input_df.withColumn(name, expr("rand()"))
        return input_df


class SystemInfoNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.fields = []
        if properties and 'step' in properties:
            f_list = properties['step'].get('fields', [])
            if f_list:
                for f in f_list[0].get('field', []):
                    name = f.get('name', [None])[0]
                    self.fields.append(name)

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        for name in self.fields:
            input_df = input_df.withColumn(name, current_timestamp())
        return input_df


class GetVariableNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.var_map = {}
        if properties and 'step' in properties:
            f_list = properties['step'].get('fields', [])
            if f_list:
                for f in f_list[0].get('field', []):
                    name = f.get('name', [None])[0]
                    var_str = f.get('variable', [None])[0]
                    if name and var_str:
                        self.var_map[name] = var_str

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        for col_name, var_raw in self.var_map.items():
            # Remove Pentaho syntax
            key = var_raw.replace('${', '').replace('}', '')
            # Get from conf, default to null string if missing
            val = spark.conf.get(key, None)
            input_df = input_df.withColumn(col_name, lit(val))
        return input_df


class InsertUpdateNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.connection = None
        self.schema = None
        self.table = None
        self.lookup_keys = []
        self.update_fields = []
        
        if properties and 'step' in properties:
            step = properties['step']
            self.connection = step.get('connection', [None])[0]
            
            # Identify target table
            lookup_cfg = step.get('lookup', [])
            if lookup_cfg:
                lk = lookup_cfg[0]
                self.schema = lk.get('schema', [None])[0]
                self.table = lk.get('table', [None])[0]
                
                # Keys for matching
                for k in lk.get('key', []):
                    self.lookup_keys.append({
                        'stream': k.get('name', [None])[0],
                        'table': k.get('field', [None])[0],
                        'cond': k.get('condition', ['='])[0]
                    })
                
                # Values to insert/update
                for v in lk.get('value', []):
                    self.update_fields.append({
                        'table': v.get('name', [None])[0],
                        'stream': v.get('rename', [None])[0],
                        'do_update': v.get('update', ['Y'])[0] == 'Y'
                    })

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        """
        Performs a Delta Lake Merge.
        """
        full_table_name = f"{self.schema}.{self.table}" if self.schema else self.table
        
        if DeltaTable is None:
            raise ImportError("DeltaTable library is required for InsertUpdateNode but not found.")

        # Build condition string: target.col = source.col
        condition_parts = []
        for k in self.lookup_keys:
            # Handle standard equality
            op = k['cond'] if k['cond'] == '=' else '==' # Spark SQL syntax
            condition_parts.append(f"target.{k['table']} {op} source.{k['stream']}")
        
        condition = " AND ".join(condition_parts)
        
        # Build Update/Insert maps
        update_map = { f"target.{f['table']}": f"source.{f['stream']}" for f in self.update_fields if f['do_update'] }
        insert_map = { f"target.{f['table']}": f"source.{f['stream']}" for f in self.update_fields }

        if DeltaTable.isDeltaTable(spark, full_table_name):
            delta_table = DeltaTable.forName(spark, full_table_name)
            
            merge_builder = delta_table.alias("target").merge(
                input_df.alias("source"),
                condition
            )
            
            if update_map:
                merge_builder = merge_builder.whenMatchedUpdate(set=update_map)
            
            merge_builder = merge_builder.whenNotMatchedInsert(values=insert_map)
            merge_builder.execute()
        else:
            # Fallback if table doesn't exist, just save it (create)
            input_df.write.format("delta").mode("append").saveAsTable(full_table_name)

        return input_df


class TypeExitExcelWriterStepNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.path = None
        self.sheet = "Sheet1"
        if properties and 'step' in properties:
            step = properties['step']
            files = step.get('file', [])
            if files:
                f = files[0]
                name = f.get('name', [''])[0]
                ext = f.get('extention', ['xlsx'])[0]
                self.path = f"{name}.{ext}"
                self.sheet = f.get('sheetname', ['Sheet1'])[0]

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        if self.path:
            # Attempt to write using spark-excel if available, otherwise fallback or error gracefully
            try:
                input_df.write \
                    .format("com.crealytics.spark.excel") \
                    .option("header", "true") \
                    .option("dataAddress", f"'{self.sheet}'!A1") \
                    .mode("overwrite") \
                    .save(self.path)
            except Exception as e:
                # If the specific Excel library is not loaded, fallback to CSV with a warning
                print(f"Warning: Excel writer failed (Library missing?). Writing as CSV to {self.path}. Error: {e}")
                input_df.write.csv(self.path, header=True, mode='overwrite')
        
        return input_df


class StepsMetricsNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.metric_cols = {}
        if properties and 'step' in properties:
            step = properties['step']
            # Map Pentaho internal metric field names to output columns
            fields = ['stepnamefield', 'stepidfield', 'steplinesinputfield', 'steplinesoutputfield', 
                    'steplinesreadfield', 'steplinesupdatedfield', 'steplineswrittentfield', 
                    'steplineserrorsfield', 'stepsecondsfield']
            for f in fields:
                val = step.get(f)
                if val:
                    self.metric_cols[f] = val[0]

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        # Generates a DataFrame matching the schema of requested metrics.
        # In a real Spark engine, these would come from the ListenerBus.
        # Here we provide a compliant 1-row DataFrame.
        selects = []
        for k, col_name in self.metric_cols.items():
            if 'name' in k:
                selects.append(lit("UnknownStep").alias(col_name))
            else:
                selects.append(lit(0).alias(col_name))
        
        if selects:
            return spark.createDataFrame([{}], schema=StructType([])).select(*selects)
        return input_df


class PGBulkLoaderNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.schema = None
        self.table = None
        self.mode = 'append'
        
        if properties and 'step' in properties:
            step = properties['step']
            self.schema = step.get('schema', [None])[0]
            self.table = step.get('table', [None])[0]
            action = step.get('load_action', ['INSERT'])[0]
            if action == 'TRUNCATE':
                self.mode = 'overwrite'
            else:
                self.mode = 'append'

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        full_table = f"{self.schema}.{self.table}" if self.schema else self.table
        
        # Write using JDBC format (Assuming Postgres Driver is loaded in Spark)
        # Credentials should be handled via Spark Conf or Options in a real env
        try:
            input_df.write \
                .format("jdbc") \
                .option("dbtable", full_table) \
                .mode(self.mode) \
                .save()
        except Exception as e:
            print(f"PGBulkLoaderNode: JDBC save attempted. ensure DB configs are set. Error: {e}")
            
        return input_df


class ScriptValueModNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.rename_fields = []
        if properties and 'step' in properties:
            # Check if there are simple renames/selects in fields
            f_list = properties['step'].get('fields', [])
            if f_list:
                for f in f_list[0].get('field', []):
                    name = f.get('name', [None])[0]
                    rename = f.get('rename', [None])[0]
                    if name and rename and name != rename:
                        self.rename_fields.append((name, rename))
                        
    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        # Perform field mapping
        for name, rename in self.rename_fields:
            input_df = input_df.withColumnRenamed(name, rename)
            
        return input_df


class SelectValuesNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.select_fields = []
        self.remove_fields = []
        self.meta_fields = []

        if properties and 'step' in properties:
            step = properties['step']
            fields = step.get('fields', [])
            if fields:
                # Select/Rename
                f_data = fields[0].get('field', [])
                for f in f_data:
                    name = f.get('name', [None])[0]
                    rename = f.get('rename', [None])[0]
                    if name:
                        self.select_fields.append((name, rename))
                
                # Remove
                r_data = fields[0].get('remove', [])
                for f in r_data:
                    self.remove_fields.append(f.get('name', [None])[0])

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        # 1. Select and Rename
        if self.select_fields:
            cols = []
            for name, rename in self.select_fields:
                if rename:
                    cols.append(col(name).alias(rename))
                else:
                    cols.append(col(name))
            input_df = input_df.select(*cols)

        # 2. Remove fields
        if self.remove_fields:
            input_df = input_df.drop(*self.remove_fields)
            
        return input_df


class SetVariableNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.mapping = []
        if properties and 'step' in properties:
            f_list = properties['step'].get('fields', [])
            if f_list:
                for f in f_list[0].get('field', []):
                    field_name = f.get('field_name', [None])[0]
                    var_name = f.get('variable_name', [None])[0]
                    if field_name and var_name:
                        self.mapping.append((field_name, var_name))

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        # Requires materialization to read data into Driver
        if input_df and self.mapping:
            try:
                row = input_df.first()
                if row:
                    for field, var in self.mapping:
                        if field in row:
                            val = str(row[field])
                            spark.conf.set(var, val)
            except Exception as e:
                print(f"SetVariableNode Error: {e}")
                
        return input_df


class SQLNode:
    def __init__(self, properties: dict = None, **kwargs):
        self.sql_script = None
        
        # Check 'entry' (Job) or 'step' (Trans)
        root = properties.get('entry') or properties.get('step')
        
        if root:
            # Direct SQL field or 'sql' property
            self.sql_script = root.get('sql')
            if isinstance(self.sql_script, list):
                self.sql_script = self.sql_script[0]
            
            # Allow fallback to top-level property
            if not self.sql_script:
                self.sql_script = properties.get('sql')

    def run(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        if self.sql_script:
            # Split by semicolon for multiple statements
            statements = [s.strip() for s in self.sql_script.split(';') if s.strip()]
            for stmt in statements:
                try:
                    spark.sql(stmt)
                except Exception as e:
                    print(f"SQLNode Execution Error: {e}")
                    
        return input_df