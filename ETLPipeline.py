from pyspark.sql import SparkSession
# from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col
import ETLFactoryDelta as factory

builder = SparkSession.builder \
    .appName('Lazy_Delta_ETL') \
    .config('spark.scheduler.mode', 'FAIR') \
    .getOrCreate()
    # .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    # .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

spark = SparkSession.builder.getOrCreate()

import pandas as pd
employees = pd.read_csv(r'C:\Users\Chimata.Charita\Downloads\Trial')
spark_df = spark.createDataFrame(employees)

# register as temp view
spark_df.createOrReplaceTempView("employees")

node_outputs = {}  # node_name → DataFrame or dict
writers = []  # list of (writer, path) tuples
def run_pipeline():
    global node_outputs, writers
    node_outputs.clear()
    print('Starting Lazy Delta Pipeline...')

    # --- Node: TI_READ_EMPLOYEES ---
    try:
        print('Executing (lazy): TI_READ_EMPLOYEES')
        input_df = None
        node_df_TI_READ_EMPLOYEES = factory.TableInputNode(sql='SELECT * FROM employees', connection=['company_db'], properties={'step': {'name': ['Input_employees'], 'type': ['TableInput'], 'description': ['Read raw employees table'], 'distribute': ['N'], 'custom_distribution': [None], 'copies': ['1'], 'partitioning': [{'method': ['none'], 'schema_name': [None]}], 'connection': ['company_db'], 'sql': ['SELECT * FROM employees'], 'limit': ['0'], 'execute_each_row': ['N'], 'lazy_conversion_active': ['N'], 'cached_row_meta_active': ['N']}})
        df_TI_READ_EMPLOYEES = node_df_TI_READ_EMPLOYEES.run(spark, input_df)
        df_TI_READ_EMPLOYEES.cache()
        node_outputs['TI_READ_EMPLOYEES'] = df_TI_READ_EMPLOYEES
    except Exception as e:
        print(f'Failed: TI_READ_EMPLOYEES - {e}')
        node_outputs['TI_READ_EMPLOYEES'] = None
    # --- Node: ORDER_BY_NAME ---
    try:
        print('Executing (lazy): ORDER_BY_NAME')
        parent_output = node_outputs.get('TI_READ_EMPLOYEES', None)
        if isinstance(parent_output, dict):
            input_df = parent_output.get('ORDER_BY_NAME', None)
            # If key missing, log warning (optional)
            if 'ORDER_BY_NAME' not in parent_output:
                print(f"Warning: Key 'ORDER_BY_NAME' not found in dict from 'TI_READ_EMPLOYEES'. Available: " + str(list(parent_output.keys())))
        else:
            input_df = parent_output
        # Auto-detected input for 'ORDER_BY_NAME' from 'TI_READ_EMPLOYEES'
        node_df_ORDER_BY_NAME = factory.SortRowsNode(properties={'step': {'name': ['Sort_employees_by_Name'], 'type': ['SortRows'], 'description': ['Read employees sorted by Name ascending'], 'distribute': ['N'], 'custom_distribution': [None], 'copies': ['1'], 'partitioning': [{'method': ['none'], 'schema_name': [None]}], 'connection': ['company_db'], 'fields': {'fieldname': 'name', 'ascending': 'Y'}, 'limit': ['0'], 'execute_each_row': ['N'], 'lazy_conversion_active': ['N'], 'cached_row_meta_active': ['N']}})
        df_ORDER_BY_NAME = node_df_ORDER_BY_NAME.run(spark, input_df)
        node_outputs['ORDER_BY_NAME'] = df_ORDER_BY_NAME
    except Exception as e:
        print(f'Failed: ORDER_BY_NAME - {e}')
        node_outputs['ORDER_BY_NAME'] = None
    # --- Node: ORDER_BY_SALARY ---
    try:
        print('Executing (lazy): ORDER_BY_SALARY')
        parent_output = node_outputs.get('TI_READ_EMPLOYEES', None)
        if isinstance(parent_output, dict):
            input_df = parent_output.get('ORDER_BY_SALARY', None)
            # If key missing, log warning (optional)
            if 'ORDER_BY_SALARY' not in parent_output:
                print(f"Warning: Key 'ORDER_BY_SALARY' not found in dict from 'TI_READ_EMPLOYEES'. Available: " + str(list(parent_output.keys())))
        else:
            input_df = parent_output
        # Auto-detected input for 'ORDER_BY_SALARY' from 'TI_READ_EMPLOYEES'
        node_df_ORDER_BY_SALARY = factory.SortRowsNode(properties={'step': {'name': ['Sort_employees_by_Salary'], 'type': ['SortRows'], 'description': ['Read employees sorted by Salaries descending'], 'distribute': ['N'], 'custom_distribution': [None], 'copies': ['1'], 'partitioning': [{'method': ['none'], 'schema_name': [None]}], 'connection': ['company_db'], 'fields': {'fieldname': 'salary', 'ascending': 'N'}, 'limit': ['0'], 'execute_each_row': ['N'], 'lazy_conversion_active': ['N'], 'cached_row_meta_active': ['N']}})
        df_ORDER_BY_SALARY = node_df_ORDER_BY_SALARY.run(spark, input_df)
        node_outputs['ORDER_BY_SALARY'] = df_ORDER_BY_SALARY
    except Exception as e:
        print(f'Failed: ORDER_BY_SALARY - {e}')
        node_outputs['ORDER_BY_SALARY'] = None
    # --- Node: FILTER_BY_SALARY_AND_YOJ ---
    try:
        print('Executing (lazy): FILTER_BY_SALARY_AND_YOJ')
        parent_output = node_outputs.get('ORDER_BY_NAME', None)
        if isinstance(parent_output, dict):
            input_df = parent_output.get('FILTER_BY_SALARY_AND_YOJ', None)
            # If key missing, log warning (optional)
            if 'FILTER_BY_SALARY_AND_YOJ' not in parent_output:
                print(f"Warning: Key 'FILTER_BY_SALARY_AND_YOJ' not found in dict from 'ORDER_BY_NAME'. Available: " + str(list(parent_output.keys())))
        else:
            input_df = parent_output
        # Auto-detected input for 'FILTER_BY_SALARY_AND_YOJ' from 'ORDER_BY_NAME'
        node_df_FILTER_BY_SALARY_AND_YOJ = factory.FilterRowsNode(properties={'step': {'name': ['Filter_High_Salary_YOJ'], 'type': ['FilterRows'], 'description': None, 'partitioning': [{'method': ['none'], 'schema_name': [None]}], 'send_true_to': ['employees_filtered'], 'send_false_to': None, 'compare': {'condition': [{'negated': ['N'], 'leftvalue': ['salary'], 'function': ['>='], 'rightvalue': ['50000']}, {'negated': ['N'], 'leftvalue': ['year_of_joining'], 'function': ['>='], 'rightvalue': ['2020']}]}, 'attributes': [None], 'cluster_schema': [None], 'remotesteps': [{'input': [None], 'output': [None]}], 'GUI': [{'xloc': ['240'], 'yloc': ['272'], 'draw': ['Y']}]}}, send_true_to=['employees_filtered'], condition=[{'negated': ['N'], 'leftvalue': ['salary'], 'function': ['>='], 'rightvalue': ['50000']}, {'negated': ['N'], 'leftvalue': ['year_of_joining'], 'function': ['>='], 'rightvalue': ['2020']}])
        df_FILTER_BY_SALARY_AND_YOJ = node_df_FILTER_BY_SALARY_AND_YOJ.run(spark, input_df)
        node_outputs['FILTER_BY_SALARY_AND_YOJ'] = df_FILTER_BY_SALARY_AND_YOJ
    except Exception as e:
        print(f'Failed: FILTER_BY_SALARY_AND_YOJ - {e}')
        node_outputs['FILTER_BY_SALARY_AND_YOJ'] = None
    # --- Node: SET_VAR_COMPANY ---
    try:
        print('Executing (lazy): SET_VAR_COMPANY')
        parent_output = node_outputs.get('ORDER_BY_SALARY', None)
        if isinstance(parent_output, dict):
            input_df = parent_output.get('SET_VAR_COMPANY', None)
            # If key missing, log warning (optional)
            if 'SET_VAR_COMPANY' not in parent_output:
                print(f"Warning: Key 'SET_VAR_COMPANY' not found in dict from 'ORDER_BY_SALARY'. Available: " + str(list(parent_output.keys())))
        else:
            input_df = parent_output
        # Auto-detected input for 'SET_VAR_COMPANY' from 'ORDER_BY_SALARY'
        node_df_SET_VAR_COMPANY = factory.SET_VARIABLESNode(properties={'attributes': {}, 'replacevars': 'Y', 'filename': '', 'file_variable_type': 'JVM', 'fields': [{'variable_name': 'JOB_LEVEL_0', 'variable_value': '1248', 'variable_type': 'ROOT_JOB'}], 'parallel': 'N'})
        df_SET_VAR_COMPANY = node_df_SET_VAR_COMPANY.run(spark, input_df)
        node_outputs['SET_VAR_COMPANY'] = df_SET_VAR_COMPANY
    except Exception as e:
        print(f'Failed: SET_VAR_COMPANY - {e}')
        node_outputs['SET_VAR_COMPANY'] = None
    # --- Node: CHECK_GENDER ---
    try:
        print('Executing (lazy): CHECK_GENDER')
        # Warning: CHECK_GENDER has 2 parents → using first
        parent_output = node_outputs.get('FILTER_BY_SALARY_AND_YOJ', None)
        if isinstance(parent_output, dict):
            input_df = parent_output.get('CHECK_GENDER', None)
        else:
            input_df = parent_output
        node_df_CHECK_GENDER = factory.FilterRowsNode(properties={'step': {'name': ['Check_Female_Employees'], 'type': ['FilterRows'], 'description': None, 'partitioning': [{'method': ['none'], 'schema_name': [None]}], 'send_true_to': ['TO_FEMALE_EMPLOYEES'], 'send_false_to': ['TO_MALE_EMPLOYEES'], 'compare': {'condition': [{'negated': ['N'], 'leftvalue': ['gender'], 'function': ['='], 'rightvalue': ['Female']}]}, 'attributes': [None], 'cluster_schema': [None], 'remotesteps': [{'input': [None], 'output': [None]}], 'GUI': [{'xloc': ['240'], 'yloc': ['272'], 'draw': ['Y']}]}}, send_true_to=['TO_FEMALE_EMPLOYEES'], send_false_to=['TO_MALE_EMPLOYEES'], condition=[{'negated': ['N'], 'leftvalue': ['gender'], 'function': ['='], 'rightvalue': ['Female']}])
        df_CHECK_GENDER = node_df_CHECK_GENDER.run(spark, input_df)
        df_CHECK_GENDER.cache()
        node_outputs['CHECK_GENDER'] = df_CHECK_GENDER
    except Exception as e:
        print(f'Failed: CHECK_GENDER - {e}')
        node_outputs['CHECK_GENDER'] = None
    # --- Node: TO_FEMALE_EMPLOYEES ---
    try:
        print('Executing (lazy): TO_FEMALE_EMPLOYEES')
        parent_output = node_outputs.get('CHECK_GENDER', None)
        if isinstance(parent_output, dict):
            input_df = parent_output.get('TO_FEMALE_EMPLOYEES', None)
            # If key missing, log warning (optional)
            if 'TO_FEMALE_EMPLOYEES' not in parent_output:
                print(f"Warning: Key 'TO_FEMALE_EMPLOYEES' not found in dict from 'CHECK_GENDER'. Available: " + str(list(parent_output.keys())))
        else:
            input_df = parent_output
        # Auto-detected input for 'TO_FEMALE_EMPLOYEES' from 'CHECK_GENDER'
        node_df_TO_FEMALE_EMPLOYEES = factory.TableOutputNode(properties={'step': {'name': ['OUT_female_emp'], 'type': ['TableOutput'], 'connection': ['company_db'], 'schema': [None], 'table': ['female_employees'], 'truncate': ['Y']}})
        writer_df_TO_FEMALE_EMPLOYEES = node_df_TO_FEMALE_EMPLOYEES.run(spark, input_df)
        writers.append((writer_df_TO_FEMALE_EMPLOYEES, '/delta/TO_FEMALE_EMPLOYEES'))
    except Exception as e:
        print(f'Failed: TO_FEMALE_EMPLOYEES - {e}')
        node_outputs['TO_FEMALE_EMPLOYEES'] = None
    # --- Node: TO_MALE_EMPLOYEES ---
    try:
        print('Executing (lazy): TO_MALE_EMPLOYEES')
        parent_output = node_outputs.get('CHECK_GENDER', None)
        if isinstance(parent_output, dict):
            input_df = parent_output.get('TO_MALE_EMPLOYEES', None)
            # If key missing, log warning (optional)
            if 'TO_MALE_EMPLOYEES' not in parent_output:
                print(f"Warning: Key 'TO_MALE_EMPLOYEES' not found in dict from 'CHECK_GENDER'. Available: " + str(list(parent_output.keys())))
        else:
            input_df = parent_output
        # Auto-detected input for 'TO_MALE_EMPLOYEES' from 'CHECK_GENDER'
        node_df_TO_MALE_EMPLOYEES = factory.TableOutputNode(properties={'step': {'name': ['OUT_male_emp'], 'type': ['TableOutput'], 'connection': ['company_db'], 'schema': [None], 'table': ['male_employees'], 'truncate': ['Y']}})
        writer_df_TO_MALE_EMPLOYEES = node_df_TO_MALE_EMPLOYEES.run(spark, input_df)
        writers.append((writer_df_TO_MALE_EMPLOYEES, '/delta/TO_MALE_EMPLOYEES'))
    except Exception as e:
        print(f'Failed: TO_MALE_EMPLOYEES - {e}')
        node_outputs['TO_MALE_EMPLOYEES'] = None

    # Execute all delayed writes (max laziness + overlap)
    # for writer, path in writers:
    #     writer.save(path)

    return node_outputs

if __name__ == '__main__':
    try:
        result = run_pipeline()
        print('Pipeline finished.')
    finally:
        spark.stop()
