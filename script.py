from parsons import Redshift, Table, Airtable
import os
import logging


#-------------------------------------------------------------------------------
# Load Environment
#-------------------------------------------------------------------------------
try:
    location = os.environ['CIVIS_RUN_ID']
    # Set environ using civis credentials from container script
    os.environ['REDSHIFT_DB'] = os.environ['REDSHIFT_DATABASE']
    os.environ['REDSHIFT_USERNAME'] = os.environ['REDSHIFT_CREDENTIAL_USERNAME']
    os.environ['REDSHIFT_PASSWORD'] = os.environ['REDSHIFT_CREDENTIAL_PASSWORD']
    os.environ['S3_TEMP_BUCKET'] = 'parsons-tmc'
    os.environ['AIRTABLE_API_KEY'] = os.environ['AIRTABLE_API_KEY_PASSWORD']
    airtable_base_key = os.environ['airtable_base_key']
    airtable_table_name = os.environ['airtable_table_name']
    redshift_table = os.environ['redshift_form_response_table']
    unique_id = os.environ['unique_id']
    int_null = os.environ['int_null']
    float_null = os.environ['float_null']

# If running locally, load this env
except KeyError:
    from dotenv import load_dotenv
    load_dotenv()
    airtable_base_key = ''  # put your base key here
    airtable_table_name = ''  # put table name here
    redshift_table = ''  # put redshift table name here



#-------------------------------------------------------------------------------
# Set up logger
#-------------------------------------------------------------------------------
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')
logger.info('Hey there! I hope youre having a nice day and sorry in advance if I cause you any trouble.')


#-------------------------------------------------------------------------------
# Instantiate classes
#-------------------------------------------------------------------------------
at = Airtable(airtable_base_key, airtable_table_name)
rs = Redshift()


#-------------------------------------------------------------------------------
# Define functions
#-------------------------------------------------------------------------------
def generate_airtable_dict():
    # Get a parsons table of airtable recordids and vanids
    records = at.get_records()
    logger.info(f'''{records.num_rows} records already in airtable''')
    record_dict = {record[unique_id]:record['id'] for record in records}
    return records,record_dict

def generate_redshift_dict():
    # Get a parsons table of the records from redshift
    rs_query = f'''
    select * from {redshift_table}
    '''
    rs_table = rs.query(rs_query)
    return rs_table

def match_datatypes(airtable_table, redshift_table):

    # Getting column names and data types for comparison
    at_cols = [col for col in list(airtable_table.columns) if col not in ['id','createdTime']]
    at_cols.sort()
    at_types = [airtable_table.get_column_types(col) for col in at_cols]

    rs_cols = list(redshift_table.columns)
    rs_cols.sort()
    rs_types = [redshift_table.get_column_types(col) for col in rs_cols]
                
                
    # Converting redshift table columns to corresponding airtable types
    for i in range(len(at_cols)):
        col = at_cols[i]
        value = airtable_table[col][0]
        redshift_table.convert_column(col, type(value))
    
    
    # Identifying columns with NoneTypes
    cols_w_nones = []
    for i in range(len(rs_cols)):
        col = rs_cols[i]
        type_list = rs_types[i]
        if 'NoneType' in type_list:
            other_type = [typ for typ in type_list if typ != 'NoneType'][0]
            col_tuple = col, other_type
            cols_w_nones.append(col_tuple)

    # Replacing NoneTypes with null values in the appropriate data type
    for col, typ in cols_w_nones:
        if typ == 'str':
            redshift_table.convert_column(col, lambda x: ''         if x == 'None' else x)
            redshift_table.convert_column(col, lambda x: ''         if x is  None  else x)
        elif typ == 'float':
            redshift_table.convert_column(col, lambda x: float_null if x == 'None' else x)
            redshift_table.convert_column(col, lambda x: float_null if x is  None  else x)
        elif typ == 'int':
            redshift_table.convert_column(col, lambda x: int_null   if x == 'None' else x)
            redshift_table.convert_column(col, lambda x: int_null   if x is  None  else x)
        else:
            logger.info('Idk what to do with this:',typ)

    # Compare data types between tables
    rs_new_types = [redshift_table.get_column_types(col) for col in rs_cols]
    logger.info(print(list(zip(at_cols, at_types,rs_new_types))))
    
    
    return airtable_table, redshift_table


#-------------------------------------------------------------------------------
# Define main
#-------------------------------------------------------------------------------
def main():
    # Get airtable table as parsons table and a dict of ids
    at_table, at_dict = generate_airtable_dict()

    # Get civis table as parsons table
    rs_table = generate_redshift_dict()

    # Match datatypes
    airtable_table, redshift_table = match_datatypes(at_table, rs_table)

    # Separate records into new records and records that need to be updated (print numbers to check that they add up)
    update_table = redshift_table.select_rows(lambda row: row[unique_id] in at_dict.keys())
    new_table = redshift_table.select_rows(lambda row: row[unique_id] not in at_dict.keys())
    logger.info(print(redshift_table.num_rows, update_table.num_rows,new_table.num_rows))
   
    # Looping through table of existing records and updating every field in those rows
    for row in update_table:
        unique = row[unique_id]
        at_id = str(at_dict[unique])
        fields = {c: row[c] for c in update_table.columns if c != 'Id' and row[c] is not None}
        at.update_record(at_id, fields, typecast = True)
        
       
    # Inserting new records
    at.insert_records(new_table)
    

#-------------------------------------------------------------------------------
# Run main
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    main()