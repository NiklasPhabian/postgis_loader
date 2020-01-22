import os
import sqlalchemy
import bbox_monrovia as bbox
import multiprocessing
import viirs
import glob
import database


db = database.PostgresDatabase(config_file='database.config', config_name='monrovia')
table_cldmsk = database.DBTable(database=db, table_name='cldmsk')
table_dnb = database.DBTable(database=db, table_name='dnb')

def dnb2db(file_name):
    print(file_name)
    db.engine.dispose()
    dnb = viirs.DNB(file_name)
    if table_dnb.contains_value(column='time_stamp', value=dnb.time_stamp):
        print('file already in db')
    else:
        dnb.read()
        df = dnb.to_clipped_df(bbox=bbox)
        df.rename(columns={'DNB_observations': 'dnb'}, inplace=True)
        df.to_sql(name='dnb', con=db.engine, if_exists='append', index=False)

def dnb_folder2db(folder_path):
    file_names = glob.glob(os.path.expanduser(folder_path) + 'VNP02DNB*')
    for file_name in file_names:
        dnb2db(file_name)

def dnb_folder2db_mp(folder_path):
    file_names = glob.glob(os.path.expanduser(folder_path) + 'VNP02DNB*')
    with multiprocessing.Pool(processes=4) as pool:
        pool.map(dnb2db, file_names)

def cldmsk2db(file_name):
    print(file_name)
    db.engine.dispose()
    cldmsk = viirs.CLDMSK(file_name)
    if table_cldmsk.contains_value(column='time_stamp', value=cldmsk.time_stamp):
        print('file already in db')
    else:
        cldmsk.read()
        df = cldmsk.to_clipped_df(bbox=bbox)
        df.rename(columns={'Clear_Sky_Confidence': 'clear_sky_confidence'}, inplace=True)
        df.to_sql(name='cldmsk', con=db.engine, if_exists='append', index=False)

def clmdks_folder2db(folder_path):
    file_names = glob.glob(os.path.expanduser(folder_path) + 'CLDMSK*')
    for file_name in file_names:
        cldmsk2db(file_name)

def clmdks_folder2db_mp(folder_path):
    file_names = glob.glob(os.path.expanduser(folder_path) + 'CLDMSK*')
    with multiprocessing.Pool(processes=4) as pool:
        pool.map(cldmsk2db, file_names)

def wipe_dnb():
    print('wiping dnb')
    table_dnb.clear()

def wipe_cldmsk():
    print('wiping cldmsk')
    table_cldmsk.clear()

if __name__ == '__main__':
    folder = '/home/griessbaum/night_lights/viirs/monrovia/'
    #wipe_dnb()
    #dnb_folder2db_mp(folder)
    #wipe_cldmsk()
    clmdks_folder2db_mp(folder)
