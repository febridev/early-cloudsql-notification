import json
import logging
import os
import subprocess
import shlex
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import (
    column, create_engine, text, insert, Table, MetaData, select, Join,delete
)
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from .auth import gcloud_login as ath
from .notification import create_message as notif

# Load environment variables
load_dotenv()

def get_quarter_info():
    """
    this function is create data
    with information quarter and year
    return will be 'Q4-2024'
    """
    # Get Current Month
    current_month = datetime.now().month

    # SET QUARTER MONTH
    if 1 <= current_month <= 3:
        quarter = 'Q1'
    elif 4 <= current_month <= 6:
        quarter = 'Q2'
    elif 7 <= current_month <= 9:
        quarter = 'Q3'
    else:
        quarter = 'Q4'
    

    #GET CURRENT YEAR
    current_year = datetime.now().year

    qdescription = f"{quarter}-{current_year}"

    return qdescription

def get_db_connection():
    """Create a connection to the database."""
    user_db = os.environ.get("USER_DB_DISCOVERY")
    pass_db = os.environ.get("PASS_DB_DISCOVERY")
    host_db = os.environ.get("HOST_DB_DISCOVERY")
    port_db = os.environ.get("PORT_DB_DISCOVERY")
    db_name = os.environ.get("DB_NAME")

    database_url = f"mysql+mysqlconnector://{user_db}:{pass_db}@{host_db}:{port_db}/{db_name}"
    engine = create_engine(database_url, pool_pre_ping=True)
    return engine


def input_csql_notification(project_name,instance_name,csql_version,quarter_info):
    """
    this function for input
    if any data from describe
    when new maintenance version is available
    """
    engine = get_db_connection()
    metadata = MetaData()

    #define table
    tcsql_notification = Table('tcsql_notification',metadata,autoload_with=engine)

    try:
        with engine.connect() as dbconnection:
            sql_statement = insert(tcsql_notification).values(project_name=f"{project_name}",instance_name=f"{instance_name}",csql_version=f"{csql_version}",quarter_info=f"{quarter_info}")
            exec_sql_statement = dbconnection.execute(sql_statement)
            dbconnection.commit()
            dbconnection.close()

    except SQLAlchemyError as e:
        logging.error(f"Database error occurred: {e}")
    except Exception as e:
        logging.error(f"Error in load_all_instances: {e}")
        exit()

def remove_csql_notification(quarter_info):
    """
    remove all csql notification 
    when quarter information not same with current quarter
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    engine = get_db_connection()
    metadata = MetaData()

    #define table
    tcsql_notification = Table('tcsql_notification',metadata,autoload_with=engine)

    try:
        with engine.connect() as dbconnection:
            delete_statement = delete(tcsql_notification).where(tcsql_notification.c.quarter_info != f"{quarter_info}")
            exec_delete_statement = dbconnection.execute(delete_statement)
            dbconnection.commit()
            dbconnection.close()
        logger.info(f"Successfully Deleted CloudSQL Notification With Expired Quarter")

    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}")
    except Exception as e:
        logger.error(f"Error in load_all_instances: {e}")
        exit()

def load_all_instances():
    """
    Retrieve all Cloud SQL instances with project and instance names.
    """
    all_instances = []
    sql_statement = text("""
    SELECT a.project_name,
               b.instance_name
        FROM tmproject a
        LEFT JOIN tthinstance b ON a.seq_id = b.project_id
        WHERE a.env_name = 'PROD'
        AND b.instance_name is not null
    """)

    engine = get_db_connection()
    try:
        with engine.connect() as db_connection:
            result = db_connection.execute(sql_statement)
            for instance in result:
                all_instances.append(dict(instance._mapping))
            return all_instances
    except SQLAlchemyError as e:
        logging.error(f"Database error occurred: {e}")
    except Exception as e:
        logging.error(f"Error in load_all_instances: {e}")
        exit()

def check_existing_csql_notification(project_name,instance_name,quarter_info):
    """
    Get exisisting maintenance notification 
    from tcsql_notification
    """
    existing_notification = []
    sql_statement = text("""
        select instance_name from tcsql_notification 
where project_name = :project_name 
and instance_name = :instance_name
and quarter_info = :quarter_info
    """)

    engine = get_db_connection()
    try:
        with engine.connect() as db_connection:
            result = db_connection.execute(sql_statement,{"project_name":project_name,"instance_name":instance_name,"quarter_info":quarter_info})
            for instance in result:
                existing_notification.append(dict(instance._mapping))
            return existing_notification
    except SQLAlchemyError as e:
        logging.error(f"Database error occurred: {e}")
    except Exception as e:
        logging.error(f"Error in load_all_instances: {e}")
        exit()


def get_maintenance_information(project_name, instance_name):
    """
    Run a gcloud command to check if a Cloud SQL instance is available
    for maintenance.
    """
    maintenance_info = []
    base_command = os.environ.get("GCLOUD_DESCRIBE")
    opt_key = os.environ.get("MAINTENANCE_KEY")
    opt_project_name = f"--project={project_name}"
    opt_instance_name = f"{instance_name}"
    opt_format = f"--format='json({opt_key})'"
    full_command = f"{base_command} {opt_instance_name} {opt_project_name} {opt_format}"

    try:
        cmd_list = shlex.split(full_command)
        cmd_output = subprocess.run(cmd_list, capture_output=True, text=True)
        cmd_json = json.loads(cmd_output.stdout)

        if cmd_json is None:
            maintenance_info.append({
                "project_name": project_name,
                "instance_name": instance_name,
                "maintenance_status": 0,
                "notes": "Instance has the latest supported maintenance version."
            })
        else:
            maintenance_info.append({
                "project_name": project_name,
                "instance_name": instance_name,
                "maintenance_status": 1,
                "notes": cmd_json['availableMaintenanceVersions'][0]
            })

        return maintenance_info
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing gcloud command: {e}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing gcloud command output: {e}")
        raise
    except Exception as e:
        logging.error(f"Error in get_maintenance_information: {e}")
        pass


def main():
    """
    Core function to run the application.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    try:
        # Login into GCP environment
        ath(os.environ.get("AUTH_EMAIL"), os.environ.get("SERVICE_ACCOUNT"))
    except Exception as e:
        logger.error(f"Authentication Exception: {e}")
        exit()

    #remove cloudsql notification with quarter expired
    remove_csql_notification(get_quarter_info())


    try:
        all_instances = load_all_instances()
        for instance in all_instances:
            try:
                logger.info(f"Checking Instances - {instance['instance_name']}")
                notification_check = get_maintenance_information(instance['project_name'],instance['instance_name'])
                if notification_check[0]['maintenance_status'] == 0:
                    logger.info(f"{notification_check[0]['instance_name']} - {notification_check[0]['notes']}")
                else:
                    # check when already send notification
                    instance_exists = check_existing_csql_notification(notification_check[0]['project_name'],notification_check[0]['instance_name'],get_quarter_info())
                    if len(instance_exists) != 1:

                        #insert into table tcsql_notification
                        input_csql_notification(notification_check[0]['project_name'],notification_check[0]['instance_name'],notification_check[0]['notes'],get_quarter_info())

                        # send notification
                        notif("success",f"{notification_check[0]['notes']}",f"{notification_check[0]['project_name']}",f"{notification_check[0]['instance_name']}")

                        logger.info(f"CloudSQL Instances {notification_check[0]['instance_name']} New Version Is Available")
                    else:
                        logger.info(f"CloudSQL Maintenance Notification Already Send")
            except Exception as e:
                logger.error(f"Failed Process Instances {instance['instance_name']}")
                continue

    except Exception as e:
        logger.error(f"Core Process Exception: {e}")
        exit()

if __name__ == "__main__":
    main()