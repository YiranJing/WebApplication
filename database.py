#!/usr/bin/env python3
"""
DeviceManagement Database module.
Contains all interactions between the webapp and the queries to the database.
"""

import configparser
import datetime
from typing import List, Optional

import setup_vendor_path  # noqa
import pg8000

################################################################################
#   Welcome to the database file, where all the query magic happens.
#   My biggest tip is look at the *week 9 lab*.
#   Important information:
#       - If you're getting issues and getting locked out of your database.
#           You may have reached the maximum number of connections.
#           Why? (You're not closing things!) Be careful!
#       - Check things *carefully*.
#       - There may be better ways to do things, this is just for example
#           purposes
#       - ORDERING MATTERS
#           - Unfortunately to make it easier for everyone, we have to ask that
#               your columns are in order. WATCH YOUR SELECTS!! :)
#   Good luck!
#       And remember to have some fun :D
################################################################################


#####################################################
#   Database Connect
#   (No need to touch
#       (unless the exception is potatoing))
#####################################################

def database_connect():
    """
    Connects to the database using the connection string.
    If 'None' was returned it means there was an issue connecting to
    the database. It would be wise to handle this ;)
    """
    # Read the config file
    config = configparser.ConfigParser()
    config.read('config.ini')
    if 'database' not in config['DATABASE']:
        config['DATABASE']['database'] = config['DATABASE']['user']

    # Create a connection to the database
    connection = None
    try:
        # Parses the config file and connects using the connect string
        connection = pg8000.connect(database=config['DATABASE']['database'],
                                    user=config['DATABASE']['user'],
                                    password=config['DATABASE']['password'],
                                    host=config['DATABASE']['host'])
    except pg8000.OperationalError as operation_error:
        print("""Error, you haven't updated your config.ini or you have a bad
        connection, please try again. (Update your files first, then check
        internet connection)
        """)
        print(operation_error)
        return None

    # return the connection to use
    return connection

#####################################################
#   Mutiple Lists Into One
#####################################################
def combine(muti_list):
    list = []
    for i in muti_list:
        list.append(i[0])
    return list
#####################################################
#   Query (a + a[i])
#   Login
#####################################################

def check_login(employee_id, password: str) -> Optional[dict]:
    """
    Check that the users information exists in the database.
        - True => return the user data
        - False => return None
    """
    ## step1: SQL quiery needed in this function
    # connect to db and get query result
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    employee_info = None
    try:
        # Try to execute the sql query and get information from database
        sql = """SELECT *
                 FROM employee
                 WHERE empid=%s AND password=%s"""
        cursor.execute(sql, (employee_id,password))
        employee_info = cursor.fetchall()[0] # first row
    except:
        # If error exists, print error and return NULL
        print("Error executing function")

    cursor.close() # Close the cursor
    connection.close() # Close the db connection

    ## Step2: Return results after check by sql query
    # Return None, if cannot find employee given the information
    if (employee_info is None):
        return None
    # When successfully check the password is correct,
    # return the detaied information of this employee
    user = {
        'empid': employee_info[0],
        'name': employee_info[1],
        'homeAddress': employee_info[2],
        'dateOfBirth': employee_info[3],
    }
    return user


#####################################################
#   Query (f[i])
#   Is Manager?
#####################################################

def is_manager(employee_id: int) -> Optional[str]:
    """
    Get the department the employee is a manager of, if any.
    Returns None if the employee doesn't manage a department.
    """

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    manager_of = None

    try:
        sql = """SELECT name
                 FROM Department
                 WHERE manager=%s"""
        cursor.execute(sql, (employee_id,))
        manager_of = cursor.fetchall()[0]
    except:
        print("Error executing function")

    cursor.close()
    connection.close()
    # the emloyee given doesn't manage a department
    if (manager_of is None):
        return None
    # the employee given manage the department
    return manager_of[0]


#####################################################
#   Query (a[ii])
#   Get My Used Devices
#####################################################

def get_devices_used_by(employee_id: int) -> list:
    """
    Get a list of all the devices used by the employee.
    """

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()

    devices = None
    try:
        sql = """SELECT deviceID, manufacturer, modelNumber
                            FROM DeviceUsedBy NATURAL JOIN Device
                            WHERE empID = %s"""
        cursor.execute(sql, (employee_id,))
        devices = cursor.fetchall() # fetch all rows
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (devices is None):
        return []
    return devices


#####################################################
#   Query (a[iii])
#   Get departments employee works in
#####################################################

def employee_works_in(employee_id: int) -> List[str]:
    """
    Return the departments that the employee works in.
    """
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    departments = None

    try:
        sql = """SELECT department
                            FROM EmployeeDepartments
                            WHERE empID = %s"""
        cursor.execute(sql, (employee_id,))
        departments = cursor.fetchall()
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (departments is None):
        return []
    return combine(departments)  ### ??? ？？？ I cannot find this function



#####################################################
#   Query (c)
#   Get My Issued Devices
#####################################################

def get_issued_devices_for_user(employee_id: int) -> list:

    """
    Get all devices issued to the user.
        - Return a list of all devices to the user.
    """

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    devices = None
    try:
        sql = """SELECT deviceID,purchaseDate, manufacturer, modelNumber
                            FROM Device
                            WHERE issuedTo = %s"""
        cursor.execute(sql, (employee_id,))
        devices = cursor.fetchall()
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (devices is None):
        return []

    return devices


#####################################################
#   Query (b)
#   Get All Models
#####################################################

def get_all_models() -> list:
    """
    Get all models available.
    """
    models = None
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        sql = """SELECT manufacturer, description, modelnumber, weight
                            FROM Model"""
        cursor.execute(sql)
        models = cursor.fetchall()
    except:
        print("Error executing function")


    cursor.close()
    connection.close()

    if (models is None):
        return []

    return models


#####################################################
#   Query (d[ii])
#   Get Device Repairs
#####################################################

def get_device_repairs(device_id: int) -> list:
    """
    Get all repairs made to a device.
    """

    repairs = None
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()

    try:
        sql = """SELECT repairid, faultreport, startdate, enddate, cost
                            FROM Repair
                            WHERE doneTo = %s"""
        cursor.execute(sql, [device_id])
        repairs = cursor.fetchall()
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (repairs is None):
        return []

    return repairs


#####################################################
#   Query (d[i])
#   Get Device Info
#####################################################

def get_device_information(device_id: int) -> Optional[dict]:
    """
    Get related device information in detail.
    """

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    device_info = None
    try:
        sql = """SELECT deviceID, serialNumber, purchaseDate, purchaseCost, manufacturer, modelNumber, issuedTo
                            FROM Device
                            WHERE deviceID = %s"""
        cursor.execute(sql, (device_id,))
        device_info = cursor.fetchall()[0]
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (device_info is None):
        print("No device information exists.")
        return None

    device = {
        'device_id': device_info[0],
        'serial_number': device_info[1],
        'purchase_date': device_info[2],
        'purchase_cost': device_info[3],
        'manufacturer': device_info[4],
        'model_number': device_info[5],
        'issued_to': device_info[6],
    }

    return device


#####################################################
#   Query (d[iii/iv])
#   Get Model Info by Device
#####################################################

def get_device_model(device_id: int) -> Optional[dict]:
    """
    Get model information about a device.
    """

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    model_info = None
    try:
        sql = """SELECT manufacturer,modelNumber,description,weight
                            FROM Device NATURAL JOIN Model
                            WHERE deviceID = %s"""
        cursor.execute(sql, (device_id,))
        model_info = cursor.fetchall()[0]
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (model_info is None):
        return None

    model = {
        'manufacturer': model_info[0],
        'model_number': model_info[1],
        'description': model_info[2],
        'weight': model_info[3],
    }

    return model


#####################################################
#   Query (e)
#   Get Repair Details
#####################################################

def get_repair_details(repair_id: int) -> Optional[dict]:
    """
    Get information about a repair in detail, including service information.
    """

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    repair_info = None
    try:
        sql = """SELECT repairID, faultReport, startDate, endDate, cost, abn, serviceName, email, doneTo
                            FROM Repair INNER JOIN Service ON (doneBy = abn)
                            WHERE repairID = %s"""
        cursor.execute(sql, [repair_id])
        repair_info = cursor.fetchall()[0]
    except:
        print("Error executing function")


    cursor.close()
    connection.close()

    if (repair_info is None):
        print("No repair information exists.")
        return None

    repair = {
        'repair_id': repair_info[0],
        'fault_report': repair_info[1],
        'start_date': repair_info[2],
        'end_date': repair_info[3],
        'cost': repair_info[4],
        'done_by': {
            'abn': repair_info[5],
            'service_name': repair_info[6],
            'email': repair_info[7],
        },
        'done_to': repair_info[8],
    }
    return repair

#####################################################
#   Query (f[ii])
#   Get Models assigned to Department
#####################################################

def get_department_models(department_name: str) -> list:
    """
    Return all models assigned to a department.
    """

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    model_allocations = None
    try:
        sql = """SELECT manufacturer, modelnumber, maxnumber
                            FROM Model NATURAL JOIN ModelAllocations
                            WHERE department  = %s"""
        cursor.execute(sql, (department_name,))
        model_allocations = cursor.fetchall()
    except:
        print("Error executing function")


    cursor.close()
    connection.close()

    if (model_allocations is None):
        return []

    return model_allocations


#####################################################
#   Query (f[iii])
#   Get Number of Devices of Model owned
#   by Employee in Department
#####################################################

def get_employee_department_model_device(department_name: str, manufacturer: str, model_number: str) -> list:
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()

    employee_counts = None
    try:
        sql = """SELECT E.empid, E.name, count(deviceID)
                            FROM Employee E INNER JOIN Device ON (issuedTo = empid) NATURAL JOIN ModelAllocations
                            WHERE department = %s AND manufacturer = %s AND modelnumber = %s
                            GROUP BY E.empid"""
        cursor.execute(sql, (department_name, manufacturer, model_number))
        employee_counts = cursor.fetchall()
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (employee_counts is None):
        return []

    return employee_counts


#####################################################
#   Query (f[iv])
#   Get a list of devices for a certain model and
#       have a boolean showing if the employee has
#       it issued.
#####################################################

def get_model_device_assigned(model_number: str, manufacturer: str, employee_id: int) -> list:
    """
    Get all devices matching the model and manufacturer and show True/False
    if the employee has the device assigned.

    E.g. Model = Pixel 2, Manufacturer = Google, employee_id = 1337
        - [123656, False]
        - [123132, True]
        - [51413, True]
        - [8765, False]
    """

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    device_assigned = None

    try:
        sql = """SELECT deviceID, CASE WHEN issuedTo = %s THEN 'True' ELSE 'False' END
                             FROM Device
                             WHERE manufacturer =%s
                              AND modelNumber = %s"""
        cursor.execute(sql, (employee_id, manufacturer, model_number))
        device_assigned = cursor.fetchall()
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (device_assigned is None):
        return []

    return device_assigned



#####################################################
#   Get a list of devices for this model and
#       manufacturer that have not been assigned.
#####################################################

def get_unassigned_devices_for_model(model_number: str, manufacturer: str) -> list:
    """
    Get all unassigned devices for the model.
    """
    device_unissued = None
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()

    try:
        sql = """SELECT deviceid
                            FROM Device
                            WHERE manufacturer =%s
                            AND modelNumber = %s
                            AND issuedTo is NULL"""
        cursor.execute(sql, (manufacturer, model_number))
        device_unissued = cursor.fetchall()
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (device_unissued is None):
        return []

    return device_unissued


#####################################################
#   Get Employees in Department
#####################################################

def get_employees_in_department(department_name: str) -> list:
    """
    Return all the employees' IDs and names in a given department.
    """
    employees = None
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()

    try:
        sql = """SELECT empid, name
                 FROM employee NATURAL JOIN employeedepartments
                 where department=%s"""
        cursor.execute(sql, [department_name])
        employees = cursor.fetchall()
    except:
        print("Error executing function")


    cursor.close()
    connection.close()

    if (employees is None):
        return []


    return employees


#####################################################
#   Query (f[v])
#   Issue Device
#####################################################

def issue_device_to_employee(employee_id: int, device_id: int):
    """
    Issue the device to the chosen employee.
    """

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    issue_device = None

    try:
        sql = """SELECT * FROM Device
              WHERE deviceid = %s AND issuedTo is not NULL"""
        cursor.execute(sql, [device_id])
        issue_device = cursor.fetchall()[0]
    except:
        print("Error executing function")

    cursor.close()
    connection.close()
    if (issue_device is not None):
        return (False, "Device already issued")

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute("""UPDATE Device
                            SET issuedTo = %s
                            WHERE deviceid = %s""", (employee_id, device_id))
        issue_device = cursor.fetchall()
    except:
        print("Error executing function")

    connection.commit()
    cursor.close()
    connection.close()

    return (True, None)


#####################################################
#   Query (f[vi])
#   Revoke Device Issued to User
#####################################################

def revoke_device_from_employee(employee_id: int, device_id: int):
    """
    Revoke the device from the employee.
    """
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()

    issue_device = None
    try:
        cursor.execute("""SELECT deviceid,issuedto
                            FROM Device
                            WHERE deviceid = %s AND issuedto = %s""",(device_id,employee_id))
        issue_device = cursor.fetchall()[0]
    except:
        print("Error executing function")
    cursor.close()
    connection.close()

    if (issue_device is None):
        return (False, "Employee not assigned to device")

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    try:
        cursor.execute(""" UPDATE Device
                            SET issuedto = NULL
                            WHERE deviceid = %s AND issuedto = %s;""",(device_id, employee_id))
    except:
        print("Error executing function")

    connection.commit()
    cursor.close()
    connection.close()
    return (True, None)
#####################################################
#   Extension 1
#   Used History
#####################################################
def used_history(employee_id: int) -> list:
    """
    Input:
    -------------------------------------------------------
    employee_id

    Output:
    -----------------------------------------------------------
    Return a list of users of each device, which is issued to this employee.
    """
    history = None
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()

    try:
        sql = """SELECT deviceID, empid, name
                    FROM Device NATURAL JOIN DeviceUsedBy NATURAL JOIN Employee
                    WHERE deviceID IN ( SELECT deviceID
                                            FROM Device JOIN Deviceusedby using (deviceID)
                                            WHERE issuedTo = %s);"""
        cursor.execute(sql, [employee_id])
        history = cursor.fetchall()
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (history is None):
        return None

    return history

#####################################################
#   Extension 2
#   Add model
#####################################################
def add_model(department: str, manufacturer: str, modelNumber: str, description: str, weight: str, maxNumber: int) -> list:

    """
    Add model for this department

    Input:
    -------------------------------------------------------
    department:
        given department_name
    detailed model information:
        modelNumber, manufacturer, description, and weight
    maxNumber:
        the maximum number of added model for this department

    Output:
    -----------------------------------------------------------
    A list of history:
        detalied information for the given model
    """

    model_info = None
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()

    try:
        # find the detailed unformation for the given model
        # record detaled information to 'history'
        sql = """SELECT *
                    FROM model
                    WHERE modelNumber = %s AND manufacturer = %s;"""
        cursor.execute(sql, (modelNumber,manufacturer))
        history = cursor.fetchall()
    except:
        print("Error executing function")

    # If the given model is new, has not been recorded in db
    if (history is None or len(history) == 0):
        # As PK and weight cannot be null, so only description can be null.
        # if the new model has no description given
        if description is None:
            sql = """INSERT INTO Model(manufacturer,modelNumber,description,weight)
                            VALUES (%s,%s,NULL,%s);"""
            cursor.execute(sql, (manufacturer,modelNumber,weight))
        else:
            # All model infortion given
            sql = """INSERT INTO Model(manufacturer,modelNumber,description,weight)
                            VALUES (%s,%s,%s,%s);"""
            cursor.execute(sql, (manufacturer,modelNumber,description,weight))
        # Update the information of ModelAllocations table
        sql = """INSERT INTO ModelAllocations(manufacturer,modelNumber,department,maxNumber)
                            VALUES (%s,%s,%s,%s);"""
        cursor.execute(sql, (manufacturer,modelNumber,department,maxNumber))
        connection.commit()
        return None

    cursor.close()
    connection.close()

    return history

#####################################################
#   Extension 3
#   Model Cost Each Month
#####################################################
def show_model_detail(manufacturer: str, model_number: str)-> list:
    """
    Add model for this department

    Input:
    -------------------------------------------------------
    gievn model:
        modelNumber and manufacturer

    Output:
    -----------------------------------------------------------
    A list:
        detalied information for the given model
    """

    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()
    model_info = None
    try:
        # get infortion of given model
        sql = """SELECT manufacturer,modelNumber,description,weight
                            FROM Model
                            WHERE manufacturer = %s AND modelNumber = %s"""
        cursor.execute(sql, (manufacturer,model_number))
        model_info = cursor.fetchall()[0]
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (model_info is None):
        return None

    model = {
        'manufacturer': model_info[0],
        'model_number': model_info[1],
        'description': model_info[2],
        'weight': model_info[3],
    }

    return model

def get_model_cost(manufacturer: str, model_number: str) -> list:
    """
    Return the average cost spent on the model each month each year.

    Input:
    -------------------------------------------------------
    gievn model:
        modelNumber and manufacturer

    Output:
    -----------------------------------------------------------
    A list:
        The average cost of each model each month each year for the given model.
    """
    costs = None
    connection = database_connect()
    if(connection is None):
        return None
    cursor = connection.cursor()

    try:
        sql = """SELECT  CAST(EXTRACT(year FROM purchaseDate) AS int) AS year,
                        CAST(EXTRACT(month FROM purchaseDate) AS int) AS month,
                        CAST(round(AVG(purchaseCost::numeric),2) AS money)
                    AS average_cost
                    FROM Device
                    WHERE  manufacturer = %s AND modelNumber = %s
                    GROUP BY year, month
                    Order by year desc, month desc;"""
        cursor.execute(sql, (manufacturer, model_number))
        costs = cursor.fetchall()
    except:
        print("Error executing function")

    cursor.close()
    connection.close()

    if (costs is None):
        return []

    return costs
