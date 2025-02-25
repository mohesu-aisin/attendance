import os

import pymssql
from datetime import datetime, timedelta

# MSSQL's minimum valid datetime value
MIN_MSSQL_DATETIME = datetime(1753, 1, 1)


def attendance():
    import frappe  # Import frappe inside the function

    """
    Connect to MSSQL, fetch new logs since the last_sync_time, and create Employee Checkin
    records in ERPNext. Avoid duplicates if an existing record is at the same time or
    if the last checkin was within 30 minutes.

    Steps:
    1) Retrieve last_sync_time from 'MSSQL Attendance Settings' Single DocType.
    2) Connect to MSSQL using pymssql.
    3) Determine 'current month' vs 'fallback month' table.
    4) Fetch logs from the current table; if that fails, try the fallback table.
    5) For each record, determine IN/OUT and map user_id to Employee (via 'attendance_device_id').
    6) Skip creation if within 30 minutes of the last punch or an exact time match.
    7) Update last_sync_time to the maximum LogDate processed.
    """

    # 1) Retrieve and sanitize last_sync_time
    last_sync_raw = frappe.db.get_single_value("MSSQL Attendance Settings", "last_sync_time")
    last_sync_dt = validate_or_default_sync_time(frappe, last_sync_raw, default_days=2)

    # 2) Connect to MSSQL
    try:
        conn = pymssql.connect(
            server=os.environ.get("ATTENDANCE_DB_HOST"),
            port=os.environ.get("ATTENDANCE_DB_PORT"),
            user=os.environ.get("ATTENDANCE_DB_USER"),
            password=os.environ.get("ATTENDANCE_DB_PASSWORD"),
            database=os.environ.get("ATTENDANCE_DB_NAME")
        )
    except Exception as e:
        frappe.log_error(message=str(e), title="Could not connect to MSSQL (pymssql Error)")
        return

    # Determine current/fallback month-year
    now = datetime.now()
    current_month = now.month
    current_year = now.year

    if current_month == 1:
        fallback_month = 12
        fallback_year = current_year - 1
    else:
        fallback_month = current_month - 1
        fallback_year = current_year

    # Build table names
    database = os.environ.get("ATTENDANCE_DB_NAME")
    table_current = f"[{database}].[dbo].[DeviceLogs_{current_month}_{current_year}]"
    table_fallback = f"[{database}].[dbo].[DeviceLogs_{fallback_month}_{fallback_year}]"

    # 3) Try the current table; if fail, fallback
    test_cursor = conn.cursor()

    try:
        # Test if current table exists
        test_cursor.execute(f"SELECT TOP 1 * FROM {table_current}")
        test_cursor.fetchone()
        logs = fetch_all_logs(conn, table_current, last_sync_dt)
    except pymssql.Error as e:
        frappe.log_error(
            message=f"Query failed for {table_current}. Error: {e}",
            title="MSSQL Attendance Sync"
        )
        # Fallback to previous month
        try:
            test_cursor.execute(f"SELECT TOP 1 * FROM {table_fallback}")
            test_cursor.fetchone()
            logs = fetch_all_logs(conn, table_fallback, last_sync_dt)
        except Exception as e2:
            frappe.log_error(
                message=f"Query failed for fallback {table_fallback}. Error: {e2}",
                title="MSSQL Attendance Sync"
            )
            conn.close()
            return

    # If no logs returned, nothing to process
    if not logs:
        conn.close()
        return

    # 4) Process all logs and track max date
    global_max_log_date = None

    for row in logs:
        user_id = row[3]  # row[3] = UserId
        log_datetime = row[4]  # row[4] = LogDate

        if (global_max_log_date is None) or (log_datetime > global_max_log_date):
            global_max_log_date = log_datetime

        # Determine IN/OUT
        c1_direction = (row[7] or "").lower()  # row[7] = C1
        direction = guess_checkin_type(frappe, user_id, log_datetime, c1_direction)

        # Map user_id -> ERPNext Employee doc
        employee_id = frappe.db.get_value(
            "Employee",
            {"attendance_device_id": user_id},
            "name"  # docname
        )
        if not employee_id:
            continue  # No match

        # Attempt to create the new checkin record
        create_employee_checkin(frappe, employee_id, log_datetime, direction)

    # Commit after processing
    frappe.db.commit()

    # 5) Update last_sync_time to the maximum LogDate processed
    if global_max_log_date:
        new_sync_str = global_max_log_date.strftime("%Y-%m-%d %H:%M:%S")
        frappe.db.set_single_value("MSSQL Attendance Settings", "last_sync_time", new_sync_str)

    conn.close()


def fetch_all_logs(conn, table_name, last_sync_dt):
    """
    Fetch all logs from `table_name` with LogDate > last_sync_dt, in ascending order.
    Returns a list (not chunked).
    """
    cursor = conn.cursor()
    query = f"""
        SELECT
            DeviceLogId, DownloadDate, DeviceId, UserId, LogDate, Direction,
            AttDirection, C1, C2, C3, C4, C5, C6, C7, WorkCode, UpdateFlag,
            EmployeeImage, FileName, Longitude, Latitude, IsApproved,
            CreatedDate, LastModifiedDate, LocationAddress, BodyTemperature,
            IsMaskOn
        FROM {table_name}
        WHERE LogDate > %s
        ORDER BY LogDate ASC
    """
    cursor.execute(query, (last_sync_dt,))
    return cursor.fetchall()


def guess_checkin_type(frappe, employee_device_id, log_datetime, suggested_direction):
    """
    Determine the log_type (IN/OUT).
    1) If you want to trust device direction from C1, uncomment.
    2) Otherwise, base it on the last known checkin:
       - If last checkin was 'IN', new one is 'OUT'
       - Else 'IN'
    """
    # If you'd like to trust the device-provided direction:
    # if suggested_direction in ["in", "out"]:
    #     return suggested_direction.title()

    emp_doc_name = frappe.db.get_value(
        "Employee",
        {"attendance_device_id": employee_device_id},
        "name"
    )
    if not emp_doc_name:
        # If not found, default to 'IN'
        return "IN"

    last_checkin_type = frappe.db.get_value(
        "Employee Checkin",
        {"employee": emp_doc_name},
        "log_type",
        order_by="time DESC"
    )

    if last_checkin_type == "IN":
        return "OUT"
    else:
        return "IN"


def create_employee_checkin(frappe, employee_id, log_datetime, direction):
    """
    Creates a new Employee Checkin record for (employee_id, log_datetime, direction).
    Skips creation if:
      1) A checkin for this exact (employee, time) already exists, or
      2) The last checkin was within 30 minutes of log_datetime.
    """
    if not log_datetime:
        return

    # 1) Check for exact same-time record
    if frappe.db.exists("Employee Checkin", {"employee": employee_id, "time": log_datetime}):
        return  # already exists

    # 2) Check time difference from last checkin
    last_record = frappe.db.get_value(
        "Employee Checkin",
        {"employee": employee_id},
        ["name", "log_type", "time"],
        order_by="time DESC",
        as_dict=True
    )

    if last_record and isinstance(last_record.time, datetime):
        diff = (log_datetime - last_record.time).total_seconds()
        if diff < 1800:  # 30 minutes
            return

    # 3) Create the doc
    doc = frappe.new_doc("Employee Checkin")
    doc.employee = employee_id
    doc.log_type = direction.upper()
    doc.time = log_datetime
    doc.save(ignore_permissions=True)


def validate_or_default_sync_time(frappe, dt_val, default_days=2):
    """
    Convert dt_val -> Python datetime >= MIN_MSSQL_DATETIME.
    If dt_val is invalid or None, fallback to (now - default_days).
    """
    if isinstance(dt_val, datetime):
        result = dt_val
    elif dt_val:
        try:
            result = datetime.strptime(dt_val, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            result = datetime.now() - timedelta(days=default_days)
    else:
        result = datetime.now() - timedelta(days=default_days)

    if result < MIN_MSSQL_DATETIME:
        result = MIN_MSSQL_DATETIME

    return result
