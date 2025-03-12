import os
import pymssql
import frappe
from datetime import datetime, timedelta

# MSSQL's minimum valid datetime value
MIN_MSSQL_DATETIME = datetime(1753, 1, 1)


def get_mssql_config():
    """Retrieve MSSQL configuration from the 'MSSQL Attendance Settings' single doctype."""
    config = frappe.get_doc("MSSQL Attendance Settings")

    db_host = config.db_host
    db_port = config.db_port
    db_user = config.db_user
    db_password = config.db_password
    db_name = config.db_name

    if not all([db_host, db_port, db_user, db_password, db_name]):
        frappe.log_error(
            message=f"Missing MSSQL configuration environment variables: {', '.join(missing)}",
            title="MSSQL Configuration Error"
        )
        return None
    try:
        db_port = int(db_port)  # Ensure port is an integer
    except ValueError:
        frappe.log_error(
            message="Invalid port number in 'MSSQL Attendance Settings'. It must be an integer.",
            title="MSSQL Configuration Error",
        )
        return None

    return {
        "ATTENDANCE_DB_HOST": db_host,
        "ATTENDANCE_DB_PORT": db_port,
        "ATTENDANCE_DB_USER": db_user,
        "ATTENDANCE_DB_PASSWORD": db_password,
        "ATTENDANCE_DB_NAME": db_name,
    }


def attendance():
    """
    Connect to MSSQL, fetch new logs since the last_sync_time, and create Employee Checkin
    records in ERPNext. Avoid duplicates if an existing record is at the same time or
    if the last checkin was within 30 minutes.
    """

    # 0) Initial Log
    frappe.logger("mssql_attendance").info("Starting MSSQL Attendance Sync...")

    # 1) Retrieve and sanitize last_sync_time
    last_sync_raw = frappe.db.get_single_value("MSSQL Attendance Settings", "last_sync_time")
    last_sync_dt = validate_or_default_sync_time(frappe, last_sync_raw, default_days=2)
    frappe.logger("mssql_attendance").debug(f"Last sync time: {last_sync_dt}")

    # 2) Get MSSQL configuration from environment variables
    config = get_mssql_config()
    if not config:
        frappe.logger("mssql_attendance").error("MSSQL configuration missing or invalid. Aborting.")
        return

    # 3) Connect to MSSQL
    try:
        conn = pymssql.connect(
            server=config["ATTENDANCE_DB_HOST"],
            port=config["ATTENDANCE_DB_PORT"],
            user=config["ATTENDANCE_DB_USER"],
            password=config["ATTENDANCE_DB_PASSWORD"],
            database=config["ATTENDANCE_DB_NAME"],
            timeout=30  # Add a timeout to prevent indefinite hanging
        )
        frappe.logger("mssql_attendance").info(f"Successfully connected to MSSQL database: {config['ATTENDANCE_DB_NAME']}")
    except Exception as e:
        frappe.log_error(
            message=f"Could not connect to MSSQL: {str(e)}",
            title="MSSQL Connection Error"
        )
        frappe.logger("mssql_attendance").error(f"Failed to connect to MSSQL: {str(e)}")
        return

    try:
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
        database = config["ATTENDANCE_DB_NAME"]
        table_current = f"[{database}].[dbo].[DeviceLogs_{current_month}_{current_year}]"
        table_fallback = f"[{database}].[dbo].[DeviceLogs_{fallback_month}_{fallback_year}]"

        # 4) Try the current table; if fail, fallback
        test_cursor = conn.cursor()
        try:
            test_cursor.execute(f"SELECT TOP 1 * FROM {table_current}")
            test_cursor.fetchone()
            frappe.logger("mssql_attendance").debug(f"Successfully connected to current table: {table_current}")
            logs = fetch_all_logs(conn, table_current, last_sync_dt)
        except pymssql.Error as e:
            frappe.log_error(
                message=f"Query failed for {table_current}. Error: {e}",
                title="MSSQL Attendance Sync"
            )
            frappe.logger("mssql_attendance").warning(f"Query failed for {table_current}. Error: {e}. Trying fallback table.")
            # Fallback to previous month
            try:
                test_cursor.execute(f"SELECT TOP 1 * FROM {table_fallback}")
                test_cursor.fetchone()
                frappe.logger("mssql_attendance").debug(f"Successfully connected to fallback table: {table_fallback}")
                logs = fetch_all_logs(conn, table_fallback, last_sync_dt)
            except Exception as e2:
                frappe.log_error(
                    message=f"Query failed for fallback {table_fallback}. Error: {e2}",
                    title="MSSQL Attendance Sync"
                )
                frappe.logger("mssql_attendance").error(f"Query failed for fallback {table_fallback}. Error: {e2}. Aborting.")
                return

        # If no logs returned, nothing to process
        if not logs:
            frappe.msgprint("No new attendance logs found.")
            frappe.logger("mssql_attendance").info("No new attendance logs found.")
            return

        frappe.logger("mssql_attendance").info(f"Fetched {len(logs)} attendance logs from MSSQL.")

        # 5) Process all logs and track maximum log date
        global_max_log_date = None
        checkin_count = 0
        skipped_count = 0

        for row in logs:
            user_id = row[3]         # row[3] = UserId
            log_datetime = row[4]      # row[4] = LogDate

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
                skipped_count += 1
                frappe.logger("mssql_attendance").warning(f"Skipping log for device ID: {user_id}. No matching employee found.")
                continue  # Skip if no matching employee

            # Attempt to create the new checkin record
            if create_employee_checkin(frappe, employee_id, log_datetime, direction):
                checkin_count += 1
            else:
                skipped_count += 1

        # Commit after processing
        frappe.db.commit()
        frappe.logger("mssql_attendance").info("Committed changes to database.")

        # 6) Update last_sync_time to the maximum LogDate processed
        if global_max_log_date:
            new_sync_str = global_max_log_date.strftime("%Y-%m-%d %H:%M:%S")
            frappe.db.set_single_value("MSSQL Attendance Settings", "last_sync_time", new_sync_str)
            frappe.logger("mssql_attendance").info(f"Updated last_sync_time to: {new_sync_str}")

        frappe.msgprint(f"Successfully created {checkin_count} new check-in records. Skipped {skipped_count} records.")
        frappe.logger("mssql_attendance").info(f"Successfully created {checkin_count} new check-in records. Skipped {skipped_count} records.")

    except Exception as e:
        frappe.log_error(
            message=f"An unexpected error occurred during attendance processing: {str(e)}",
            title="MSSQL Attendance Sync"
        )
        frappe.logger("mssql_attendance").exception(f"An unexpected error occurred during attendance processing: {str(e)}")

    finally:
        if conn:
            conn.close()
            frappe.logger("mssql_attendance").info("Closed MSSQL connection.")

    frappe.logger("mssql_attendance").info("MSSQL Attendance Sync completed.")


def fetch_all_logs(conn, table_name, last_sync_dt):
    """
    Fetch all logs from `table_name` with LogDate > last_sync_dt, in ascending order.
    Returns a list of log records.
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
    try:
        cursor.execute(query, (last_sync_dt,))
        rows = cursor.fetchall()
        frappe.logger("mssql_attendance").debug(f"Successfully fetched {len(rows)} logs from {table_name} since {last_sync_dt}.")
        return rows
    except pymssql.Error as e:
        frappe.log_error(
            message=f"Error fetching logs from {table_name}: {e}",
            title="MSSQL Attendance Sync"
        )
        frappe.logger("mssql_attendance").error(f"Error fetching logs from {table_name}: {e}")
        return []


def guess_checkin_type(frappe, employee_device_id, log_datetime, suggested_direction):
    """
    Determine the log_type (IN/OUT) based on the last checkin record.
    Optionally, you can trust the device-provided direction.
    """
    # Uncomment the following if you want to trust the device-provided direction:
    # if suggested_direction in ["in", "out"]:
    #     return suggested_direction.title()

    emp_doc_name = frappe.db.get_value(
        "Employee",
        {"attendance_device_id": employee_device_id},
        "name"
    )
    if not emp_doc_name:
        frappe.logger("mssql_attendance").debug(f"No employee found for device ID: {employee_device_id}. Defaulting to IN.")
        return "IN"  # Default to IN if no Employee found

    last_checkin_type = frappe.db.get_value(
        "Employee Checkin",
        {"employee": emp_doc_name},
        "log_type",
        order_by="time DESC"
    )
    guessed_direction = "OUT" if last_checkin_type == "IN" else "IN"
    frappe.logger("mssql_attendance").debug(f"Guessed check-in type for employee {emp_doc_name} at {log_datetime} as {guessed_direction}.")
    return guessed_direction


def create_employee_checkin(frappe, employee_id, log_datetime, direction):
    """
    Creates a new Employee Checkin record for the specified employee at log_datetime.
    Skips creation if:
      1) An exact same checkin exists, or
      2) The previous checkin is within 30 minutes.
    Returns True if a checkin was created, False otherwise.
    """
    if not log_datetime:
        frappe.logger("mssql_attendance").warning(f"Skipping check-in creation for {employee_id} due to missing log_datetime.")
        return False

    # Check for an existing record with the same employee and time
    if frappe.db.exists("Employee Checkin", {"employee": employee_id, "time": log_datetime}):
        frappe.logger("mssql_attendance").debug(f"Skipping check-in creation for {employee_id} at {log_datetime} - duplicate record found.")
        return False  # Already exists

    # Check time difference from the last checkin
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
            frappe.logger("mssql_attendance").debug(f"Skipping check-in creation for {employee_id} at {log_datetime} - previous check-in within 30 minutes.")
            return False

    doc = frappe.new_doc("Employee Checkin")
    doc.employee = employee_id
    doc.log_type = direction.upper()
    doc.time = log_datetime
    try:
        doc.save(ignore_permissions=True)
        frappe.logger("mssql_attendance").info(f"Created new check-in record for {employee_id} at {log_datetime} ({direction}).")
        return True
    except Exception as e:
        frappe.log_error(
            message=f"Failed to create check-in record for {employee_id} at {log_datetime}: {e}",
            title="MSSQL Attendance Sync"
        )
        frappe.logger("mssql_attendance").error(f"Failed to create check-in record for {employee_id} at {log_datetime}: {e}")
        return False


def validate_or_default_sync_time(frappe, dt_val, default_days=2):
    """
    Convert dt_val to a valid datetime. If dt_val is invalid or None, fallback to (now - default_days).
    Ensures the datetime is not less than MSSQL's minimum valid datetime.
    """
    if isinstance(dt_val, datetime):
        result = dt_val
    elif dt_val:
        try:
            result = datetime.strptime(dt_val, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            frappe.logger("mssql_attendance").warning(f"Invalid last_sync_time format: {dt_val}. Using default.")
            result = datetime.now() - timedelta(days=default_days)
    else:
        frappe.logger("mssql_attendance").info("No last_sync_time found. Using default.")
        result = datetime.now() - timedelta(days=default_days)

    if result < MIN_MSSQL_DATETIME:
        frappe.logger("mssql_attendance").warning(f"Calculated sync time {result} is earlier than the minimum MSSQL datetime. Using minimum datetime.")
        result = MIN_MSSQL_DATETIME

    return result
