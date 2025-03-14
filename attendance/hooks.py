# -*- coding: utf-8 -*-
# Copyright (c) 2025, Mohesu
# See license.txt
"""Configuration for hooks."""


app_name = "attendance"
app_title = "Attendance"
app_publisher = "Mohesu"
app_description = "Sync attendance data from biometric device"
app_icon = "octicon octicon-sync"
app_color = "grey"
app_email = "app@mohesu.com"
app_license = "mit"


scheduler_events = {
    "cron": {
        # Every 20 minutes
        "*/20 * * * *": [
            "attendance.sync.mssql.attendance"
        ]
    }
}
