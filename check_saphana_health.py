#!/usr/bin/env python3
# coding=utf-8
################################################
# Rodolphe ALT
# 0.1b
# goal : check SAP HANA database from Nagios
#################################################
# hostname = sap_hana_server
# sqlport = 30044
# username = TECH_MONI
# password = UltraComplexPassword2020!
# example : python check_saphana_health.py --hostname sap_hana_server --username TECH_MONI --password UltraComplexPassword2020! --sqlport 30044 --mode backup
# example : python check_saphana_health.py --hostname sap_hana_server --username TECH_MONI --password UltraComplexPassword2020! --sqlport 30044 --mode alert --timeout 600
# example : ./check_saphana_health.sh --hostname sap_hana_server --username TECH_MONI --password UltraComplexPassword2020! --sqlport 30044 --mode alert --timeout 600
#################################################


import sys
from hdbcli import dbapi
import argparse
from prettytable import PrettyTable
from datetime import datetime, timedelta
import re
from enum import Enum


def function_exit(status):
    if status == "OK":
        sys.exit(0)
    if status == "WARNING":
        sys.exit(1)
    if status == "CRITICAL":
        sys.exit(2)
    if status == "UNKNOWN":
        sys.exit(3)


def function_check_M_SYSTEM_OVERVIEW(section, name, type):
    command_sql = (
        "SELECT STATUS,VALUE FROM SYS.M_SYSTEM_OVERVIEW WHERE SECTION='"
        + section
        + "' and NAME='"
        + name
        + "'"
    )
    cursor.execute(command_sql)
    resultat = cursor.fetchone()
    resultat_0 = resultat[0]
    resultat_1 = resultat[1]
    perf = ""
    if type == "CPU":
        perf = " | "
        available = resultat_1.split(" ")[1].replace(",", "")
        used = "{}%".format(resultat_1.split(" ")[3])
        resultat_1 = used
        perf += " '{state}'={value};;;;{max} ".format(
            state="cpu_used", value=used, max=""
        )
        perf += " '{state}'={value};;;;{max} ".format(
            state="cpu_available", value=available, max=""
        )
    elif re.fullmatch(r"Datafiles|Logfiles|Tracefiles", type):
        perf = " | "
        # Example return string 'Size 4608.0 GB, Used 4083.0 GB, Free 12 %'
        split_result = resultat_1.split(" ")
        size = split_result[1]
        size_unit = split_result[2].replace(",", "")
        perf += " '{state}'={value}{unit};;;;{max} ".format(
            state="size", value=size, unit=size_unit, max=""
        )
        used = split_result[4]
        used_unit = split_result[5].replace(",", "")
        perf += " '{state}'={value}{unit};;;;{max} ".format(
            state="used", value=used, unit=used_unit, max=""
        )
        free = split_result[7]
        free_unit = split_result[8]
        perf += " '{state}'={value}{unit};;;;{max} ".format(
            state="free", value=free, unit=free_unit, max=""
        )
    elif type == "Memory":
        # Example return string 'Physical 4031.87 GB, Swap 2.00 GB, Used 2514.05'
        perf = " | "
        split_result = resultat_1.split(" ")
        physical = split_result[1]
        physical_unit = resultat_1.split(" ")[2].replace(",", "")
        perf += " '{state}'={value}{unit};;;;{max} ".format(
            state="physical", value=physical, unit=physical_unit, max=""
        )
        swap = split_result[4]
        swap_unit = resultat_1.split(" ")[5].replace(",", "")
        perf += " '{state}'={value}{unit};;;;{max} ".format(
            state="swap", value=swap, unit=swap_unit, max=""
        )
        used = split_result[7]
        # Used unit is not returned, so we use the same as for physical (fingercross that it is always the same)
        used_unit = physical_unit
        perf += " '{state}'={value}{unit};;;;{max} ".format(
            state="used", value=used, unit=used_unit, max=""
        )

    out = "{} - SAP HANA {} : {} ".format(resultat_0, type, resultat_1)
    print(out + perf)
    function_exit(resultat_0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""Check SAP HANA database
            backup : last backup""",
    )
    requiredNamed = parser.add_argument_group("required named arguments")
    requiredNamed.add_argument("--hostname", help="SAP HANA hostname", required=True)
    requiredNamed.add_argument("--username", help="SAP HANA login", required=True)
    requiredNamed.add_argument("--password", help="SAP HANA password", required=True)
    requiredNamed.add_argument("--sqlport", help="SAP HANA SQL port", required=True)
    requiredNamed.add_argument(
        "--mode",
        help="backup_data, backup_log , version, cpu, memory, mem_host, services, services_all, license_usage, db_data, db_log, db_trace, alert, sid, log_usage, raw_record_count, plc_replication",
    )
    requiredNamed.add_argument(
        "--warning", help="Warning threshold for modes supporting it."
    )
    requiredNamed.add_argument(
        "--critical", help="Critical threshold for modes supporting it."
    )
    requiredNamed.add_argument("--timeout", help="increase the default (60s) timeout")

    args = parser.parse_args(sys.argv[1:])
try:
    connection = dbapi.connect(
        address=args.hostname,
        port=args.sqlport,
        user=args.username,
        password=args.password,
    )
    if args.timeout != None:
        connection.timeout = int(args.timeout)
    cursor = connection.cursor()

    if args.mode == "backup_data":
        # -- last backups data since 3 days
        perf = ""
        critical = 2
        if args.critical:
            critical = int(args.critical)
        last_successful_backup = ""
        last_successful_detail = ""
        command_sql = "SELECT TOP 1 ENTRY_TYPE_NAME,STATE_NAME,SYS_START_TIME FROM SYS.M_BACKUP_CATALOG where (ENTRY_TYPE_NAME = 'differential data backup' or ENTRY_TYPE_NAME = 'complete data backup') and state_name='successful' order by SYS_START_TIME desc;"
        # command_sql = "SELECT top 1 sys_start_time FROM SYS.M_BACKUP_CATALOG where entry_type_name = 'complete data backup' and state_name='successful' order by sys_start_time desc;"
        cursor.execute(command_sql)
        # last_successful_backup = (cursor.fetchone())
        result = cursor.fetchone()
        last_successful_backup = result[2]
        status = result[1]
        type = (result[0]).capitalize()
        if last_successful_backup:
            bkp_age = datetime.now() - last_successful_backup
            bkp_age_days = int(bkp_age.total_seconds() / 86400)
            if bkp_age >= timedelta(days=critical):
                resultat_status = "CRITICAL"
                last_successful_detail = (
                    "{} older than {} days. (last successful: {}) ".format(
                        type, critical, str(last_successful_backup)
                    )
                )
            else:
                resultat_status = "OK"
                last_successful_detail = (
                    "{} not older than {} days. (last successful: {})".format(
                        type, critical, str(last_successful_backup)
                    )
                )
            perf += " | '{state}'={value}s;;;;{max} ".format(
                state="bkp_age",
                value=int(bkp_age.total_seconds()),
                max=critical * 86400,
            )
        else:
            resultat_status = "CRITICAL"
            last_successful_detail = "Could not find entry for last successful complete data backup in SYS.M_BACKUP_CATALOG."
        out = "{} - SAP HANA Backup: {}".format(resultat_status, last_successful_detail)

        print(out + perf)
        function_exit(resultat_status)

    if args.mode == "log_usage":
        command_sql = "select STATE,round(SUM(USED_SIZE)/1024/1024,2)  as USED_MB, round(SUM(TOTAL_SIZE)/1024/1024,2) as TOTAL_MB FROM M_LOG_SEGMENTS group by STATE order by STATE;"
        cursor.execute(command_sql)
        resultat = cursor.fetchall()
        out = "OK: "
        perf = " | "
        for row in resultat:
            state = row[0]
            value = round(row[1], 0)
            max = round(row[2], 0)
            out += "State: {state} / USED_MB: {value} / TOTAL_MB: {max} ".format(
                state=state, value=value, max=max
            )
            perf += "'{state}'={value}MB;;;;{max} ".format(
                state=state, value=value, max=max
            )
            # print("State: %s / USED_MB: %s / TOTAL_MB: %s | '%s'=%sMB;;;;%s" % (row[0],row[1],row[2],row[0],row[1],row[2]))
            # print("State: {state} / USED_MB: {value} / TOTAL_MB: {max} | '{state}'={value}MB;;;;{max}".format(state = row[0],value = row[1],max = row[2]))
        print(out + perf)
    if args.mode == "backup_log":
        perf = ""
        critical = 3
        if args.critical:
            critical = int(args.critical)
        last_successful_backup = ""
        last_successful_detail = ""
        command_sql = "SELECT top 1 sys_start_time FROM SYS.M_BACKUP_CATALOG where entry_type_name = 'log backup' and state_name='successful' order by sys_start_time desc;"
        cursor.execute(command_sql)
        last_successful_backup = cursor.fetchone()
        if last_successful_backup:
            bkp_age = datetime.now() - last_successful_backup[0]
            bkp_age_hours = int(bkp_age.total_seconds() / 3600)
            bkp_age_min = int((bkp_age.total_seconds() - bkp_age_hours * 3600) / 60)
            perf += " | '{state}'={value}s;;;;{max} ".format(
                state="bkp_age", value=bkp_age.total_seconds(), max=critical * 3600
            )
            if bkp_age >= timedelta(hours=critical):
                command_sql = (
                    "SELECT value FROM m_inifile_contents where key='log_mode';"
                )
                cursor.execute(command_sql)
                log_mode = cursor.fetchone()
                if log_mode[0] == "overwrite":
                    resultat_status = "WARNING"
                    last_successful_detail = "LOG MODE Overwrite enabled"
                else:
                    resultat_status = "CRITICAL"
                    last_successful_detail = "Last log backup performed {}h{}m ago. (last successful  : {}) ".format(
                        bkp_age_hours, bkp_age_min, str(last_successful_backup[0])
                    )
            else:
                resultat_status = "OK"
                last_successful_detail = "Last log backup performed {}h{}m ago. (last successful  : {})".format(
                    bkp_age_hours, bkp_age_min, str(last_successful_backup[0])
                )
        else:
            resultat_status = "CRITICAL"
            last_successful_detail = (
                "Could not find entry for last log backup in SYS.M_BACKUP_CATALOG."
            )
        out = "{} - SAP HANA LOG Backups: {}".format(
            resultat_status, last_successful_detail
        )

        print(out + perf)
        function_exit(resultat_status)

    if args.mode == "version":
        # check SAP HANA version
        command_sql = "SELECT VERSION FROM SYS.M_DATABASE"
        cursor.execute(command_sql)
        resultat = cursor.fetchone()
        print("OK - SAP HANA running version : %s |\n|" % resultat)

    if args.mode == "memory":
        warning = 80
        critical = 90
        if args.warning:
            warning = int(args.warning)
        if args.critical:
            critical = int(args.critical)
        # check SAP HANA memory
        command_sql = 'SELECT LPAD(TO_DECIMAL(ROUND(SUM(INSTANCE_TOTAL_MEMORY_USED_SIZE) OVER () / 1024 / 1024 / 1024), 10, 0), 9) as "HANA instance memory (used)", LPAD(TO_DECIMAL(ROUND(SUM(INSTANCE_TOTAL_MEMORY_ALLOCATED_SIZE) OVER () / 1024 / 1024 / 1024), 10, 0), 9) as "HANA instance memory (allocated)", LPAD(TO_DECIMAL(ROUND(SUM(ALLOCATION_LIMIT) OVER () / 1024 / 1024 / 1024), 10, 0), 9) as "HANA instance memory (limit)" FROM M_HOST_RESOURCE_UTILIZATION'
        cursor.execute(command_sql)
        resultat = cursor.fetchone()
        resultat_0 = int(resultat[0])
        resultat_1 = int(resultat[1])
        resultat_2 = int(resultat[2])
        value_warn = format(resultat_2 * warning / 100)
        value_crit = format(resultat_2 * critical / 100)
        resultat_percentage = "{0:.0%}".format(1.0 * resultat_0 / resultat_2)
        resultat_per_num = float(resultat_percentage[:-1])
        if resultat_per_num <= warning:
            resultat_status = "OK"
        elif resultat_per_num >= critical:
            resultat_status = "CRITICAL"
        elif resultat_per_num > warning and resultat_per_num < critical:
            resultat_status = "WARNING"
        print(
            "%s - SAP HANA Used Memory (%s) : %s GB Used / %s GB Allocated / %s GB Limit | mem=%sGB;%s;%s;0;%s"
            % (
                resultat_status,
                resultat_percentage,
                resultat_0,
                resultat_1,
                resultat_2,
                resultat_0,
                value_warn,
                value_crit,
                resultat_2,
            )
        )
        function_exit(resultat_status)

    if args.mode == "raw_record_count":
        table_limit = 15  # Only the largest 15 Tables will be shown
        max_rows = 2_147_483_648  # Hard Limit from SAP
        total_record_warning = 1_400_000_000
        total_record_critical = 1_700_000_000
        delta_record_warning = 100_000_000
        delta_record_critical = 110_000_000
        if args.warning and ":" in args.warning:
            total_record_warning = int(float(args.warning.split(":")[0]))
            delta_record_warning = int(float(args.warning.split(":")[1]))
        else:
            raise Exception("Argument warning is empty or not in format w1:w2")
        if args.critical and ":" in args.critical:
            total_record_critical = int(float(args.critical.split(":")[0]))
            delta_record_critical = int(float(args.critical.split(":")[1]))
        else:
            raise Exception("Argument critical is empty or not in format w1:w2")
        # The query will return the results already ordered so we know the first entry is the largest
        command_sql = f"select HOST, SCHEMA_NAME, TABLE_NAME, PART_ID, LAST_MERGE_TIME, RECORD_COUNT, RAW_RECORD_COUNT_IN_MAIN, RAW_RECORD_COUNT_IN_DELTA, RAW_RECORD_COUNT_IN_MAIN + RAW_RECORD_COUNT_IN_DELTA as TOTAL_RECORD from M_CS_TABLES where (RAW_RECORD_COUNT_IN_MAIN + RAW_RECORD_COUNT_IN_DELTA) > {total_record_warning} or RAW_RECORD_COUNT_IN_DELTA > {delta_record_warning} order by TOTAL_RECORD DESC LIMIT {table_limit};"

        cursor.execute(command_sql)
        resultat = cursor.fetchall()
        if not resultat:
            print(
                f"OK - No table found with more than {total_record_warning:,} total records and {delta_record_warning:,} records in delta."
            )
            function_exit("OK")

        largest_total_record = max(resultat, key=lambda item: item[8])
        largest_delta_record = max(resultat, key=lambda item: item[7])
        if (
            largest_total_record[8] > total_record_critical
            or largest_delta_record[7] > delta_record_critical
        ):
            resultat_status = "CRITICAL"
        else:
            resultat_status = "WARNING"
        style = f'style="border: 1px solid;"'
        output_list = [
            f"<tr><th {style}>Table Name</th><th {style}>Partition ID</th><th {style}>Total Record</th><th {style}>Raw Record in Delta</th></tr>"
        ]
        output_text = []
        tables_critical = []
        tables_warning = []
        perf = " | "
        for row in resultat:
            table_name = row[2]
            total_record = int(row[8])
            raw_record_in_delta = int(row[7])
            partition_id = int(row[3])

            if total_record > total_record_critical:
                total_record_color = "red"
                tables_critical.append(table_name)
            elif total_record > total_record_warning:
                total_record_color = "yellow"
                tables_warning.append(table_name)
            else:
                total_record_color = "transparent"

            if raw_record_in_delta > delta_record_critical:
                delta_record_color = "red"
                tables_critical.append(table_name)
            elif raw_record_in_delta > delta_record_warning:
                delta_record_color = "yellow"
                tables_warning.append(table_name)
            else:
                delta_record_color = "transparent"
            #'label'=value[UOM];[warn];[crit];[min];[max]
            perf += f" '{table_name}_total_records'={total_record};{total_record_warning};{total_record_critical};;{max_rows} "
            perf += f" '{table_name}_delta_records'={raw_record_in_delta};{delta_record_warning};{delta_record_critical};; "
            total_record_style = (
                f'style="border: 1px solid;background-color: {total_record_color};"'
            )
            delta_record_style = (
                f'style="border: 1px solid;background-color: {delta_record_color};"'
            )
            output_list.append(
                f"<tr><td {style}>{table_name}</td><td {style}>{partition_id}</td><td {total_record_style}>{total_record:,}</td><td {delta_record_style}>{raw_record_in_delta:,}</td></tr>"
            )
            output_text.append("")

        output_html = '<table style="border-collapse: collapse;">{}</table>'.format(
            "".join(output_list)
        )
        print(
            f"{resultat_status} - Critical tables {tables_critical} - Warning tables {tables_warning}\n{output_html}{perf}"
        )
        function_exit(resultat_status)

    if args.mode == "services":
        # check SAP HANA services
        command_sql = "SELECT SERVICE_NAME,ACTIVE_STATUS FROM SYS.M_SERVICES where IS_DATABASE_LOCAL='TRUE'"
        cursor.execute(command_sql)
        resultat = cursor.fetchall()
        # init variables
        resultat_all = ""
        resultat_status = ""
        resultat_control = 0
        for row in resultat:
            # Details about each services
            if row[0] == "indexserver":
                resultat_all = resultat_all + row[0] + ":" + row[1] + " [mandatory]\n"
            else:
                resultat_all = resultat_all + row[0] + ":" + row[1] + "\n"
            # Status
            if row[1] == "NO":
                resultat_control = resultat_control + 1
            else:
                resultat_control = resultat_control + 0
            if row[0] == "indexserver" and row[1] == "NO":
                resultat_control = resultat_control + 20
        if resultat_control == 0:
            resultat_status = "OK"
        elif resultat_control <= 1:
            resultat_status = "WARNING"
        elif resultat_control <= 20:
            resultat_status = "CRITICAL"
        print(
            "%s - SAP HANA Services. \n%s | 'services'=%s;1;20;0;100"
            % (resultat_status, resultat_all, resultat_control)
        )
        function_exit(resultat_status)

    if args.mode == "license_usage":
        warning = 80
        critical = 90
        if args.warning:
            warning = int(args.warning)
        if args.critical:
            critical = int(args.critical)

        # check SAP HANA license usage
        command_sql = "SELECT PRODUCT_LIMIT,PRODUCT_USAGE FROM SYS.M_LICENSE"
        cursor.execute(command_sql)
        resultat = cursor.fetchone()
        resultat_1 = int(resultat[0])
        resultat_0 = int(resultat[1])
        warning_gb = resultat_0 / 100 * warning
        critical_gb = resultat_0 / 100 * critical
        resultat_percentage = "{0:.0%}".format(1.0 * resultat_0 / resultat_1)
        resultat_per_num = float(resultat_percentage[:-1])
        if resultat_per_num <= warning:
            resultat_status = "OK"
        elif resultat_per_num >= critical:
            resultat_status = "CRITICAL"
        elif resultat_per_num > warning and resultat_per_num < critical:
            resultat_status = "WARNING"
        print(
            "%s - SAP HANA license (%s) : %s GB Usage / %s GB Limit | license_usage=%sGB;%s;%s;0;%s"
            % (
                resultat_status,
                resultat_percentage,
                resultat_0,
                resultat_1,
                resultat_0,
                warning_gb,
                critical_gb,
                resultat_1,
            )
        )
        function_exit(resultat_status)

    if args.mode == "db_data":
        # check SAP HANA data files disk usage
        function_check_M_SYSTEM_OVERVIEW("Disk", "Data", "Datafiles")

    if args.mode == "db_log":
        # check SAP HANA log files disk usage
        function_check_M_SYSTEM_OVERVIEW("Disk", "Log", "Logfiles")

    if args.mode == "db_trace":
        # check SAP HANA trace files disk usage
        function_check_M_SYSTEM_OVERVIEW("Disk", "Trace", "Tracefiles")

    if args.mode == "cpu":
        # check SAP HANA cpu
        function_check_M_SYSTEM_OVERVIEW("CPU", "CPU", "CPU")

    if args.mode == "mem_host":
        # check SAP HANA memory
        function_check_M_SYSTEM_OVERVIEW("Memory", "Memory", "Memory")

    if args.mode == "services_all":
        # check SAP HANA services
        function_check_M_SYSTEM_OVERVIEW("Services", "All Started", "Services")

    if args.mode == "plc_replication":
        critical = 24
        if args.critical:
            critical = int(args.critical)

        # check SAP PLC replication job
        command_sql = f'select * from "SAP_PLC_1"."sap.plc.db::map.t_scheduler_log";'
        cursor.execute(command_sql)
        results = cursor.fetchall()

        jobs_done = 0
        output = []
        jobs_last_run = None
        jobs_last_run_id = None
        for row in results:
            state = row.column_values[row.column_names.index("STATE")]
            if state in ("DONE"):
                finished_time = row.column_values[
                    row.column_names.index("FINISHED_TIME")
                ]
                run_id = row.column_values[row.column_names.index("RUN_ID")]
                if jobs_last_run == None or jobs_last_run < finished_time:
                    jobs_last_run = finished_time
                    jobs_last_run_id = run_id
                if datetime.now() - finished_time < timedelta(hours=critical):
                    jobs_done += 1
                    output.append(
                        f'Job {run_id} finished at {finished_time.strftime("%d/%m/%y %H:%M")}'
                    )

        if output:
            newline_char = "<br>"
            print(f"OK: {jobs_done} replication job(s) finished during the last {critical}h.\n{newline_char.join(output)}")
            return_state = "OK"
        else:
            last_run_text = ""
            if jobs_last_run:
                last_run_text = f'Last replication job run was {jobs_last_run.strftime("%d/%m/%y %H:%M")} with RUN_ID {jobs_last_run_id}'

            print(
                f"CRITICAL: No finished replication jobs found running for the last {critical}h. {last_run_text}"
            )
            return_state = "CRITICAL"

        function_exit(return_state)

    class AlertRatings(Enum):
        Information = 1
        Low = 2
        Medium = 3
        High = 4

    if args.mode == "alert":
        warning = 3
        critical = 4
        if args.critical:
            critical = int(args.critical)
        if args.warning:
            warning = int(args.warning)
        # Alert rating 1 = Information; 2 = Low; 3 = Medium; 4 = High
        command_sql = f"SELECT ALERT_RATING,ALERT_NAME,ALERT_DETAILS FROM _SYS_STATISTICS.STATISTICS_CURRENT_ALERTS WHERE ALERT_RATING >={warning}"
        cursor.execute(command_sql)
        resultat = cursor.fetchall()
        output = ""
        worst_rating = 1
        output_list = ["<tr><th>Rating</th><th>Name</th><th>Details</th></tr>"]
        state = "UNKNOWN"
        alert_counter = 0
        warning_counter = 0
        critical_counter = 0
        for row in resultat:
            rating = row[0]
            name = row[1]
            details = row[2]
            if rating > worst_rating:
                worst_rating = rating
            if rating == warning:
                warning_counter += 1
            if rating == critical:
                critical_counter += 1
            output_list.append(
                "<tr><td>{} ({})</td><td>{}</td><td>{}</td></tr>".format(
                    AlertRatings(rating).name, rating, name, details
                )
            )
            alert_counter += 1
        if worst_rating == warning:
            state = "WARNING"
        if worst_rating >= critical:
            state = "CRITICAL"
        if alert_counter == 0 or worst_rating < AlertRatings.Medium.value:
            state = "OK"
        output_html = "<table>{}</table>".format("".join(output_list))
        print(
            "{} - {} SAP HANA Alert(s) with rating level >= {} found ({} High / {} Medium).\n{}".format(
                state,
                alert_counter,
                AlertRatings(warning).name,
                critical_counter,
                warning_counter,
                output_html,
            )
        )
        function_exit(state)

    if args.mode == "sid":
        # check SAP HANA SID
        command_sql = (
            "select top 1 DATABASE_NAME from SYS.M_DATABASES where ACTIVE_STATUS='YES';"
        )
        cursor.execute(command_sql)
        resultat = cursor.fetchone()
        print("OK - SAP HANA SID : %s |\n|" % resultat)
    connection.commit()
    connection.close()
except Exception as e:
    print("CRITICAL: An error occured: {0}".format(repr(e)))
    function_exit("CRITICAL")
