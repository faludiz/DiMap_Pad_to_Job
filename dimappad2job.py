import sys
import os.path
import tempfile
from zipfile import ZipFile
import sqlite3
import shutil
import math
import uuid


def unzip_file(zip_file, extract_to):
    """Unzip the given file to the specified folder."""
    with ZipFile(zip_file, 'r') as zip_obj:
        zip_obj.extractall(path=extract_to)


def print_projects(tmpfolder):
    """Print all project names from the Job table in the database."""
    con = sqlite3.connect(tmpfolder + "/" + "roadJobDatabase.db")
    cur = con.cursor()
    sql = 'SELECT * FROM "Job";'
    for row in cur.execute(sql):
        print(row[1])
    con.close()


def get_work_id(folder, projname):
    """Retrieve the work ID for the given project name."""
    con = sqlite3.connect(folder + "/" + "roadJobDatabase.db")
    cur = con.cursor()

    sql = 'SELECT * FROM "Job" WHERE "name" IS "' + projname + '" LIMIT 1;'

    work_id = ""

    for row in cur.execute(sql):
        work_id = row[0]
    con.close()
    return work_id


def rad_dms(rad):
    """Convert radians to degrees, minutes, and seconds."""
    dd = math.degrees(rad)
    mult = -1 if dd < 0 else 1
    mnt, sec = divmod(abs(dd) * 3600, 60)
    deg, mnt = divmod(mnt, 60)
    return mult * (deg + mnt / 100 + sec / 10000)

def get_ts_data(info):
    """Extract TS information from the given data."""
    deviceModel = ""
    deviceSn = ""

    for i in range(len(info)):
        sdata = info[i].split(':')
        if len(sdata) > 1:
            if sdata[0] == "deviceModel":
                deviceModel = sdata[1]
            if sdata[0] == "deviceSn":
                deviceSn = sdata[1]
    return [deviceModel, deviceSn]

def get_station_data(info):
    """Extract station information from the given data."""
    stn = []
    name = ""
    hi = ""
    for i in range(len(info)):
        sdata = info[i].split(':')
        if len(sdata) > 1:
            if sdata[0] == "stationName":
                name = sdata[1]
            if sdata[0] == "hi":
                hi = sdata[1]
    stn.append(name)  #0
    stn.append(hi)  #1
    return stn


def get_observation_data(info):
    """Extract observation information from the given data."""
    obs = []
    ha = 0.0
    va = 0.0
    sd = 0.0
    hr = 0.0
    for i in range(len(info)):
        sdata = info[i].split(':')
        if len(sdata) > 1:
            if sdata[0] == "hr":
                hr = float(sdata[1])
            if sdata[0] == "ha":
                ha = rad_dms(float(sdata[1]))
            if sdata[0] == "va":
                va = rad_dms(float(sdata[1]))
            if sdata[0] == "sd":
                sd = float(sdata[1])
    obs.append(ha)  #0
    obs.append(va)  #1
    obs.append(sd)  #2
    obs.append(hr)  #3
    return obs


def save_job(folder, projname):
    """Save the job information to a file."""
    WorkID = get_work_id(folder, projname)
    con = sqlite3.connect(folder + "/" + "surveyDatabase.db")
    cur = con.cursor()
    sql = 'SELECT * FROM "Point" WHERE "work_id" IS "' + WorkID + '";'

    StationName = ""
    LastStationName = ""
    jobfn = projname + '.job'

    job = open(jobfn, "w")

    # header
    job.write("23=4112\n")
    job.write("50=%s\n" % (projname))

    for row in cur.execute(sql):
        pname = row[0]
        pcode = row[1]
        pinfo: str = row[13]
        pinfo = pinfo.replace('\\"', "")
        pinfo = pinfo.replace('\\', "")
        pinfo = pinfo.replace('"', "")
        pinfo = pinfo.replace('{', "")
        pinfo = pinfo.replace('}', "")
        sinfo = pinfo.split(',')
        # print(sinfo)

        for i in range(len(sinfo)):
            sdata = sinfo[i].split(':')
            if len(sdata) > 1:
                if sdata[0] == "stationName":
                    StationName = sdata[1]
                    if StationName != LastStationName:
                        nfo = get_ts_data(sinfo)
                        job.write("55=%s %s\n" % (nfo[0], nfo[1]))
                        nfo = get_station_data(sinfo)
                        job.write("2=%s\n" % (nfo[0]))
                        job.write("3=%s\n" % (nfo[1]))
                        LastStationName = StationName
        ## check if TS data is present
        if "TS:deviceType" in pinfo:
            nfo = get_observation_data(sinfo)
            job.write("5=%s\n" % (pname))  # pname
            job.write("4=%s\n" % (pcode))  # pcode
            prefix = ""
            if nfo[1] > 180:
                prefix = "1"
            job.write(prefix + "7=%0.5f\n" % (nfo[0]))  # ha
            job.write(prefix + "8=%0.5f\n" % (nfo[1]))  # va
            job.write("9=%0.4f\n" % (nfo[2]))  # sd
            job.write("6=%0.4f\n" % (nfo[3]))  # rh
    con.close()
    job.close()


def main():
    tmpfolder = tempfile.gettempdir() + "/" + uuid.uuid4().hex

    if ((len(sys.argv) <= 2)):
        print("DiMap Pad Backup to Job © Faludi Zoltán, INTELLIGEO Kft")
        print("")
        print("Usage:")
        print("")
        print("dimappad2job.py exported_backup_file.zip project_name")
        print("")
        print("Result Geodimeter Job file: project_name.job")
        print("")
        print("To list projects use the next command:")
        print("")
        print("dimappad2job.py --list exported_backup_file.zip")
        sys.exit(1)

    if ((len(sys.argv) >= 2)) and (sys.argv[1] == "--list"):
        zipfn = sys.argv[2]
        if os.path.exists(zipfn):
            unzip_file(zipfn, tmpfolder)
            print_projects(tmpfolder)
            # delete temporary folder
            shutil.rmtree(tmpfolder)
            print('list.done')
        sys.exit(0)

    if (len(sys.argv) >= 3):
        zipfn = sys.argv[1]
        projname = sys.argv[2]

        if os.path.exists(zipfn):
            unzip_file(zipfn, tmpfolder)
            save_job(tmpfolder, projname)
            # delete temporary folder
            shutil.rmtree(tmpfolder)
            print('job.done')
        sys.exit(0)


if __name__ == '__main__':
    main()
