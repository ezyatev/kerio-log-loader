import argparse
import datetime
import logging
import multiprocessing
import os
import re
import shutil
import tempfile

import dotenv
import pymysql
import pytz

dotenv.load_dotenv()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", "-f", type=str,
                        required=True, help="path to log file")
    parser.add_argument("--numprocs", "-p", type=int, default=2 * multiprocessing.cpu_count(),
                        help="number of processes")
    parser.add_argument("--numrows", "-r", type=int, default=100,
                        help="number of rows for INSERT statement")
    parser.add_argument("--chunksize", "-s", type=int, default=20000,
                        help="number of lines for chunk file")
    return parser.parse_args()


def get_chunk_path(tmpdir, fid):
    return os.path.join(tmpdir, f"chunk{fid}.log")


def load_chunk(chunk, numrows):
    """
    We need one pysql.connect() for each process/thread. 
    As far as I know that's the only way to fix it. 
    PyMySQL is not thread safe, so the same connection 
    can't be used across multiple process/thread.
    """
    logging.info("Loading chunk from PID %d", os.getpid())
    connection = pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", default=3306)),
        user=os.getenv("DB_USER", "log"),
        passwd=os.getenv("DB_PASSWD"),
        db=os.getenv("DB_NAME", "log"),
        autocommit=True)
    try:
        process(connection, chunk, numrows)
    except:
        logging.exception('An error occurred while loading the chunk')
    finally:
        connection.close()


def split_to_chunks(tmpdir, filename, chunksize):
    chunks = []
    with open(filename) as infile:
        fid = 1
        chunk = get_chunk_path(tmpdir, fid)
        f = open(chunk, "w")
        chunks.append(chunk)
        for i, line in enumerate(infile):
            f.write(line)
            if not i % chunksize:
                f.close()
                fid += 1
                chunk = get_chunk_path(tmpdir, fid)
                f = open(chunk, "w")
                chunks.append(chunk)
        f.close()
    return chunks


def insert_rows(connection, values):
    with connection.cursor() as cursor:
        sql = "INSERT INTO `log`(ip, dt, url) VALUES {};\n".format(
            ",".join(values))
        cursor.execute(sql)


def parse_line(line):
    match = re.match((r"\b(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b.*?"
                      r"\[(?P<dt>\d{2}\/\w{3}\/\d{4}:\d+:\d{2}:\d{2}\s[\+\-]\d{4})\]"
                      r"\s\"\w+\s(?P<url>.+?)\s"), line)
    if not match:
        raise ValueError(f"An error occurred while parsing the line:\n{line}")
    return match


def get_datetime(datetime_str):
    """
    MariaDB doesn't support timezones with it's DATETIME type.
    """
    return (datetime.datetime.strptime(datetime_str, "%d/%b/%Y:%H:%M:%S %z")
            .astimezone(pytz.timezone("utc"))
            .replace(tzinfo=None))


def get_url(url):
    return pymysql.escape_string(url)[:500]


def process(connection, chunk, numrows):
    with open(chunk, "r") as f:
        values = []
        for i, line in enumerate(f):
            match = parse_line(line)
            ip = match.group("ip")
            dt = get_datetime(match.group("dt"))
            url = get_url(match.group("url"))
            values.append(f"('{ip}', '{dt}', '{url}')")

            if i != 0 and i % numrows == 0:
                insert_rows(connection, values)
                values = []

        if values:
            insert_rows(connection, values)


def main():
    args = get_args()
    pool = multiprocessing.Pool(processes=args.numprocs)
    tmpdir = tempfile.mkdtemp()

    chunks = split_to_chunks(tmpdir, args.file, args.chunksize)
    for chunk in chunks:
        pool.apply_async(load_chunk, (chunk, args.numrows))

    pool.close()
    pool.join()

    shutil.rmtree(tmpdir)


if __name__ == "__main__":
    main()
