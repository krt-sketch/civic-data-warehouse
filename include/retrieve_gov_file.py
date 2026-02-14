import shutil
import wget
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from zipfile import ZipFile
import subprocess, os
from airflow.utils.log import logging_mixin

def unpack_zip(path_to_zip, extract_path, logger):
    """
    Recursively unzips a file and outputs its content to a directory.
    :return: None
    """
    parent_archive = ZipFile(path_to_zip)
    parent_archive.extractall(extract_path)
    namelist = parent_archive.namelist()
    parent_archive.close()
    for name in namelist:
        try:
            if name.endswith(".zip"):
                unpack_zip(path_to_zip=extract_path + name, extract_path=extract_path, logger=logger)
                os.remove(extract_path + name)
                logger.info(f"{name} successfully unzipped")
        except:
            logger.error(f"{name} is not a .zip - full path : {extract_path + name}")
            pass


def upload_to_s3(s3_conn_id, filename, key, bucket, logger):
    """
    Uploads a target file to s3
    :return: None
    """
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3_hook.load_file(filename=filename, key=key, bucket_name=bucket, replace=True)
    logger.info(f"{filename} successfully uploaded to s3 in bucket {bucket} and with key {key}")

def retrieve_gov_file(filename, file_url, bucket, s3_conn_id, task_id):
    """
    Downloads a single file to a temporary directory, recursively unzips it, and uploads it to s3
    :return: none
    """
    logger = logging_mixin.LoggingMixin().logger()

    download_dest = "/tmp/" + filename
    wget.download(file_url, download_dest)
    logger.info(f"{download_dest} downloaded")

    prepped_dir = f"/tmp/prepped_{task_id}"[:50]
    logger.info(f"using tmp directory '{prepped_dir}'")

    # The directories can remain from run to run.
    logger.info("clearing tmp directory")
    for root, dirs, files in os.walk(prepped_dir):
        for file in files:
            file_to_remove = os.path.join(root, file)
            logger.debug(f"removing file {file_to_remove}")
            os.remove(file_to_remove)
    logger.info("tmp directory cleared")

    # KRT note : The original version now only uses the tmp_dir name with a slash on the end.
    #            In the interest of matching the original code, I'm not changing that - but it needs
    #             to be cleaned up later.
    prepped_dir = f"{prepped_dir}/"

    if filename.endswith(".zip"):
        unpack_zip(download_dest, prepped_dir, logger)
    else:
        shutil.move(
            download_dest,
            prepped_dir + os.path.basename(download_dest).split("/")[-1],
        )
    for file in os.listdir(prepped_dir):
        if file.endswith(".mdb"):
            export_mdb_file(logger, file, prepped_dir)

    for file in os.listdir(prepped_dir):
        logger.info(f"{file} found in {prepped_dir} for sending to S3")
        if file.endswith(".csv"):
            export_csv_file(bucket, s3_conn_id, logger, file, prepped_dir)

def export_csv_file(bucket, s3_conn_id, logger, file, prepped_dir):
    OBJECT = file.replace(" ", "")
    PATH_TO_FILE = prepped_dir + file
    BUCKET = bucket
    upload_to_s3(
                s3_conn_id=s3_conn_id,
                filename=PATH_TO_FILE,
                bucket=BUCKET,
                key=OBJECT,
                logger=logger
            )
    logger.info(f"{file} successfully loaded to S3")

def export_mdb_file(logger, file, prepped_dir):
    logger.info(f"{file} identified as .mdb")
    try:
        prepped_file = f"{prepped_dir}{file}"
        cmd_open_mdb = f"mdb-tables {prepped_file}"
        logger.info(f"executing command {cmd_open_mdb}")
        table_names = subprocess.Popen(
                    cmd_open_mdb,
                    stdout=subprocess.PIPE,
                    shell=True,
                )
        output = table_names.communicate()[0].decode("ascii")
        logger.debug(output)
        tables = output.split(" ")
        logger.debug(tables)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
                    "command '{}' return with error (code {}): {}".format(
                        e.cmd, e.returncode, e.output
                    )
                )
    for table in tables:
        if table != "" and table != "\n":
            export_file = (
                        prepped_dir
                        + os.path.splitext(file)[0]
                        + "_"
                        + table.replace(" ", "_")
                        + ".csv"
                    )
            logger.info(f"Exporting {table} to {export_file}")
            with open(export_file, "wb") as f:
                try:
                    subprocess.check_call(
                                ["mdb-export", prepped_file, table], stdout=f
                            )
                except subprocess.CalledProcessError as e:
                    raise RuntimeError(
                                "command '{}' return with error (code {}): {}".format(
                                    e.cmd, e.returncode, e.output
                                )
                            )
