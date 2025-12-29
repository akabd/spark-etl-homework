import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    src_path = os.path.join(project_root, "src")

    os.environ["PYTHONPATH"] = src_path + os.pathsep + os.environ.get("PYTHONPATH", "")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("spark-etl-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraClassPath", src_path)
        .config("spark.executorEnv.PYTHONPATH", os.environ["PYTHONPATH"])
        .config("spark.driver.extraJavaOptions", f"-Duser.dir={project_root}")
        .getOrCreate()
    )
    yield spark
    spark.stop()
