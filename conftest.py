import pytest
from lib.utils import get_spark_session

@pytest.fixture
def spark():
    spark_session =  get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()


@pytest.fixture
def expected_results(spark):
    result_schema = "state string, count int"
    return spark.read \
        .format("csv") \
        .schema(result_schema) \
        .load("data/test_results/state_aggregate.csv")