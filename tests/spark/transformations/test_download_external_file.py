import pytest

from requests_mock.mocker import Mocker

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.download_external_file import DownloadExternalFile, download_url

pytestmark = pytest.mark.spark

@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # case 0 : no target_column -> should replace the original column
            dict(download_external_file_column="download_url", filename_column="dynamic_filename", upload_location_column="upload_location", output_location_column="upload_url"),
            # expected output
            [
                dict(id=1, download_url="https://duckduckgo.com/", dynamic_filename="file_number_1.txt", upload_location="/tmp/download_test/", upload_url="/tmp/download_test/file_number_1.txt"),
                dict(id=2, download_url="https://duckduckgo.com/", dynamic_filename="file_number_2.txt", upload_location="/tmp/download_test/", upload_url="/tmp/download_test/file_number_2.txt"),
                dict(id=3, download_url="https://duckduckgo.com/", dynamic_filename="file_number_3.txt", upload_location="/tmp/download_test/", upload_url="/tmp/download_test/file_number_3.txt"),
            ],
        )
    ],
)
def test_base(input_values, expected, spark):
    input_df = spark.createDataFrame([[1, "https://random.url.todownload/file.txt", "file_number_1.txt", "/tmp/download_test"], [2, "https://random.url.todownload/file.txt", "file_number_2.txt", "/tmp/download_test"], [3, "https://random.url.todownload/file.txt", "file_number_3.txt", "/tmp/download_test"]], ["id", "download_url", "dynamic_filename", "upload_location"])
    with Mocker() as m:
        m.get("htts://random.url.todownload/file.txt", content=b"ok", status_code=int(200))
        df = DownloadExternalFile(**input_values).transform(input_df)

        df.show()

        actual = [k.asDict() for k in df.collect()]
        print(actual)
        assert actual == expected

def test_download_url():
    with Mocker() as m:
        m.get("https://random.url.todownload/file.txt", content=b"ok", status_code=int(200))
        assert download_url("https://random.url.todownload/file.txt", "/tmp/download_test/", "file_number_1.txt") == "/tmp/download_test/file_number_1.txt"