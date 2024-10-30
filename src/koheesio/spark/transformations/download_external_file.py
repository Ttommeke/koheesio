"""
This module defines the DownloadColumn class, a subclass of ColumnsTransformation.
"""
from typing import List, Literal, Optional, Union
from koheesio.models import Field
from koheesio.spark.transformations import ColumnsTransformation
from pyspark.sql import functions as f
from pathlib import Path
import requests
from pyspark.sql.column import Column


@f.udf
def download_url_ufd(url: str, basepath: str, filename: str) -> str:
    download_url(url, basepath, filename)

def download_url(url: str, basepath: str, filename: str) -> str:
    """
    input:
        - presigned_url
        - basepath of filesystem
        - filename to save

    output: filepath

    This udf will save the snapshot file to volume path if download succeeds
    and return filepath, if download unsuccessfull the reason code and
    reason will be returned.
    """
    basepath = Path(basepath)
    if not basepath.exists():
        basepath.mkdir(parents=True, exist_ok=True)
    full_path = f"{basepath}/{filename}"
    response = requests.get(url)
    if response.status_code == 200:
        with open(full_path, "wb") as file:
            file.write(response.content)
            return full_path
    else:
        return f"{response.status_code} - {response.reason}"

class DownloadExternalFile(ColumnsTransformation):

    download_external_file_column: str = Field(
        description="The column containing the download link.", 
    )
    filename_column: Optional[str] = Field(
        description="Column with the name of the file to be uploaded.", 
    )
    upload_location_column: Union[str] = Field(
        description="Column with the locaion of the file to be uploaded.", 
    )
    output_location_column: str = Field(
        default="uploaded_url",
        description="Output column containing the location of the uploaded file.",
    )

    def execute(self):

        filename = f.col(self.filename_column) if self.filename_column else f.expr("uuid()")

        self.df = self.df.withColumn(self.output_location_column, download_url_ufd(f.col(self.download_external_file_column), f.col(self.upload_location_column), filename))

        self.output.df = self.df
