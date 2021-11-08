import dask.dataframe as dd
from dask.dataframe.core import DataFrame


class Bmi_Manager():

    bmi_file_name: str

    @classmethod
    def calculate_bmi(cls, df: DataFrame, heightCm='HeightCm', weightKg='WeightKg') -> DataFrame:
        return df.assign(BMI=round(df[weightKg] * 100 / df[heightCm], 1))

    @classmethod
    def build_bmi_df(cls) -> DataFrame:
        bmi_df: DataFrame = dd.read_json(cls.bmi_file_name, orient=str)
        return bmi_df.map_partitions(cls.calculate_bmi)
