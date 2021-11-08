from .parse_bmi_category import Bmi_Category_Manager
from .bmi_manager import Bmi_Manager
from dask.dataframe.core import DataFrame
import pandas as pd


class Bmi_Calculator():

    output_columns = ['Category', 'Risk']
    columns_to_consider = ['BMI Category', 'Health risk']
    bmi_file_name: str
    category_file: str
    category_df: DataFrame
    bmi_df: DataFrame

    @classmethod
    def compute_bmi(cls, df: DataFrame) -> DataFrame:
        df = df.copy()
        df[cls.output_columns] = df['BMI'].apply(
            lambda x: cls.category_df.iloc[cls.category_df.index.get_loc(x)][cls.columns_to_consider])
        return df

    @classmethod
    def setup(cls):
        Bmi_Category_Manager.category_file = cls.category_file
        Bmi_Manager.bmi_file_name = cls.bmi_file_name
        cls.category_df: DataFrame = Bmi_Category_Manager.bmi_category()
        cls.bmi_df: DataFrame = Bmi_Manager.build_bmi_df()

    @classmethod
    def build_bmi_risk(cls) -> DataFrame:
        cls.setup()
        cls.category_df.index = cls.category_index(cls.category_df)
        df = cls.map_and_build_bmi_risk(cls.bmi_df)
        return df

    @classmethod
    def map_and_build_bmi_risk(cls, bmi_df: DataFrame) -> DataFrame:
        df = bmi_df.map_partitions(cls.compute_bmi)
        return df

    @classmethod
    def category_index(cls, category_df: DataFrame) -> DataFrame:
        cls.category_df = category_df
        return pd.IntervalIndex.from_arrays(
            category_df['Low'], category_df['Hi'], closed='both')
