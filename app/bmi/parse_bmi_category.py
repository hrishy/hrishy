from numpy import number
import pandas as pd
from pprint import pprint

from pandas.core.frame import DataFrame


class Bmi_Category_Manager:

    category_file: str
    bmi_range_column: str = "BMI Range (kg/m2)"
    above: number = 100  # very high risk just a huge number

    @classmethod
    def bmi_category(cls) -> DataFrame:
        df = pd.read_csv(cls.category_file)
        category_df = cls.bmi_category_df(df)
        return category_df

    @classmethod
    def bmi_category_df(cls, df: DataFrame):
        df[["Low", "Hi"]] = df[cls.bmi_range_column].str.split(
            " and | - ", expand=True)
        df["Hi"] = df["Hi"].str.replace("below", "0")
        df["Hi"] = df["Hi"].str.replace("above", str(cls.above))
        df = df.drop(cls.bmi_range_column, axis="columns")
        df[["Hi", "Low"]] = df[["Hi", "Low"]].apply(pd.to_numeric)
        df.loc[0, "Hi"] = df.loc[0, "Low"]
        df.loc[0, "Low"] = 0.0
        return df
