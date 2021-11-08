import pytest
import dask.dataframe as dd
import pandas as pd
from dask.dataframe.core import DataFrame, Series
import pytest
from app.bmi.bmi_manager import Bmi_Manager
from app.bmi.parse_bmi_category import Bmi_Category_Manager
from app.bmi.bmi_calculator import Bmi_Calculator


def test_calculate_bmi(mock_bmi_df: DataFrame) -> None:
    df = mock_bmi_df.map_partitions(Bmi_Manager.calculate_bmi)
    bmi = df['BMI'][0].compute()[0]
    assert bmi == 44.4


# def test_category_index(category_df: DataFrame) -> None:
#     category_df.index = Bmi_Calculator.category_index(category_df)


def test_all_required_columns(mock_bmi_df: DataFrame, mock_category_df: DataFrame) -> None:
    bmi_df_with_bmi = mock_bmi_df.map_partitions(Bmi_Manager.calculate_bmi)
    mock_category_df.index = Bmi_Calculator.category_index(mock_category_df)
    Bmi_Calculator.category_df = mock_category_df
    bmi_result_df = Bmi_Calculator.map_and_build_bmi_risk(bmi_df_with_bmi)
    bmi = bmi_result_df['BMI'][0].compute()[0]
    category = bmi_result_df['Category'][0].compute()[0]
    risk = bmi_result_df['Risk'][0].compute()[0]

    errors = []

    if not bmi == 44.4:
        message = "For the given Weight and Height BMI should be 44.4"
        errors.append(message)

    if not category == 'Very severly obese':
        message = "For the given weight and Height category should be Very severly obese"
        errors.append(message)

    if not risk == 'Very high risk':
        message = "For the given weight and Height risk should be Very high risk"

    assert not errors, f"errors occured:{'chr(92)'.join(errors)}"


@pytest.fixture()
def mock_bmi_df() -> DataFrame:
    df = pd.DataFrame({"Gender": ["Male"],
                       "HeightCm": [180],
                      "WeightKg": [80]
                       })
    bmi_df: DataFrame = dd.from_pandas(df, npartitions=1)
    return bmi_df


@pytest.fixture()
def mock_category_df() -> DataFrame:
    df = pd.DataFrame(
        {"BMI Category":
         ["Underweight", "Normal weight", "Overwright",
             "Moderately Obese", "Severly obese", "Very severly obese"],
         "Hi": [18.4, 24.9, 29.9, 34.9, 39.9, 100],
         "Low": [0, 18.5, 25, 30, 35, 40],
         "Health risk": ["Malnutrition risk", "Low risk", "Enhanced risk",
                         "Medium risk", "High risk", "Very high risk"]
         })
    return df
