from pprint import pprint
from bmi.bmi_calculator import Bmi_Calculator
from timeit import default_timer as timer
from pathlib import Path


def main():
    start = timer()
    bmi_file_name = "bmi_5M.1.json"
    category_file = "bmi_category.csv"
    sample_data_dir = ".\sample_data"

    category_file = Path(sample_data_dir, category_file)
    bmi_file = Path(sample_data_dir, bmi_file_name)

    Bmi_Calculator.category_file = category_file
    Bmi_Calculator.bmi_file_name = bmi_file

    df = Bmi_Calculator.build_bmi_risk()
    pprint(df.head())
    elapsed_time = round(timer() - start, 0)

    print(
        f"Building BMI with Category for {len(df)} records took, {elapsed_time} seconds")


if __name__ == "__main__":
    main()
