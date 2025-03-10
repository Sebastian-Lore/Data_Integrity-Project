import pandas as pd
import pandera as pa
from pandera import Column, Check

# define schema
alarm_schema = pa.DataFrameSchema({
    "alarm_id": Column(int),
    "timestamp": Column(str, Check.str_matches(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"), nullable=False),
    "site_id": Column(int),
    "alarm_code": Column(str, Check.str_matches(r"^ALM\d+$"), nullable=False),
    "severity": Column(str, Check.isin(["Critical", "Major", "Minor", "Warning"])),
    "status": Column(str, Check.isin(["New", "Acknowledged", "Resolved"]))
})

# function to validate CSV data
def validate_csv(file_path):
    df = pd.read_csv(file_path)
    try:
        alarm_schema.validate(df)
    except pa.errors.SchemaError as e:
        # handle specific validation errors and print messages
        failure_cases = e.failure_cases
        if not failure_cases.empty:
            print("Validation errors found:")

            # loop through failure cases and handle each type
            for _, error in failure_cases.iterrows():
                print(failure_cases)
                print("")

                failure = error["failure_case"]
                index = error["index"]
                # print(f"This is failure: {failure}")
                # print(f"This is index: {index}")

    except Exception as ex:
        print(f"An error occurred: {ex}")

if __name__ == "__main__":
    validate_csv("C:/Users/slore/Desktop/Python Projects/data_integrity/data.csv")
