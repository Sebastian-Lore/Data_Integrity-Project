# imports
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

# validate CSV data
def validate_csv(file_path):
    df = pd.read_csv(file_path)
    try:
        alarm_schema.validate(df, lazy=True) # enable lazy validation to catch all errors at once
    except pa.errors.SchemaErrors as e:  # use "SchemaErrors" not "SchemaError"
        failure_cases = e.failure_cases
        if not failure_cases.empty:
            print("\nValidation errors found:\n")
            
            for _, error in failure_cases.iterrows():
                index = error["index"]
                failure_case = error["failure_case"]
                
                failing_row = df.iloc[index]

                for column in alarm_schema.columns:
                    if failure_case == failing_row[column]:
                        print(f"\nColumn '{column}' failed validation:")
                        print(f"  alarm_id: {failing_row['alarm_id']}")
                        print(f"  {column}: {failing_row[column]}")
                        print(f"  Failure: {failure_case}\n")
                        break

    except Exception as ex:
        print(f"An error occurred: {ex}")


if __name__ == "__main__":
    validate_csv("C:/Users/slore/Desktop/Python Projects/data_integrity/data.csv")