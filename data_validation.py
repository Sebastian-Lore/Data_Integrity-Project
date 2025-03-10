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

# function to validate CSV data
def validate_csv(file_path):
    try:
        df = pd.read_csv(file_path)

        # store the original timestamp column
        df["original_timestamp"] = df["timestamp"]

        # convert timestamp to datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce")

        # find invalid timestamps (rows where conversion failed)
        invalid_timestamps = df[df["timestamp"].isna()]
        if not invalid_timestamps.empty:
            print("Invalid timestamps found:")
            print(invalid_timestamps[["alarm_id", "original_timestamp"]])  # Show the original timestamps

        # remove invalid timestamps to prevent schema validation failure
        df = df.dropna(subset=["timestamp"])

        # convert valid timestamps back to string for Pandera validation
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # drop the helper column before final validation
        df = df.drop(columns=["original_timestamp"])

        # validate schema
        validated_df = alarm_schema.validate(df)
        print("CSV data is valid (except for reported timestamp errors).")

    except pa.errors.SchemaError as e:
        # handle specific validation errors and print messages
        failure_cases = e.failure_cases
        if not failure_cases.empty:
            print("Validation errors found:")

            # loop through failure cases and handle each type
            for _, error in failure_cases.iterrows():
                column = error["failure_case"]
                
                # get the row index of the failure case in the original DataFrame
                failing_row = df.iloc[error.name]
                row_index = failing_row.name + 2  # Adding 2 to match the row index of the CSV file

                print(f"Row {row_index} failure_case: {column}")
                
                # print based on the failure case type
                if "timestamp" in column:
                    print("Invalid timestamps found:")
                elif "alarm_code" in column:
                    print("Invalid alarm_code found:")
                elif "severity" in column:
                    print("Invalid severity found:")
                elif "status" in column:
                    print("Invalid status found:")
                else:
                    print("Invalid data found:")

    except Exception as ex:
        print(f"An error occurred: {ex}")

if __name__ == "__main__":
    validate_csv("C:/Users/slore/Desktop/Python Projects/data_integrity/data.csv") 
