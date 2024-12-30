import pandas as pd
import sys
import uuid


def generate_output_filename(input_file: str) -> str:
    random_id = str(uuid.uuid4())[:6]
    return input_file.rsplit(".", 1)[0] + f"_processed_{random_id}.csv"


def create_state_county_column(df: pd.DataFrame) -> pd.DataFrame:
    df["State_County"] = df["STNAME"] + "_" + df["CTYNAME"]
    return df


def get_summable_columns(df: pd.DataFrame, id_cols: list) -> list:
    potential_sum_cols = [
        col for col in df.columns if col not in id_cols and col != "AGEGRP"
    ]
    return (
        df[potential_sum_cols]
        .select_dtypes(include=["int64", "float64"])
        .columns.tolist()
    )


def aggregate_age_groups(
    df: pd.DataFrame, id_cols: list, sum_cols: list
) -> pd.DataFrame:
    grouped = (
        df.groupby(id_cols)
        .agg({"AGEGRP": lambda x: "5+6+7", **{col: "sum" for col in sum_cols}})
        .reset_index()
    )

    final_cols = [
        "SUMLEV",
        "STATE",
        "COUNTY",
        "STNAME",
        "State_County",
        "CTYNAME",
        "YEAR",
        "AGEGRP",
    ] + sum_cols
    return grouped[final_cols]


def print_summary(result: pd.DataFrame, original_shape: tuple, output_file: str):
    print(f"\nProcessing complete:")
    print(f"Original shape: {original_shape}")
    print(f"New shape: {result.shape}")
    print(f"\nSample of processed data (first 2 rows):")
    print(
        result[
            ["State_County", "YEAR", "AGEGRP", "TOT_POP", "TOT_MALE", "TOT_FEMALE"]
        ].head(2)
    )
    print(f"\nOutput saved to: {output_file}")


def process_county_data(input_file: str):
    df = pd.read_csv(input_file)
    output_file = generate_output_filename(input_file)

    id_cols = ["SUMLEV", "STATE", "COUNTY", "STNAME", "CTYNAME", "State_County", "YEAR"]

    df = create_state_county_column(df)
    sum_cols = get_summable_columns(df, id_cols)
    result = aggregate_age_groups(df, id_cols, sum_cols)

    result.to_csv(output_file, index=False)
    print_summary(result, df.shape, output_file)


def main():
    if len(sys.argv) != 2:
        print("Usage: python process_csv.py <input_file>")
        sys.exit(1)

    process_county_data(sys.argv[1])


if __name__ == "__main__":
    main()
