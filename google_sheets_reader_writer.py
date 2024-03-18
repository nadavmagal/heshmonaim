import pygsheets
import pandas as pd


class GoogleSheetReaderWriter:
    def __init__(
        self, spreadsheet_name: str, credentials_file: str = "", sheet_name: str = ""
    ):
        if not credentials_file:
            raise Exception("GoogleSheetReaderWriter: Credentials file not specified")
        self.spreadsheet_name = spreadsheet_name
        self.client = pygsheets.authorize(service_file=credentials_file)
        self.spreadsheet = self.client.open(self.spreadsheet_name)

    def write_cells(self, df: pd.DataFrame, sheet_name: str):
        # Clear the existing content of the worksheet
        try:
            worksheet = self.spreadsheet.worksheet_by_title(sheet_name)
        except pygsheets.exceptions.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(sheet_name)

        worksheet.clear()
        worksheet.set_dataframe(df, "A1")

        # Get the headers from the first dictionary in the list
        # Write the headers to the worksheet
        # headers = [df.columns.tolist()]
        # worksheet.update_values('A1', headers)
        # worksheet.update_values('A2', df.values.tolist())

    def delete_worksheet(self, sheet_name):
        worksheet = self.spreadsheet.worksheet_by_title(sheet_name)
        self.spreadsheet.del_worksheet(worksheet)



    def update_row(self, row: pd.DataFrame, sheet_name: str):
        worksheet = self.spreadsheet.worksheet_by_title(sheet_name)
        cellrow = worksheet.find(
            str(row["id"].values[0]), matchEntireCell=True
        )  # get row by id
        rownum = cellrow[0].row
        # worksheet.get_values(f'A{rownum}', f'Z{rownum}')
        last_col = chr(64 + max(row.shape))
        worksheet.update_values(
            crange=f"A{rownum}:{last_col}{rownum}",
            values=[row.values.flatten().tolist()],
            majordim="ROWS",
        )

    def read_cells(self):
        df = pd.DataFrame()
        for worksheet in self.spreadsheet.worksheets():
            df = pd.concat([df, worksheet.get_as_df()], ignore_index=True)
        return df


def main():
    # Example usage
    spreadsheet_id = "your_spreadsheet_id"
    sheet_name = "Sheet1"

    reader_writer = GoogleSheetReaderWriter(spreadsheet_id)

    # Read cells from the sheet
    cells = reader_writer.read_cells(sheet_name)
    print(cells)

    # Perform differential updating based on a new list of cells
    new_cells = [
        {"row": 2, "column": 1, "value": "New Value 1"},
        {"row": 3, "column": 2, "value": "New Value 2"},
    ]
    reader_writer.update_cells(sheet_name, new_cells)


if __name__ == "__main__":
    main()
