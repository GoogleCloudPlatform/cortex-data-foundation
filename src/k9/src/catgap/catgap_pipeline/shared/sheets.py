# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Crafting Ads to Product Hierarchy Mapping spreadsheet."""

import logging
import typing

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import bigquery
from google.oauth2 import credentials

import gspread
import gspread_formatting

from auth import get_auth

# Mapping Spreadsheet name and location
_MAPPING_FILE_NAME = "SAPHierToAdsMapping"
_MAPPING_FOLDER = "Cortex CATGAP"

# Sheet names
HIER_SHEET_NAME = "Products Hierarchy"
MAPPING_SHEET_NAME = "Ads to Products Mapping"
JOURNAL_SHEET_NAME = "Cortex Ads AI Updates"

# Column names
_MAPPING_COLUMNS = [
    "Campaign Name",
    "Ad Group Name",
    "Add-Remove",
    "SAP Category",
]
_JOURNAL_COLUMNS = [
    "Mapping Date (UTC)",
    "Ad Campaign",
    "Ad Group"
]

MAPPING_COL_NUM = len(_MAPPING_COLUMNS)
JOURNAL_COL_NUM = len(_JOURNAL_COLUMNS)

PLUS_SIGN = "✚"  # This is a special '+' character
MINUS_SIGN = "━"  # This is a special '-' character


def get_mapping_journal_hier_sheets(
    spreadsheet: gspread.Spreadsheet
) -> typing.Tuple[gspread.Worksheet, gspread.Worksheet, gspread.Worksheet]:
    ws_mapping = None
    ws_journal = None
    ws_hier = None

    wss = spreadsheet.worksheets()

    # checking whether sheets exist
    ws_mappings = list(filter(lambda x: x.title == MAPPING_SHEET_NAME, wss))
    ws_journals = list(filter(lambda x: x.title == JOURNAL_SHEET_NAME, wss))
    ws_hiers = list(filter(lambda x: x.title == HIER_SHEET_NAME, wss))

    if len(ws_mappings) > 0:
        ws_mapping = ws_mappings[0]
    if len(ws_journals) > 0:
        ws_journal = ws_journals[0]
    if len(ws_hiers) > 0:
        ws_hier = ws_hiers[0]
    return (ws_mapping, ws_journal, ws_hier)


def format_mapping_spreadsheet(
    spreadsheet_url: str, creds: credentials.Credentials,
    prod_hiers: typing.List[str]
) -> typing.Tuple[gspread.Worksheet, gspread.Worksheet, gspread.Worksheet]:
    """Prepares a mapping spreadsheet with sheets, product hierarchy data,
        columns and formatting formatting.

    Args:
        spreadsheet_id (str): spreadsheet file id.
        creds (credentials.Credentials): credentials to use
        prod_hiers (typing.List[str]): product hierarchies list

    Returns:
        typing.Tuple[gspread.Worksheet, gspread.Worksheet, gspread.Worksheet]:
          a tuple of sheets with [mapping, changes journal, Product hierarchy]
    """

    client = gspread.authorize(creds)
    sheet = client.open_by_url(spreadsheet_url)
    wss = sheet.worksheets()

    ws_mapping, ws_journal, ws_hier = get_mapping_journal_hier_sheets(sheet)

    # If only one worksheet,
    # delete it unless it's one of the mapping sheets.
    remove_sheets = None
    if len(wss) == 1:
        remove_sheets = wss.copy()

    if not ws_mapping:
        # no mapping sheet, so add one
        ws_mapping = sheet.add_worksheet(MAPPING_SHEET_NAME, 2,
                                         MAPPING_COL_NUM)
    else:
        remove_sheets = None
        # making sure it has at least MAPPING_COL_NUM columns
        if ws_mapping.col_count < MAPPING_COL_NUM:
            ws_mapping.add_cols(MAPPING_COL_NUM - ws_mapping.col_count)
        if ws_mapping.row_count < 2:
            ws_mapping.add_rows(2 - ws_mapping.row_count)
    # insert top row of the mapping sheet with column names
    ws_mapping.update(
        [_MAPPING_COLUMNS],
        major_dimension="ROWS",
    )

    if not ws_journal:
        # no journal sheet, so add one
        ws_journal = sheet.add_worksheet(JOURNAL_SHEET_NAME, 2,
                                         JOURNAL_COL_NUM)
    else:
        remove_sheets = None
        # making sure it has at least JOURNAL_COL_NUM columns
        if ws_journal.col_count < JOURNAL_COL_NUM:
            ws_journal.add_cols(JOURNAL_COL_NUM - ws_journal.col_count)
        if ws_journal.row_count < 2:
            ws_journal.add_rows(2 - ws_journal.row_count)
    # insert top row of the journal sheet with column names
    ws_journal.update(
        [_JOURNAL_COLUMNS],
        major_dimension="ROWS",
    )

    if not ws_hier:
        # no journal hierarchy, so add one
        ws_hier = sheet.add_worksheet(HIER_SHEET_NAME, 1, 1)
    else:
        remove_sheets = None

    # remove extra sheets if needed.
    if remove_sheets:
        for s in remove_sheets:
            sheet.del_worksheet(s)

    # creating or updating the hierarchy, sorted
    prod_hiers_sorted = sorted(prod_hiers)

    # ensuring number of columns and rows
    hier_len = len(prod_hiers_sorted)
    if ws_hier.col_count < 1:
        ws_hier.add_cols(1)
    if ws_hier.row_count < hier_len:
        ws_hier.add_rows(hier_len - ws_hier.row_count)

    # adding hierarchy strings
    ws_hier.batch_update(
        [{
            "range": "A1:A",
            "majorDimension": "COLUMNS",
            "values": [prod_hiers_sorted],
        }],
        value_input_option=gspread.worksheet.ValueInputOption.raw,
    )

    # Now, working on data validation and formatting.

    # Working on the mapping sheet.

    # Category column data validation (dropdown list of hierarchies).
    rule_mapping = gspread_formatting.DataValidationRule(
        gspread_formatting.BooleanCondition(
            "ONE_OF_RANGE", [f"='{HIER_SHEET_NAME}'!A1:A{hier_len}"]),
        strict=True,
        showCustomUi=True,
    )
    gspread_formatting.set_data_validation_for_cell_range(
        ws_mapping, "D2:D", rule_mapping)
    # Add-Remove (+/-) column data validation (dropdown list with + and -).
    rule_add_remove = gspread_formatting.DataValidationRule(
        gspread_formatting.BooleanCondition("ONE_OF_LIST",
                                            [PLUS_SIGN, MINUS_SIGN]),
        strict=True,
        showCustomUi=True,
    )
    gspread_formatting.set_data_validation_for_cell_range(
        ws_mapping, "C2:C", rule_add_remove)

    # First row formatting
    gspread_formatting.format_cell_range(
        ws_mapping,
        "A1:D1",
        gspread_formatting.CellFormat(
            textFormat=gspread_formatting.TextFormat(
                fontFamily="Google Sans",
                bold=True,
                foregroundColor=gspread_formatting.Color(1, 1, 1),
                foregroundColorStyle=gspread_formatting.ColorStyle(
                    rgbColor=gspread_formatting.Color(1, 1, 1)),
            ),
            backgroundColorStyle=gspread_formatting.ColorStyle(
                rgbColor=gspread_formatting.Color(0, 0, 1)),
            backgroundColor=gspread_formatting.Color(0, 0, 1),
        ),
    )

    # Basic filter and frozen header row.
    ws_mapping.clear_basic_filter()
    ws_mapping.set_basic_filter()
    ws_mapping.freeze(1)

    # Conditional formatting for "-" (Unicode ━)
    rules = gspread_formatting.get_conditional_format_rules(ws_mapping)
    rules.clear()
    rule = gspread_formatting.ConditionalFormatRule(
        ranges=[gspread_formatting.GridRange.from_a1_range("C2:D", ws_mapping)],
        booleanRule=gspread_formatting.BooleanRule(
            condition=gspread_formatting.BooleanCondition(
                "CUSTOM_FORMULA",
                [{
                    "userEnteredValue":
                        f'=SEARCH("{MINUS_SIGN}",INDIRECT(ADDRESS(ROW(),3)))'
                }],
            ),
            format=gspread_formatting.CellFormat(
                textFormat=gspread_formatting.TextFormat(
                    foregroundColor=gspread_formatting.Color(1, 1, 1),
                    foregroundColorStyle=gspread_formatting.ColorStyle(
                        rgbColor=gspread_formatting.Color(1, 1, 1)),
                ),
                backgroundColorStyle=gspread_formatting.ColorStyle(
                    rgbColor=gspread_formatting.Color(0.8, 0, 0)),
                backgroundColor=gspread_formatting.Color(0.8, 0, 0),
            ),
        ),
    )
    rules.insert(len(rules) - 1, rule)

    # Conditional formatting for "+" (Unicode ✚)
    rule = gspread_formatting.ConditionalFormatRule(
        ranges=[gspread_formatting.GridRange.from_a1_range("C2:D", ws_mapping)],
        booleanRule=gspread_formatting.BooleanRule(
            condition=gspread_formatting.BooleanCondition(
                "CUSTOM_FORMULA",
                [{
                    "userEnteredValue":
                        f'=SEARCH("{PLUS_SIGN}",INDIRECT(ADDRESS(ROW(),3)))'
                }],
            ),
            format=gspread_formatting.CellFormat(
                textFormat=gspread_formatting.TextFormat(
                    foregroundColor=gspread_formatting.Color(1, 1, 1),
                    foregroundColorStyle=gspread_formatting.ColorStyle(
                        rgbColor=gspread_formatting.Color(1, 1, 1)),
                ),
                backgroundColorStyle=gspread_formatting.ColorStyle(
                    rgbColor=gspread_formatting.Color(0.41568628, 0.65882355,
                                                      0.30980393)),
                backgroundColor=gspread_formatting.Color(
                    0.41568628, 0.65882355, 0.30980393),
            ),
        ),
    )
    rules.insert(len(rules) - 1, rule)

    # Conditional formatting for campaign row color alternating.
    rule = gspread_formatting.ConditionalFormatRule(
        ranges=[gspread_formatting.GridRange.from_a1_range("A1:B", ws_mapping)],
        booleanRule=gspread_formatting.BooleanRule(
            condition=gspread_formatting.BooleanCondition(
                "CUSTOM_FORMULA",
                [{
                    "userEnteredValue": "=iseven(match($A1,unique($A$2:$A),0))"
                }],
            ),
            format=gspread_formatting.CellFormat(
                backgroundColorStyle=gspread_formatting.ColorStyle(
                    rgbColor=gspread_formatting.Color(0.8509804, 0.8509804,
                                                      0.8509804)),
                backgroundColor=gspread_formatting.Color(
                    0.8509804, 0.8509804, 0.8509804),
            ),
        ),
    )
    rules.insert(len(rules) - 1, rule)

    # Conditional formatting for ad group name text (bold/no bold) alternating.
    # rule = gspread_formatting.ConditionalFormatRule(
    #     ranges=[gspread_formatting.GridRange.from_a1_range("B2:B", ws_mapping)],
    #     booleanRule=gspread_formatting.BooleanRule(
    #         condition=gspread_formatting.BooleanCondition(
    #             "CUSTOM_FORMULA",
    #             [{
    #                 "userEnteredValue":
    #                     ("=INDIRECT(ADDRESS(ROW(),COLUMN())) \u003c\u003e"
    #                      " INDIRECT(ADDRESS(ROW()-1,COLUMN()))")
    #             }],
    #         ),
    #         format=gspread_formatting.CellFormat(
    #             textFormat=gspread_formatting.TextFormat(bold=True),),
    #     ),
    # )
    # rules.insert(len(rules) - 1, rule)

    rules.save()

    # Working on the journal.

    # First row formatting
    gspread_formatting.format_cell_range(
        ws_journal,
        "A1:C1",
        gspread_formatting.CellFormat(
            textFormat=gspread_formatting.TextFormat(
                fontFamily="Google Sans",
                bold=True,
                foregroundColor=gspread_formatting.Color(1, 1, 1),
                foregroundColorStyle=gspread_formatting.ColorStyle(
                    rgbColor=gspread_formatting.Color(1, 1, 1)),
            ),
            backgroundColorStyle=gspread_formatting.ColorStyle(
                rgbColor=gspread_formatting.Color(0, 0, 1)),
            backgroundColor=gspread_formatting.Color(0, 0, 1),
        ),
    )

    # Basic filter and frozen header row.
    ws_journal.clear_basic_filter()
    ws_journal.set_basic_filter()
    ws_journal.freeze(1)

    # Conditional formatting for the journal.
    rules = gspread_formatting.get_conditional_format_rules(ws_journal)
    rules.clear()

    rules.save()

    finish_spreadsheet(ws_mapping, ws_journal, ws_hier)

    return (ws_mapping, ws_journal, ws_hier)


def finish_spreadsheet(
    ws_mapping: gspread.Worksheet,
    ws_journal: gspread.Worksheet,
    ws_hier: gspread.Worksheet,
) -> None:
    """Finishes mapping sheet preparation, adding auto-resizing and sorting.

    Args:
        ws_mapping (gspread.Worksheet): Mapping sheet
        ws_journal (gspread.Worksheet): Journal sheet
        ws_hier (gspread.Worksheet): Hierarchy sheet
    """

    # Hierarchies column.
    ws_hier.spreadsheet.batch_update({
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": ws_hier.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": ws_hier.col_count,
                    }
                }
            },
            {
                "sortRange": {
                    "range": {
                        "sheetId": ws_hier.id,
                        "startRowIndex": 0,
                        "endRowIndex": ws_hier.row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": ws_hier.col_count,
                    },
                    "sortSpecs": [{
                        "sortOrder": "ASCENDING",
                        "dimensionIndex": 0
                    }],
                }
            },
        ]
    })

    # Mappings are sorted by Campaign and Ad Group names.
    ws_mapping.spreadsheet.batch_update({
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": ws_mapping.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": ws_mapping.col_count,
                    }
                }
            },
            {
                "sortRange": {
                    "range": {
                        "sheetId": ws_mapping.id,
                        "startRowIndex": 1,
                        "endRowIndex": ws_mapping.row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": ws_mapping.col_count,
                    },
                    "sortSpecs": [
                        {
                            "sortOrder": "ASCENDING",
                            "dimensionIndex": 0
                        },
                        {
                            "sortOrder": "ASCENDING",
                            "dimensionIndex": 1
                        },
                    ],
                }
            },
        ]
    })

    # Journal is sorted by date (descending), campaign name and ad group name.
    ws_journal.spreadsheet.batch_update({
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": ws_journal.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": ws_journal.col_count,
                    }
                }
            },
            {
                "sortRange": {
                    "range": {
                        "sheetId": ws_journal.id,
                        "startRowIndex": 1,
                        "endRowIndex": ws_journal.row_count,
                        "startColumnIndex": 0,
                        "endColumnIndex": ws_journal.col_count,
                    },
                    "sortSpecs": [
                        {
                            "sortOrder": "DESCENDING",
                            "dimensionIndex": 0
                        },
                        {
                            "sortOrder": "ASCENDING",
                            "dimensionIndex": 1
                        },
                        {
                            "sortOrder": "ASCENDING",
                            "dimensionIndex": 2
                        },
                    ],
                }
            },
        ]
    })


def _create_or_get_sheet(
    file_name: str, folder_name: str, creds: credentials.Credentials
) -> typing.Tuple[str, str, typing.Union[bool, HttpError]]:
    """Creates a new mapping spreadsheet.
    If spreadsheet exists, returns it.

    Args:
        file_name (str): spreadsheet name.
        folder_name (str): folder name (only first level folders allowed)
        creds (any): credentials to use.

    Returns:
        typing.Tuple[str, str, typing.Union[bool, HttpError]]: tuple with
            [file id, folder id,
                boolean on whether it's a new file (if true)
                or an HttpError if there was an error]
    """
    # pylint: disable=maybe-no-member
    try:
        drive_service = build("drive", "v3", credentials=creds)

        folder_id = None
        # Does the folder exist?
        response = (drive_service.files().list(
            q=("mimeType='application/vnd.google-apps.folder' and name="
               f"'{folder_name}'"),
            spaces="drive",
            fields="files(id)",
            pageToken=None,
        ).execute())
        for file in response.get("files", []):
            folder_id = file.get("id")
            break

        if not folder_id:
            # create a folder
            file_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            file = (drive_service.files().create(body=file_metadata,
                                                 fields="id").execute())
            folder_id = file.get("id")

        file_id = None
        response = (drive_service.files().list(
            q=("mimeType='application/vnd.google-apps.spreadsheet' and"
               f" name='{file_name}' and '{folder_id}' in parents"),
            spaces="drive",
            fields="files(id)",
            pageToken=None,
        ).execute())
        for file in response.get("files", []):
            file_id = file.get("id")
            break

        created_new = False

        if not file_id:
            service = build("sheets", "v4", credentials=creds)
            spreadsheet = {"properties": {"title": file_name}}
            spreadsheet = (service.spreadsheets().create(
                body=spreadsheet,
                fields="spreadsheetId,spreadsheetUrl").execute())
            file_id = spreadsheet.get("spreadsheetId")

            drive_service.files().update(
                fileId=file_id,
                addParents=folder_id,
            ).execute()
            created_new = True

        return (file_id, folder_id, created_new)
    except HttpError as error:
        print(f"ERROR: {error}")
        return (None, None, error)


def _get_shareable_link(
        file_or_folder_id: str,
        creds: credentials.Credentials) -> typing.Union[str, None]:
    """Creates a sharing link to a file or folder on Google Drive.

    Args:
        file_or_folder_id (str): file or folder id.
        creds (credentials.Credentials): credentials to use

    Returns:
        str: sharing URL or None if there was an error
    """
    # pylint: disable=maybe-no-member
    try:
        service = build("drive", "v3", credentials=creds)

        request_body = {
            "role": "writer",
            "type": "anyone",
        }

        _ = (service.permissions().create(fileId=file_or_folder_id,
                                          body=request_body).execute())

        response_share_link = (service.files().get(
            fileId=file_or_folder_id, fields="webViewLink").execute())
        url = response_share_link.get("webViewLink")

        return url
    except HttpError as error:
        print(f"ERROR: {error}")
        return None


def get_mapping_sheet(project_id: str, customer_and_mandt: str,
                      service_account_principal: str) -> str:
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
    creds = get_auth(project_id, service_account_principal)

    spreadsheet_id, _, new_flag_or_http_error = _create_or_get_sheet(
        f"{_MAPPING_FILE_NAME}-{customer_and_mandt}", _MAPPING_FOLDER, creds)
    if not spreadsheet_id:
        raise new_flag_or_http_error
    sheet_url = _get_shareable_link(spreadsheet_id, creds)

    return sheet_url


def prepare_mapping_spreadsheet(sheet_url: str,
                                project_id: str,
                                processing_dataset: str,
                                location: str,
                                service_account_principal: str,
                                user_email: str = None):
    prod_hiers: typing.List[str] = []
    with bigquery.Client(project=project_id, location=location) as client:
        query = ("SELECT fullhier FROM "
                 f"`{project_id}.{processing_dataset}.prod_hierarchy`")
        query_job = client.query(query)
        rows = query_job.result()
        for row in rows:
            prod_hiers.append(str(row["fullhier"]))

    creds = get_auth(project_id, service_account_principal)
    ws_mapping, ws_journal, ws_hier = format_mapping_spreadsheet(
        sheet_url, creds, prod_hiers)
    finish_spreadsheet(ws_mapping, ws_journal, ws_hier)

    if user_email:
        ws_hier.spreadsheet.share(user_email, "group", "writer", notify=True)
