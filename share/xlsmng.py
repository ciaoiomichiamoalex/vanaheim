from openpyxl.styles import Font
import datetime
import openpyxl

DEFAULT_FONT = 'Aptos Narrow'
FORMATS = {
    str: '@',
    int: '#,##0',
    float: '#,##0.00',
    datetime.date: 'dd/mm/yyyy',
    datetime.time: 'h:mm:ss;@',
    datetime.datetime: 'dd/mm/yyyy h:mm:ss;@'
}


def write_excel(fou: str, rows: list, sheet_name: str = None, header: list[str] = None, font_face: str = None) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    if sheet_name:
        ws.title = sheet_name

    if header:
        for col_num, name in enumerate(header, start=1):
            ws.cell(row=1, column=col_num).value = name
            ws.cell(row=1, column=col_num).font = Font(name=font_face if font_face else DEFAULT_FONT, bold=True)
            ws.cell(row=1, column=col_num).number_format = FORMATS[type(name)]

    for row_num, row in enumerate(rows, start=2 if header else 1):
        for col_num, col in enumerate(row, start=1):
            ws.cell(row=row_num, column=col_num).value = col
            ws.cell(row=row_num, column=col_num).font = Font(name=font_face if font_face else DEFAULT_FONT)
            ws.cell(row=row_num, column=col_num).number_format = FORMATS[type(col)]

    if header:
        ws.auto_filter.ref = ws.dimensions
    wb.save(fou)
