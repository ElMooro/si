"""A minimal .xlsx writer for the tests: shared strings for text, plain values for numbers, gaps for None."""
import io
import zipfile
from xml.sax.saxutils import escape

M = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


def _col(i):
    s = ""
    i += 1
    while i:
        i, rem = divmod(i - 1, 26)
        s = chr(65 + rem) + s
    return s


def make_xlsx(sheets):
    """sheets = {name: [[cell, ...], ...]}; a cell is str, int, float or None."""
    strings, index = [], {}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("xl/workbook.xml", '<workbook xmlns="%s" xmlns:r="%s"><sheets>%s</sheets></workbook>' % (
            M, R, "".join('<sheet name="%s" sheetId="%d" r:id="rId%d"/>' % (escape(n), i + 1, i + 1) for i, n in enumerate(sheets))))
        z.writestr("xl/_rels/workbook.xml.rels", '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">%s</Relationships>' % "".join(
            '<Relationship Id="rId%d" Target="worksheets/sheet%d.xml"/>' % (i + 1, i + 1) for i in range(len(sheets))))
        for i, rows in enumerate(sheets.values()):
            body = []
            for r, row in enumerate(rows):
                cells = []
                for c, v in enumerate(row):
                    if v is None:
                        continue
                    ref = "%s%d" % (_col(c), r + 1)
                    if isinstance(v, str):
                        if v not in index:
                            index[v] = len(strings)
                            strings.append(v)
                        cells.append('<c r="%s" t="s"><v>%d</v></c>' % (ref, index[v]))
                    else:
                        cells.append('<c r="%s"><v>%r</v></c>' % (ref, v))
                body.append('<row r="%d">%s</row>' % (r + 1, "".join(cells)))
            z.writestr("xl/worksheets/sheet%d.xml" % (i + 1), '<worksheet xmlns="%s"><sheetData>%s</sheetData></worksheet>' % (M, "".join(body)))
        z.writestr("xl/sharedStrings.xml", '<sst xmlns="%s">%s</sst>' % (M, "".join("<si><t>%s</t></si>" % escape(s) for s in strings)))
    return buf.getvalue()
