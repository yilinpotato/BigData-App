"""把 report/report.md 转成 report/report.pdf。
依赖(用户级): markdown, weasyprint；中文字体: ~/.local/share/fonts (msyh/simhei)。
运行: python report/report/build_pdf.py  (或 python report/build_pdf.py)
"""
import os
from urllib.parse import quote
import markdown
from weasyprint import HTML

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
MD = os.path.join(HERE, "report.md")
PDF = os.path.join(HERE, "report.pdf")

with open(MD, encoding="utf-8") as f:
    text = f.read()

html_body = markdown.markdown(
    text, extensions=["tables", "fenced_code", "toc", "sane_lists"]
)

# 把 ../figures/ 相对路径换成绝对 file:// URL（仓库路径含空格，需 url 编码）
fig_url = "file://" + quote(os.path.join(REPO, "figures")) + "/"
html_body = html_body.replace('src="../figures/', f'src="{fig_url}')

CSS = """
@page { size: A4; margin: 1.8cm 1.7cm; }
body { font-family: 'Microsoft YaHei','SimHei',sans-serif; font-size: 10.5pt;
       line-height: 1.55; color: #222; }
h1 { font-size: 19pt; border-bottom: 2px solid #1f77b4; padding-bottom: 6px; }
h2 { font-size: 14pt; color: #1f4e79; border-bottom: 1px solid #ccc;
     padding-bottom: 3px; margin-top: 18px; }
h3 { font-size: 11.5pt; color: #1f77b4; margin-top: 12px; }
table { border-collapse: collapse; width: 100%; font-size: 9.3pt; margin: 8px 0; }
th, td { border: 1px solid #bbb; padding: 4px 7px; text-align: left; }
th { background: #eef3f8; }
img { max-width: 88%; display: block; margin: 8px auto; }
code, pre { font-family: 'DejaVu Sans Mono',monospace; font-size: 8.8pt;
            background: #f5f5f5; }
pre { padding: 8px; border-radius: 4px; white-space: pre-wrap; }
blockquote { color: #555; border-left: 3px solid #ccc; padding-left: 10px; margin-left: 0; }
"""

full = f"<html><head><meta charset='utf-8'><style>{CSS}</style></head><body>{html_body}</body></html>"
HTML(string=full, base_url=HERE).write_pdf(PDF)
print("wrote", PDF, "(%.1f KB)" % (os.path.getsize(PDF) / 1024))
