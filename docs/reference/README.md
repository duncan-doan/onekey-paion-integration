# Reference documents

The source specifications for this integration. The files themselves are
**gitignored** — they are large vendor binaries and may be licence-restricted,
so they are not committed. Copy them here from the IQVIA delivery bundle.

## Expected files

| File | What it covers |
| --- | --- |
| `1. OneKey API - START HERE Guide.pdf` | Orientation; §6.1.1 lists the environment base URLs. |
| `2. OneKey Data Overview.pdf` | The OneKey data model in business terms. |
| `3. OneKey API Solution Overview.pdf` | How the API fits a client integration. |
| `4. Onekey API Developer Guide.pdf` | **The authority on request structure.** §5.2.2 authentication, §5.2.7 endpoints, §6.13 population refresh, §9.4 the full codBase list. |
| `5. OneKey API Mapping.xlsx` | Field-level mapping reference. |
| `7. OneKey Functionality Highlights.pdf` | Feature summary. |
| `8. OneKey API Sample Use.pdf` | Worked request/response examples. |
| `OneKey Functionality Highlights – VR Requirements.xlsx` | Validation-request requirements. |
| `Paion Account - Contact Standard Data Model.xlsx` | Target CRM schema: the `account` and `contact` attribute lists this integration maps onto. |

## Reading them

The PDFs are text-extractable:

```bash
pip install pypdf
python -c "
import pypdf
r = pypdf.PdfReader('docs/reference/4. Onekey API Developer Guide.pdf')
print(r.pages[11].extract_text())"
```
