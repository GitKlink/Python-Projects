# Python-Projects

Standalone Python utilities and prototypes.

## IPM scenario optimiser

Files:
- `ipm_scenario_optimizer.py` — Python V1 scenario optimisation engine.
- `IPM_Scenario_Input_Template.xlsx` — runnable input/config template with synthetic placeholder data.

The workbook contains three named input tables:
- `tblWorkers`
- `tblScopeConfig`
- `tblScenarioConfig`

Example:

```bash
python ipm_scenario_optimizer.py --input IPM_Scenario_Input_Template.xlsx --output ./output
```

Dependencies:

```text
pandas
numpy
scipy
openpyxl
pyarrow
```

The template data is synthetic and must be replaced before production use.
