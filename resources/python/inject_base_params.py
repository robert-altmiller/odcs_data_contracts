import yaml
from pathlib import Path
from jinja2 import Template

BASE_DIR = Path(__file__).resolve().parent.parent  # goes to resources/

def update_workflow_template(wf_name):
    template_path = BASE_DIR / "workflows" / f"{wf_name}_template.yaml"
    param_path = BASE_DIR / "python" / "base_params.yaml"
    output_path = BASE_DIR / "workflows" / f"{wf_name}.yml"

    # Load the workflow template
    with open(template_path) as wf:
        template = Template(wf.read())

    # Load the base parameters
    with open(param_path) as pf:
        params = yaml.safe_load(pf)

    # Render and write
    rendered = template.render(params=params)
    with open(output_path, "w") as out:
        out.write(rendered)

# Run for both workflows
update_workflow_template("data_contract_create")
update_workflow_template("data_contract_deploy")