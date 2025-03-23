import yaml
from jinja2 import Template

def update_workflow_template(wf_name):
    # Load the workflow YAML template
    with open(f"../workflows/{wf_name}_template.yaml") as wf:
        template = Template(wf.read())
    # Load the base parameters YAML
    with open(f"../../base_params.yaml") as pf:
        params = yaml.safe_load(pf)
    # Render new workflow YAML template and write to local
    rendered = template.render(params=params)
    with open(f"../workflows/{wf_name}.yml", "w") as out:
        out.write(rendered)
        
update_workflow_template("data_contract_create")
update_workflow_template("data_contract_deploy")