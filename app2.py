from flask import Flask, request, jsonify, render_template,session
import xml.etree.ElementTree as ET
from flask_cors import CORS
import os 
from dotenv import load_dotenv
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor
from langsmith import traceable
from langsmith.wrappers import wrap_openai
import zipfile
from google.cloud import secretmanager
import json
import re

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET_KEY")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "teqcertify-50657adb7cfc.json"

def fetch_api_key():
    client = secretmanager.SecretManagerServiceClient()
    secret_name = "projects/457073865923/secrets/Tableau_Accelerator/versions/3"
    response = client.access_secret_version(request={"name": secret_name})
    api_key = response.payload.data.decode("UTF-8")
    return api_key

api_key = fetch_api_key()
apikey_dict = json.loads(api_key)
open_api_key = apikey_dict['OPEN_AI_API_KEY']
app_secret_key = apikey_dict['APP_SECRET_KEY']
langchain_api_key = apikey_dict.get('LANGCHAIN_API_KEY')



app.secret_key = app_secret_key
CORS(app)

os.environ['LANGCHAIN_TRACING_V2']='true'
LANGCHAIN_ENDPOINT="https://api.smith.langchain.com/api/v1/runs/multipart"
LANGCHAIN_API_KEY=langchain_api_key
os.environ['LANGCHAIN_PROJECT']="Tableau-Accelerator"


client =wrap_openai(OpenAI(api_key= api_key))

@app.route('/')
def upload_file():
    return render_template('upload.html')

@app.route('/extract_metadata', methods=['POST'])
def extract_metadata():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file.filename.endswith('.twb'):
        try:
            return process_twb_file(file)
        except ET.ParseError:
            return jsonify({'error': 'Error parsing the XML'}), 400
        except Exception as e:
            print(f"An error occurred: {e}")
            return jsonify({'error': 'An internal error occurred.'}), 500
        
    elif file.filename.endswith('.twbx'):
        try:
            with zipfile.ZipFile(file) as z:
                for name in z.namelist():
                    if name.endswith('.twb'):
                        with z.open(name) as twb_file:
                            return process_twb_file(twb_file)

                return jsonify({'error': 'No .twb file found inside the .twbx archive'}), 400

        except zipfile.BadZipFile:
            return jsonify({'error': 'Invalid .twbx file'}), 400
        except Exception as e:
            print(f"An error occurred: {e}")
            return jsonify({'error': 'An internal error occurred.'}), 500

    else:
        return jsonify({'error': 'Invalid file type. Only .twb and .twbx files are allowed.'}), 400


def process_twb_file(file):
    tree = ET.parse(file)
    root = tree.getroot()

    metadata = {
        'title': root.get('original-version', 'No title found'),
        'datasources': [],
        'dashboards': [],
        "worksheets":[],
        'calculations': [],
        'parameters': [],
        'joins': [],
        'columns': [],
        'Table':{}
    }

    calculation_mapping = {}

    datasources = root.find('datasources')
    if datasources is not None:
        for datasource in datasources.findall('datasource'):

            connections = datasource.findall('connection')

            for connection in connections:
                named_connections = connection.findall('.//named-connections/named-connection')
                for nc in named_connections:
                    inner_conn = nc.find('connection')
                    conn_type = conn_name = server_name = db_name = 'N/A'
                    table_columns = []

                    if inner_conn is not None:
                        conn_type = inner_conn.get('class', 'N/A')
                        conn_name = inner_conn.get('server', 'N/A')
                        server_name = inner_conn.get('server', 'N/A')
                        db_name = inner_conn.get('dbname', 'N/A')

                    relation = datasource.findall('connection/relation/relation')
                    if relation is not None:
                        table_columns = [col.get('name', 'Unnamed Column') for col in relation]

                    datasource_details = {
                        'Connection Type': conn_type,
                        'Connection Name': conn_name,
                        'Server Name': server_name,
                        'Database Name': db_name,
                        'Tables': table_columns
                    }

                    metadata['datasources'].append(datasource_details)
            table_metadata = {}
            metadata_records = datasource.findall('.//metadata-records/metadata-record')
            for record in metadata_records:
                if record is not None:
                    raw_table_name = record.findtext('parent-name', default='[Unknown Table]')
                    table_name = re.sub(r'[\[\]]', '', raw_table_name).strip()
                    column_name = record.findtext('remote-name', default='Unknown Column')
                    col_type = record.findtext('local-type', default='Unknown Type')

                    if table_name not in table_metadata:
                        table_metadata[table_name] = []

                    table_metadata[table_name].append({"column_name" : column_name, "col_type" : col_type})
            metadata['Table'] = table_metadata

    for datasource in datasources.findall('datasource'):
        for column in datasource.findall('column'):
            column_info = {
                'name': column.get('name', 'Unnamed Column'),
                'datatype': column.get('datatype', 'Unknown Datatype'),
                'role': column.get('role', 'Unknown Role'),
                'aggregation': column.get('aggregation', 'None'),
            }
            metadata['columns'].append(column_info)

    worksheets = root.findall(".//worksheet")
    for worksheet in worksheets:
        type_tree = worksheet.find(".//table/panes/pane/mark")
        worksheet_info = {
            'name': worksheet.get('name', 'Unnamed Worksheet'),
            'type':type_tree.get('class','Unknown Type')
        }
        metadata['worksheets'].append(worksheet_info)

    dashboards = root.find('dashboards')
    if dashboards is not None:
        for dashboard in dashboards.findall('dashboard'):
            dashboard_name = dashboard.get('name', 'Unnamed Dashboard')
            worksheet_names = []

            zones = dashboard.findall('.//zones/zone')
            for zone in zones:
                run_element = zone.find('.//formatted-text/run')
                if run_element is not None and run_element.text:
                    worksheet_names.append(run_element.text)

            seen = set()
            unique_worksheets = []
            for ws in worksheet_names:
                if ws not in seen:
                    seen.add(ws)
                    unique_worksheets.append(ws)

            metadata['dashboards'].append({
                'Dashboard Name': dashboard_name,
                'Worksheets': unique_worksheets
            })


    for datasource in datasources.findall(".//datasource"):
        calculated_fields = datasource.findall(".//column[calculation]")  
        for calc_field in calculated_fields:
            calc_element = calc_field.find('calculation')  
            if calc_element is not None:
                formula = calc_element.get('formula', 'No formula')
                calc_name = calc_field.get('caption', calc_field.get('name', 'Unnamed Calculation'))
                name = calc_field.get('name')

                calc_info = {
                    'name': calc_name,
                    'formula': formula
                }
                if is_parameter(formula):
                    metadata['parameters'].append(calc_info)
                else:
                    metadata['calculations'].append(calc_info)
                    calculation_mapping[name] = formula

    for calc in metadata['calculations']:
        formula = calc['formula']
        for name, calc_formula in calculation_mapping.items():
            pattern = r'(?i)\bcalculation.*?_\d+\b'
            formula = re.sub(pattern, calc_formula, formula)
        calc['formula'] = formula


    session['calculations'] = metadata['calculations']

    metadata['joins'] = extract_joins(datasources)
    metadata['relationships'] = extract_relationships(datasources)

    return jsonify(metadata)

def is_parameter(formula):
    if formula.isdigit() or formula.startswith('"') or formula.startswith('#'):
        return True
    return False
    
@app.route('/convert_to_domo', methods=['POST'])
@traceable
def convert_to_domo():
    try:
        calculations = request.json.get('calculations', [])
        if not calculations:
            return jsonify({'error': 'No calculations provided'}), 400

        with ThreadPoolExecutor() as executor:
            domo_formulas = list(executor.map(call_openai, [calc['formula'] for calc in calculations]))

        return jsonify(domo_formulas)

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'error': 'An internal error occurred.'}), 500

def call_openai(formula):
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": "You are a helpful assistant."},
                      {"role": "user", "content": f"Convert the following calculation to Domo Beast Mode format: {formula}. Return only the beast mode calculation.Avoid additional text."}]
        )
        domo_formula = response.choices[0].message.content.strip()
        return {'original_formula': formula, 'domo_formula': domo_formula}
    except Exception as e:
        print(f"Error during OpenAI API call: {e}")
        return {'original_formula': formula, 'domo_formula': 'Error converting formula'}


def extract_joins(datasources):
    joins_data = [] 
    for datasource in datasources.findall('datasource'):
        relationships = datasource.findall('.//relation')  
        for relation in relationships:
            join_info = {
                'left_table': None,
                'right_table': None,
                'join_type': relation.get('join', 'No join type'),
                'on_clause': None
            }

            left_relation = relation.find('./relation[@join="left"]')  
            right_relation = relation.find('./relation[@join="right"]')  
            
            if left_relation is not None:
                join_info['left_table'] = left_relation.get('name', 'No left table')
            if right_relation is not None:
                join_info['right_table'] = right_relation.get('name', 'No right table')

            
            clause = relation.find('.//clause/expression')
            if clause is not None:
                expressions = clause.findall('expression')
                if len(expressions) >= 2:
                    left_expression = expressions[0].get('op')
                    right_expression = expressions[1].get('op')
                    join_info['on_clause'] = f"{left_expression} = {right_expression}"

            joins_data.append(join_info)

    return joins_data


def extract_relationships(datasource):
    relationships_data = []

    relationships = datasource.find('.//relationships')  
    if relationships is not None:
        for relation in relationships.findall('relationship'):
            relationship_info = {
                'left_table': relation.find('first-end-point').attrib.get('object-id', 'No left table'),
                'right_table': relation.find('second-end-point').attrib.get('object-id', 'No right table'),
                'on_clause': None  
            }

            clause = relation.find('expression')
            if clause is not None:
                expressions = clause.findall('expression')
                if len(expressions) >= 2:
                    left_expression = expressions[0].attrib.get('op', 'No left expression')  
                    right_expression = expressions[1].attrib.get('op', 'No right expression') 
                    relationship_info['on_clause'] = f"{left_expression} = {right_expression}"

            relationships_data.append(relationship_info)

    return relationships_data


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
