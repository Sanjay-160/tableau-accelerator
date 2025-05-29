from flask import Flask, request, jsonify, render_template
import xml.etree.ElementTree as ET
from flask_cors import CORS

app = Flask(__name__)

CORS(app)

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
    
    try:
        # Parse the .twb file
        tree = ET.parse(file)
        root = tree.getroot()

        # Extract metadata
        metadata = {
            'title': root.get('original-version', 'No title found'),
            'datasources': [],
            'worksheets': [],
            'dashboards': [],
            'calculations': [],
            'joins': [],
            'columns': []
        }

        # Extract datasources
        datasources = root.find('datasources')
        if datasources is not None:
            for datasource in datasources.findall('datasource'):
                datasource_info = {
                    'name': datasource.get('name', 'Unnamed Datasource'),
                    'caption': datasource.get('caption', 'No caption'),
                    'connections': [],
                    'relationships': []
                }

                # Extract connections
                connections = datasource.findall('.//connection')
                for connection in connections:
                    connection_info = {
                        'dbname': connection.get('dbname', 'No dbname'),
                        'server': connection.get('server', 'No server')
                    }
                    datasource_info['connections'].append(connection_info)

                # Append the extracted datasource info to metadata
                metadata['datasources'].append(datasource_info)

        # Extract columns information
        for datasource in datasources.findall('datasource'):
            for column in datasource.findall('column'):
                column_info = {
                    'name': column.get('name', 'Unnamed Column'),
                    'datatype': column.get('datatype', 'Unknown Datatype'),
                    'role': column.get('role', 'Unknown Role'),
                    'aggregation': column.get('aggregation', 'None'),
                }
                metadata['columns'].append(column_info)

        # Extract worksheets
        worksheets = root.findall(".//worksheet")
        for worksheet in worksheets:
            worksheet_info = {
                'name': worksheet.get('name', 'Unnamed Worksheet')
            }
            metadata['worksheets'].append(worksheet_info)

        # Extract dashboards
        dashboards = root.findall(".//dashboard")
        for dashboard in dashboards:
            dashboard_info = {
                'name': dashboard.get('name', 'Unnamed Dashboard')
            }
            metadata['dashboards'].append(dashboard_info)

        # Extract calculated fields
        for datasource in datasources.findall(".//datasource"):
            calculated_fields = datasource.findall(".//column[calculation]")  
            for calc_field in calculated_fields:
                calc_element = calc_field.find('calculation')  
                if calc_element is not None:
                    calc_info = {
                        'name': calc_field.get('caption', calc_field.get('name', 'Unnamed Calculation')),
                        'formula': calc_element.get('formula', 'No formula')  
                    }
                    metadata['calculations'].append(calc_info)

        # Extract joins using a separate function
        metadata['joins'] = extract_joins(datasources)
        metadata['relationships'] = extract_relationships(datasource)

        # Debugging: print out the metadata for verification
        print("Extracted Metadata:", metadata)

        return jsonify(metadata)

    except ET.ParseError:
        return jsonify({'error': 'Error parsing the XML'}), 400
    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'error': 'An internal error occurred.'}), 500

def extract_joins(datasources):
    joins_data = []  # List to hold join information
    for datasource in datasources.findall('datasource'):
        relationships = datasource.findall('.//relation')  # Extract all relations
        for relation in relationships:
            join_info = {
                'left_table': None,
                'right_table': None,
                'join_type': relation.get('join', 'No join type'),
                'on_clause': None  # Placeholder for the join condition
            }
            
            # Extract table names
            left_relation = relation.find('./relation[@join="left"]')  # Find left table
            right_relation = relation.find('./relation[@join="right"]')  # Find right table
            
            if left_relation is not None:
                join_info['left_table'] = left_relation.get('name', 'No left table')
            if right_relation is not None:
                join_info['right_table'] = right_relation.get('name', 'No right table')

            # Extract the join condition from the clause
            clause = relation.find('.//clause/expression')
            if clause is not None:
                # Get the left and right expressions
                expressions = clause.findall('expression')
                if len(expressions) >= 2:
                    left_expression = expressions[0].get('op')  # Left side of join condition
                    right_expression = expressions[1].get('op')  # Right side of join condition
                    join_info['on_clause'] = f"{left_expression} = {right_expression}"

            # Append join info to the list
            joins_data.append(join_info)

    return joins_data

def extract_relationships(datasource):
    relationships_data = []  # List to hold relationship information

    # Access relationships directly from the datasource
    relationships = datasource.find('.//relationships')  # Check for relationships at any depth
    if relationships is not None:
        for relation in relationships.findall('relationship'):
            relationship_info = {
                'left_table': relation.find('first-end-point').attrib.get('object-id', 'No left table'),
                'right_table': relation.find('second-end-point').attrib.get('object-id', 'No right table'),
                'on_clause': None  # Placeholder for the relationship condition
            }

            # Extract the join condition
            clause = relation.find('expression')
            if clause is not None:
                expressions = clause.findall('expression')
                if len(expressions) >= 2:
                    left_expression = expressions[0].attrib.get('op', 'No left expression')  # Left side of relationship condition
                    right_expression = expressions[1].attrib.get('op', 'No right expression')  # Right side of relationship condition
                    relationship_info['on_clause'] = f"{left_expression} = {right_expression}"

            # Append relationship info to the list
            relationships_data.append(relationship_info)

    return relationships_data

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
