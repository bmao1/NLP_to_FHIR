#!/usr/bin/env python3.6

import requests
import json
from uuid import uuid4

resource_map={'DiseaseDisorderMention':'Condition','IdentifiedAnnotation':'Observation', 'SignSymptomMention':'Observation', 'MedicationMention':'MedicationStatement','ProcedureMention':'Procedure'}

def buildResource(passinfo={}):
    resource ={}
    position_ext = [{ "url": "begin","valueInteger":passinfo['begin']},{ "url": "end","valueInteger":passinfo['end']}]
    version_code = [{ "system": "http://fhir-registry.smarthealthit.org/CodeSystem/nlp-version", "code": "r0.1.1","display": "reader version"}, 
                    { "system": "http://fhir-registry.smarthealthit.org/CodeSystem/nlp-version", "code": "nlp0.1.2", "display": "NLP version"},
                    { "system": "http://fhir-registry.smarthealthit.org/CodeSystem/nlp-version", "code": "w0.1.1", "display": "writer version"}]
    modifier_ext = [{"url":"http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-algorithm",
                     "extension":[{ "url":"version", "valueCodeableConcept": {"coding": version_code,"text": "version info for NLP process"}},
                                  {"url":"dateofauthorship","valueDate":passinfo['notesdate']},
                                  {"url":"identifier","valueString": "Placeholder. record id or something similar. need to figure out a way to trace back to the original source"}
                                  ]
                     },
                     {"url":"http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-polarity",
                     "valueInteger":passinfo['polarity']}]
    
    res_uuid = str(uuid4())
    if passinfo['resourcetype'] == 'MedicationStatement':
        resource = {"resourceType":"MedicationStatement",
                    "id":res_uuid,
                    "status":"unknown",
                    "medicationCodeableConcept":{ 
                        "coding": passinfo['codelist']},
                    "extension":[{
                        "url":"http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position",
                        "extension": position_ext}],
                    "subject":{"reference":passinfo['subject']},
                    "modifierExtension":modifier_ext
                    }
    elif passinfo['resourcetype'] == 'Observation':
        resource = {"resourceType":"Observation",
                    "id":res_uuid,
                    "status":"unknown",
                    "code":{ 
                        "coding": passinfo['codelist']},
                    "extension":[{
                        "url":"http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position",
                        "extension": position_ext}],
                    "subject":{"reference":passinfo['subject']},
                    "modifierExtension":modifier_ext
                    }
    elif passinfo['resourcetype'] == 'Condition':
        resource = {"resourceType":"Condition",
                    "id":res_uuid,
                    "clinicalStatus" : {"coding" : [{"system" : "http://terminology.hl7.org/CodeSystem/condition-clinical","code" : "active","display" : "Active"}]},
                    "verificationStatus" : {"coding" : [{"system" : "http://terminology.hl7.org/CodeSystem/condition-ver-status","code" : "unconfirmed","display" : "Unconfirmed"}]},
                    "category":[{"coding":[{"system": "http://terminology.hl7.org/CodeSystem/condition-category","code": "problem-list-item", "display" : "Problem List Item"}]}],
                    "code":{ 
                        "coding": passinfo['codelist']},
                    "extension":[{
                        "url":"http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position",
                        "extension": position_ext}],
                    "subject":{"reference":passinfo['subject']},
                    "modifierExtension":modifier_ext
                    }
    elif passinfo['resourcetype'] == 'Procedure':
        resource = {"resourceType":"Procedure",
                    "id":res_uuid,
                    "status":"unknown",
                    "code":{ 
                        "coding": passinfo['codelist']},
                    "extension":[{
                        "url":"http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-text-position",
                        "extension": position_ext}],
                    "subject":{"reference":passinfo['subject']},
                    "modifierExtension":modifier_ext
                    }
    else:
        pass
    return resource


def process_sentence(sent,uuid,encounterdate,outputpath):
    # replace ctakes container ip
    url = 'http://localhost:8080/ctakes-web-rest/service/analyze'
    r = requests.post(url, data=sent)
    try:
        contentParse = r.json()
        for sem in contentParse:
            if sem in resource_map:
                add_cuis(contentParse[sem], sem, uuid, encounterdate, outputpath)
    except:
        pass

    return
        

def add_cuis(nlp_json, sem_type, uuid, encounterdate, outputpath):
    for atts in nlp_json:
        code_list = []
        for cuiAtts in atts['conceptAttributes']:
            if cuiAtts['codingScheme'] == 'SNOMEDCT_US':
                system = 'http://snomed.info/sct'
            elif cuiAtts['codingScheme'] == 'RXNORM':
                system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
            else :
                system = cuiAtts['codingScheme']
            codeset = {"system": system, "code":cuiAtts['code'],"display":atts['text'],}
            code_list.append(codeset)
        
        codeset =  {"system": 'urn:oid:2.16.840.1.113883.6.86', "code":atts['conceptAttributes'][0]['cui'], "display":'cui'}  
        code_list.append(codeset)

        codeset =  {"system": 'http://fhir-registry.smarthealthit.org/CodeSystem/nlp-tui', "code":atts['conceptAttributes'][0]['tui'], "display":'tui'}  
        code_list.append(codeset)
        
        passinfo = {'resourcetype':resource_map[sem_type],
                    'begin':atts['begin'], 
                    'end': atts['end'],
                    'notesdate': encounterdate,
                    'codelist': code_list,
                    'polarity': atts['polarity'],
                    'subject': 'Patient/' + uuid}
        resource = buildResource(passinfo)
        if len(resource) >0: 
            with open(outputpath + resource_map[sem_type] +'.ndjson', 'a') as outfile:
                outfile.write(json.dumps(resource))
                outfile.write('\n')
    return

