import os
import multiprocessing as mp
import logging
import re
from tqdm import tqdm
from glob import glob
import sys
import json
from base64 import b64decode
import requests
from uuid import uuid4


resource_map={'DiseaseDisorderMention':'Condition','IdentifiedAnnotation':'Observation', 'SignSymptomMention':'Observation', 'MedicationMention':'MedicationStatement','ProcedureMention':'Procedure'}


def process_ndjson(infile):
    with open(infile) as f:
        for line_num, line in enumerate(f):
            try:
                resource = json.loads(line)
            except:
                logging.warning('Skip line - Invalid json at line {} in {}'.format(line_num, infile))
            sourceCheck = 0
            
            # for March demo and i2b2 data, only keep DocumentReference
            if resource["resourceType"] != "DocumentReference":
                break
            try:
                if resource["resourceType"] == "DocumentReference":
                    uuid = resource["subject"]["reference"][-36:]
                    encounterdate = resource["date"]
                    for code in resource["category"]:
                        for coding in code["coding"]:
                            if coding["system"] == "http://hl7.org/fhir/us/core/CodeSystem/us-core-documentreference-category" and coding["code"] == "clinical-note":
                                sourceCheck = 1
            except:
                continue

            passParameters = {"refUUID": resource["id"],
                              "encounterdate": encounterdate,
                              "patiUUID": uuid
                              }

            if sourceCheck == 1 and "content" in resource:
                for item in resource["content"]:
                    if item["attachment"]["contentType"] == "text/plain": 
                        if "data" in item["attachment"]:
                            passParameters["clinNotes"] = b64decode(item["attachment"]["data"]).decode('utf-8')
                        elif "url" in item["attachment"]:
                            try:
                                with open(item["attachment"]["url"], "r") as note_f:
                                    passParameters["clinNotes"] = note_f.read()
                            except:
                                continue
                    if "clinNotes" in passParameters:    
                        process_sentence(passParameters)

def process_uuidtxt(infile):
    with open(infile) as f:
        content = f.readlines()
        content = [x.rstrip() for x in content] 
        text=""
        encounterdate='1900-01-01'
        i=0
        uuid = str(infile.split('/')[-1]).split('.')[0]

        for line in content:
            i += 1
            if re.match('\d{4}-\d{2}-\d{2}',line) or i==len(content) : #in date format for EOF
                if len(text) >1:
                    passParameters = {"encounterdate": encounterdate,
                                      "patiUUID": uuid,
                                      "clinNotes": text
                                      }
                    process_sentence(passParameters)
                    text=""
                    encounterdate = line
                else:
                    encounterdate = line
            else :
                text = text + "\n" + line


def validate_uuid(uuid):
    from uuid import UUID
    try:
        UUID(uuid)
        return True
    except:
        return False

def process_file(inpath):
    if inpath.lower().endswith("ndjson"):
        process_ndjson(inpath)
    elif inpath.lower().endswith("txt") and validate_uuid(os.path.basename(inpath)[:-4]) :
        process_uuidtxt(inpath)
    else:
        logging.warning('Unrecognized file - ' + inpath)

"""
def loop_files(inpath, disable_progress = True):
    if os.path.isfile(inpath):
        process_file(inpath)
    elif os.path.isdir(inpath):
        inpath = re.sub(r'\/$', '', inpath) +"/"
        for filename in tqdm(glob(inpath+'*.txt') + glob(inpath+'*.ndjson'), disable=disable_progress):
            process_file(filename)
"""
def buildResource(passinfo={}):
    resource ={}
    position_ext = [{ "url": "begin","valueInteger":passinfo['begin']},{ "url": "end","valueInteger":passinfo['end']}]
    modifier_ext = [{"url":"http://fhir-registry.smarthealthit.org/StructureDefinition/nlp-algorithm",
                     "extension":[{"url":"dateofauthorship","valueDate":passinfo['notesdate']}]
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
                    "subject":{"reference":passinfo["subject"]},
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
                    "subject":{"reference":passinfo["subject"]},
                    "modifierExtension":modifier_ext
                    }
    elif passinfo['resourcetype'] == 'Condition':
        resource = {"resourceType":"Condition",
                    "id":res_uuid,
                    "category": [{"coding" : [
                        {"system" : "http://terminology.hl7.org/CodeSystem/condition-category",
                          "code" : "problem-list-item",
                          "display" : "Problem List Item"}],
                      "text" : "Problem"
                    }],
                    "clinicalStatus":"inactive",
                    "verificationStatus":"unconfirmed",
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



def process_sentence(parameters):
    # replace ctakes container ip
    url = 'http://localhost:8080/ctakes-web-rest/service/analyze'

    try:
        r = requests.post(url, data=parameters["clinNotes"])
        contentParse = r.json()
    except:
        logging.warning("Unable to process following text: " + parameters["clinNotes"])
        return
    
    for sem in contentParse:
        if sem in resource_map:
            add_cuis(contentParse[sem], sem, parameters)
    return 




def add_cuis(nlp_json, sem_type, parameters): # uuid, encounterdate, q
    ret = []
    for atts in nlp_json:
        
        # dedup records by keep 1 output per encounter per cui
        if parameters["refUUID"] in cuiTracker:
            if atts['conceptAttributes'][0]["cui"] in cuiTracker[parameters["refUUID"]]:
                continue
            else:
                cuiTracker[parameters["refUUID"]].append(atts['conceptAttributes'][0]["cui"])
        else:
            cuiTracker[parameters["refUUID"]] = [atts['conceptAttributes'][0]["cui"]]
        
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
                    'notesdate': parameters["encounterdate"],
                    'codelist': code_list,
                    'polarity': atts['polarity'],
                    'subject': 'Patient/' + parameters["patiUUID"]}
        if "refUUID" in parameters:
            passinfo["refUUID"] = parameters["refUUID"]
        resource = buildResource(passinfo)
        if len(resource) >0: 
            with open(outpath + resource_map[sem_type] +'.ndjson', 'a') as outfile:
                outfile.write(json.dumps(resource))
                outfile.write('\n')

    return 





def main(args):
    if len(args) < 2:
        sys.stderr.write('2 required arguments: <input ndjson file> <output dir> [enable progress bar=true]\n')
        sys.exit(-1)

    if len(args) == 3:
        disable_progress = not (args[2].lower() == 'true')

    inpath = args[0]
    if os.path.isdir(inpath):
        inpath = re.sub(r'\/$', '', inpath) +"/"
    
    global outpath 
    outpath = args[1]
    outpath = re.sub(r'\/$', '', outpath) +"/"
    disable_progress = True

    # dedup records by keep 1 output per encounter per cui
    global cuiTracker
    cuiTracker = {}
    
    if os.path.isfile(inpath):
        print("processing file: " + inpath)
        process_file(inpath)
        
    elif os.path.isdir(inpath):
        inpath = re.sub(r'\/$', '', inpath) +"/"
        for filename in tqdm(glob(inpath+'*.txt') + glob(inpath+'*.ndjson'), disable=disable_progress):
            print("processing : " + filename)
            process_file(filename)


if __name__ == "__main__":
   main(sys.argv[1:])


