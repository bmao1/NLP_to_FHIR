#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 31 16:34:05 2021

@author: binmao
"""

from glob import glob
import re
import sys
from ctakes_rest import process_sentence 

#sem_type_list =['DiseaseDisorderMention','SignSymptomMention','MedicationMention','ProcedureMention']
#resource_map={'DiseaseDisorderMention':'Condition', 'SignSymptomMention':'Observation', 'MedicationMention':'MedicationStatement','ProcedureMention':'Procedure'}


def main(args):
    if len(args) < 2:
        sys.stderr.write('2 required arguments: <input dir> <output file>\n')
        sys.exit(-1)

    inpath = re.sub(r'\/$', '', args[0])
    outpath = re.sub(r'\/$', '', args[1])
    for filename in glob(inpath + '/*.txt'):
        with open(filename) as f:
            content = f.readlines()
            content = [x.rstrip() for x in content] 
            text=""
            encounterdate='1900-01-01'
            i=1
            
            uuid = str(filename.split('/')[-1]).split('.')[0]
            for line in content:
                i=i+1
                if re.match('\d{4}-\d{2}-\d{2}',line) or i==len(content) : #in date format for EOF
                    if len(text) >1:
                        process_sentence(text,uuid,encounterdate,outpath)
                        text=""
                        encounterdate = line
                    else:
                        encounterdate = line
                else :
                    text = text + " " + line
    

if __name__ == '__main__':
    main(sys.argv[1:])











        
        
        
        
        
