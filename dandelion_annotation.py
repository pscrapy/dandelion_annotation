import datetime
import codecs
import time
import json

from dandelion import DataTXT

with open("config.json") as fin:
    config_data = json.load(fin)

datatxt = DataTXT(app_id = config_data['application_id'], 
                  app_key = config_data['application_key'])

def simple_clean(text):
    text = " ".join(text.replace("’","'").split())
    return text.lower()



#import spacy
#nlp = spacy.load('it_core_news_sm',
#                 disable=["tagger", "parser", "ner"])
#def spacy_clean(text):
#    text = " ".join(text.replace("’","'").split())
#    doc = nlp(text)
#    tokens = [token.lemma_.strip() for token in doc if
#              not token.is_stop
#              and not nlp.vocab[token.lemma_].is_stop
#              and not token.is_punct
#              and not token.is_digit
#              ]
#    text = " ".join(tokens)
#    return text.lower()


def dandelion_nex(string, min_conf = 0.8, epsilon = 0.3, language="it"):
    """
    Query dandelion API exposing response headers for rate management
    - min_conf: cutoff value for annotation service
    - epsilon: noise value for annotation service
    """
    annotated_string = string
    
    response = datatxt._do_raw_request( "https://api.dandelion.eu/datatxt/nex/v1",
                        {"text": string,
                         "lang": language , 
                         "$app_id" : config_data['application_id'], 
                         "$app_key" : config_data['application_key'],
                         "include_lod" : True,
                         "epsilon" : epsilon,
                         "min_confidence" : min_conf},
                        "post")
    
    payload = response.json()
    headers = response.headers
    
    return payload, headers


def annotation_splicer(text, annotations, 
                       prefix = True, preproc_func = None, spacing = True):
    """
    Splice annotations into text, applying preprocessing if necessary
    - prefix: shorten URI prefixes
    - preproc_func: text pre-processing function (automatically enables entity encoding)
    - spacing: wrap entities in spaces to allow easier pre-processing
    """
    annotated_string = text
    
    # enable entity encoding
    if preproc_func:
        dbrs = list(set([x["lod"]['dbpedia'] for x in annotations]))
        dbr2code = { x : "xxx%015dxxx" % i for i,x in enumerate(dbrs) }
    
    shift = 0
    # for each annotation dict in dandelion response
    for a in annotations:
        # get start/end position
        start = a["start"]
        end = a["end"]
        
        # entity encoding
        if preproc_func:
            symbol = dbr2code[a["lod"]['dbpedia']]
        else:
            symbol = a["lod"]['dbpedia']
        
        # spacing
        if spacing: symbol = " " + symbol + " "
        
        # splicing
        annotated_string = annotated_string[:start+shift] + symbol + annotated_string[shift+end:]
        shift = shift + len(symbol) - (end - start)
    
    # apply preproc_func to entire text
    if preproc_func:
        proc_string = preproc_func(annotated_string)
        
        # revert entity encoding
        for entity, code in dbr2code.items():
            proc_string = proc_string.replace(code,entity)
    else:
        proc_string = annotated_string
    
    # prefix shortening
    if prefix: proc_string = proc_string.replace("http://dbpedia.org/resource/", "dbr:").replace("http://it.dbpedia.org/resource/", "dbr:")
    
    return proc_string


def annotate(texts, dump_path = None, out_file = None, 
             quota=30000, min_conf = 0.75, epsilon = 0.3,
             splicer_preproc = simple_clean, splicer_space = True,
             verbose = True):
    """
    Bulk annotation of text iterable.
    After each call checks remaining daily quota against response headers and sleeps until next reset if necessary.
    - texts: iterable of strings to be annotated
    - dump_path: if set, for each text in collection the annotation service response is written to file as <dump_path>/<collection_index>.json
    - out_file: if set, for each reset sleep the current status of the collection annotation is dumped to file as newline-separated plaintext
    - quota: number of daily calls before sleep
    - min_conf: cutoff value for annotation service
    - epsilon: noise value for annotation service
    - splicer_preproc: preprocessing function to apply to annotation context
    - splicer_space: pad annotation with spaces
    - verbose: enable progress dialogs to standard output
    """
    
    remaining = quota
    next_reset = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
    
    annotated = []

    t0 = time.time()
    
    # for each article 
    for i,t in enumerate(texts):
    
        if i%1000==0 and verbose: 
            print("done %s in %.3f (%s calls remaining)"% (i, time.time()-t0, remaining))
            t0 = time.time()
        
        # safety check
        if len(t) // 3500 > remaining:
            now = datetime.datetime.now(datetime.timezone.utc)
            delta_t = (next_reset - now).seconds + 60
            if out_file:
                with codecs.open(out_file,"w") as fout:
                    fout.write("\n".join(annotated) +"\n")
            if verbose: print("[%s] - Sleeping for %s secondi (%s done)" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),time.time(),delta_t,i))
            time.sleep( delta_t )    
        
        # query dandelion
        payload, headers = dandelion_nex(t, min_conf=min_conf, epsilon = epsilon)
        
        # parse headers
        remaining = int(headers['X-Dl-Units-Left'])
        next_reset = datetime.datetime.strptime(headers['X-Dl-Units-Reset'], "%Y-%m-%d %H:%M:%S %z")
        
        # archive payload
        if dump_path:
            if dump_path[-1] != '/': dump_path += "/"
            with codecs.open(dump_path + "%s.json" % str(i), "w") as fout:
                json.dump(payload['annotations'],fout)
        
        # splice response
        annotated_text = annotation_splicer(t, payload['annotations'], preproc_func = splicer_preproc, spacing = splicer_space)
        
        annotated.append(annotated_text)
        
        time.sleep(1)
    return annotated