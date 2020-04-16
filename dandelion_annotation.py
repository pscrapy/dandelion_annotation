import datetime
import codecs
import time
import ujson as json
​
from dandelion import DataTXT
​
datatxt = DataTXT(app_id='LOL', app_key='NOPE')
​
def dandelion_nex(string, min_conf = 0.8, eps = 0.3):
    annotated_string = string
    
    response = datatxt._do_raw_request( "https://api.dandelion.eu/datatxt/nex/v1",
                        {"text": string,
                         "lang":"it" , 
                         "$app_id" : "LOL", 
                         "$app_key" : "NOPE",
                         "include_lod" : True,
                         "epsilon" : eps,
                         "min_confidence" : min_conf},
                        "post")
    
    payload = response.json()
    headers = response.headers
    
    return payload, headers
​
​
def annotation_splicer(text, annotations, prefix = True, preproc_func = None, spacing = True):
    """Splice annotations into text
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
​
def annotate(texts, dump_path = None, out_file = None, verbose = True):
    
    remaining = 30000
    next_reset = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
​
    annotated = []
​
    t0 = time.time()
​
    # for each article 
    for i,t in enumerate(texts):
​
        if i%1000==0 and verbose: 
            print("done %s in %.3f (%s calls remaining)"% (i, time.time()-t0, remaining))
            t0 = time.time()
​
        # safety check
        if len(t) // 3500 > remaining:
            now = datetime.datetime.now(datetime.timezone.utc)
            delta_t = (next_reset - now).seconds + 60
            if out_file:
                with codecs.open(out_file,"w") as fout:
                    fout.write("\n".join(annotated) +"\n")
            if verbose: print("[%s] - Sleeping for %s secondi (%s done)" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),time.time(),delta_t,i))
            time.sleep( delta_t )    
​
        # query dandelion
        payload, headers = dandelion_nex(t, min_conf=0.75)
​
        # parse headers
        remaining = int(headers['X-Dl-Units-Left'])
        next_reset = datetime.datetime.strptime(headers['X-Dl-Units-Reset'], "%Y-%m-%d %H:%M:%S %z")
​
        # archive payload
        if dump_path:
            if dump_path[-1] != '/': dump_path += "/"
            with codecs.open(dump_path + "%s.pkl" % str(i), "w") as fout:
                json.dump(payload['annotations'],fout)
​
        # splice response
        annotated_text = annotation_splicer(t, payload['annotations'], preproc_func = clean, spacing = True)
​
        annotated.append(annotated_text)
​
        time.sleep(1)
    return annotated