import time

def HeaderPreprocessing(event, context):
    time.sleep(2)
    return {"status": "HeaderPreprocessing complete"}

def TableExtraction(event, context):
    time.sleep(2)
    return {"status": "TableExtraction complete"}

def FeatureExtraction(event, context):
    time.sleep(2)
    return {"status": "FeatureExtraction complete"}

def HeaderExtraction(event, context):
    time.sleep(2)
    return {"status": "HeaderExtraction complete"}

def CPAggr(event, context):
    time.sleep(2)
    return {"status": "CPAggr complete"}

def GeneralPreprocessing(event, context):
    time.sleep(2)
    return {"status": "GeneralPreprocessing complete"}

def SplitInd(event, context):
    time.sleep(2)
    return {"status": "SplitInd complete"}

def Div(event, context):
    time.sleep(2)
    # Return a dummy contractualPackages array for downstream processing.
    return {"contractualPackages": [
        {"packageId": "dummy1"},
        {"packageId": "dummy2"}
    ]}
